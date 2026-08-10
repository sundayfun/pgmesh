package pgmesh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// CopyBatchFunc performs one physical COPY operation for a single target.
type CopyBatchFunc[T any] func(ctx context.Context, rows []T) (int64, error)

type copyRequest struct {
	result   *Future[int64]
	rowCount int

	mu           sync.Mutex
	pendingParts int
	errs         []error
}

func newCopyRequest(rowCount int) *copyRequest {
	return &copyRequest{
		result:       newFuture[int64](),
		rowCount:     rowCount,
		mu:           sync.Mutex{},
		pendingParts: 0,
		errs:         nil,
	}
}

func (r *copyRequest) addPart() {
	r.mu.Lock()
	r.pendingParts++
	r.mu.Unlock()
}

func (r *copyRequest) finishPart(err error) bool {
	r.mu.Lock()
	if err != nil {
		r.errs = append(r.errs, err)
	}
	r.pendingParts--
	if r.pendingParts != 0 {
		r.mu.Unlock()
		return false
	}
	err = errors.Join(r.errs...)
	r.mu.Unlock()

	if err != nil {
		r.result.resolve(0, err)
	} else {
		r.result.resolve(int64(r.rowCount), nil)
	}
	return true
}

type copyBatch[T any] struct {
	ctx      context.Context
	rows     []T
	requests []*copyRequest
	timer    *time.Timer
	queuedAt time.Time
	reason   CopyBatchFlushReason
}

// CopyBatcher coalesces rows for one generated COPY query and one physical
// database target. It executes up to CopyBatchConfig.MaxConcurrentBatches
// physical COPY operations concurrently.
type CopyBatcher[T any] struct {
	mu sync.Mutex

	config    CopyBatchConfig
	copyBatch CopyBatchFunc[T]
	observers []CopyBatchObserver
	active    *copyBatch[T]
	queue     []*copyBatch[T]
	inFlight  int
	pending   map[*copyRequest]struct{}
}

// NewCopyBatcher validates config and creates a batcher for one physical COPY
// target.
func NewCopyBatcher[T any](
	config CopyBatchConfig,
	copyBatch CopyBatchFunc[T],
	observers ...CopyBatchObserver,
) (*CopyBatcher[T], error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if copyBatch == nil {
		return nil, ErrNilCopyBatchFunc
	}
	return &CopyBatcher[T]{
		mu:        sync.Mutex{},
		config:    config.normalized(),
		copyBatch: copyBatch,
		observers: append([]CopyBatchObserver(nil), observers...),
		active:    nil,
		queue:     nil,
		inFlight:  0,
		pending:   make(map[*copyRequest]struct{}),
	}, nil
}

// Submit accepts rows for asynchronous, coalesced COPY. Once Submit returns a
// pending Future, canceling ctx no longer cancels the accepted write. Callers
// must not mutate rows or referenced row data until the Future resolves.
func (b *CopyBatcher[T]) Submit(ctx context.Context, rows []T) *Future[int64] {
	return b.submit(ctx, rows, true)
}

// SubmitUnbatched accepts rows as one physical COPY without coalescing them
// with other submissions. It shares execution capacity with coalesced batches
// and participates in Flush barriers, so it may wait in the queue. Callers must
// not mutate rows or referenced row data until the Future resolves.
func (b *CopyBatcher[T]) SubmitUnbatched(ctx context.Context, rows []T) *Future[int64] {
	return b.submit(ctx, rows, false)
}

func (b *CopyBatcher[T]) submit(
	ctx context.Context,
	rows []T,
	coalesce bool,
) *Future[int64] {
	if len(rows) == 0 {
		return ResolvedFuture[int64](0, nil)
	}
	if err := ctx.Err(); err != nil {
		return ResolvedFuture[int64](0, err)
	}

	request := newCopyRequest(len(rows))
	b.mu.Lock()
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		request.result.resolve(0, err)
		return request.result
	}
	b.pending[request] = struct{}{}
	if coalesce {
		b.appendRequestLocked(ctx, rows, request)
	} else {
		b.enqueueUnbatchedLocked(ctx, rows, request)
	}
	batches := b.scheduleLocked()
	b.mu.Unlock()

	b.dispatch(batches)
	return request.result
}

func (b *CopyBatcher[T]) enqueueUnbatchedLocked(
	ctx context.Context,
	rows []T,
	request *copyRequest,
) {
	request.addPart()
	b.enqueueLocked(&copyBatch[T]{
		ctx:      context.WithoutCancel(ctx),
		rows:     append([]T(nil), rows...),
		requests: []*copyRequest{request},
		timer:    nil,
		queuedAt: time.Now(),
		reason:   CopyBatchFlushReasonUnbatched,
	})
}

func (b *CopyBatcher[T]) appendRequestLocked(
	ctx context.Context,
	rows []T,
	request *copyRequest,
) {
	for len(rows) > 0 {
		if b.active == nil {
			b.openBatchLocked(ctx)
		}

		rowCount := len(rows)
		if b.config.MaxRowsPerBatch > 0 {
			rowCount = min(rowCount, b.config.MaxRowsPerBatch-len(b.active.rows))
		}
		b.active.rows = append(b.active.rows, rows[:rowCount]...)
		b.active.requests = append(b.active.requests, request)
		request.addPart()
		rows = rows[rowCount:]

		if b.config.MaxRowsPerBatch > 0 && len(b.active.rows) == b.config.MaxRowsPerBatch {
			b.enqueueLocked(b.detachActiveLocked(CopyBatchFlushReasonFull))
		}
	}
}

func (b *CopyBatcher[T]) openBatchLocked(ctx context.Context) {
	batch := &copyBatch[T]{
		ctx:      context.WithoutCancel(ctx),
		rows:     nil,
		requests: nil,
		timer:    nil,
		queuedAt: time.Now(),
		reason:   "",
	}
	batch.timer = time.AfterFunc(b.config.Linger, func() {
		b.flushExpiredBatch(batch)
	})
	b.active = batch
}

func (b *CopyBatcher[T]) flushExpiredBatch(batch *copyBatch[T]) {
	b.mu.Lock()
	if b.active != batch {
		b.mu.Unlock()
		return
	}
	b.active = nil
	batch.timer = nil
	batch.reason = CopyBatchFlushReasonLinger
	b.enqueueLocked(batch)
	batches := b.scheduleLocked()
	b.mu.Unlock()

	b.dispatch(batches)
}

func (b *CopyBatcher[T]) detachActiveLocked(reason CopyBatchFlushReason) *copyBatch[T] {
	batch := b.active
	b.active = nil
	if batch != nil && batch.timer != nil {
		batch.timer.Stop()
		batch.timer = nil
	}
	if batch != nil {
		batch.reason = reason
	}
	return batch
}

func (b *CopyBatcher[T]) enqueueLocked(batch *copyBatch[T]) {
	if batch == nil || len(batch.rows) == 0 {
		return
	}
	b.queue = append(b.queue, batch)
}

func (b *CopyBatcher[T]) dequeueLocked() *copyBatch[T] {
	batch := b.popQueueLocked()
	if batch.reason != CopyBatchFlushReasonLinger {
		return batch
	}

	for len(b.queue) > 0 {
		next := b.queue[0]
		if next.reason != CopyBatchFlushReasonLinger {
			break
		}
		if b.config.MaxRowsPerBatch > 0 &&
			len(batch.rows)+len(next.rows) > b.config.MaxRowsPerBatch {
			break
		}
		next = b.popQueueLocked()
		batch.rows = append(batch.rows, next.rows...)
		batch.requests = append(batch.requests, next.requests...)
	}
	return batch
}

func (b *CopyBatcher[T]) popQueueLocked() *copyBatch[T] {
	batch := b.queue[0]
	b.queue[0] = nil
	b.queue = b.queue[1:]
	return batch
}

func (b *CopyBatcher[T]) scheduleLocked() []*copyBatch[T] {
	capacity := b.config.MaxConcurrentBatches - b.inFlight
	if capacity <= 0 || len(b.queue) == 0 {
		return nil
	}

	batches := make([]*copyBatch[T], 0, min(capacity, len(b.queue)))
	for capacity > 0 && len(b.queue) > 0 {
		batches = append(batches, b.dequeueLocked())
		b.inFlight++
		capacity--
	}
	return batches
}

func (b *CopyBatcher[T]) dispatch(batches []*copyBatch[T]) {
	for _, batch := range batches {
		go b.runBatch(batch)
	}
}

func (b *CopyBatcher[T]) runBatch(batch *copyBatch[T]) {
	startedAt := time.Now()
	count, err := b.copyBatch(batch.ctx, batch.rows)
	if err == nil && count != int64(len(batch.rows)) {
		err = fmt.Errorf(
			"%w: got %d, want %d",
			ErrCopyBatchCountMismatch,
			count,
			len(batch.rows),
		)
	}

	observation := CopyBatchObservation{
		RowCount:          len(batch.rows),
		SubmissionCount:   len(batch.requests),
		Reason:            batch.reason,
		QueueDuration:     startedAt.Sub(batch.queuedAt),
		ExecutionDuration: time.Since(startedAt),
		Err:               err,
	}
	for _, observer := range b.observers {
		if observer != nil {
			observer(batch.ctx, observation)
		}
	}

	completed := make([]*copyRequest, 0, len(batch.requests))
	for _, request := range batch.requests {
		if request.finishPart(err) {
			completed = append(completed, request)
		}
	}

	b.mu.Lock()
	for _, request := range completed {
		delete(b.pending, request)
	}
	b.inFlight--
	batches := b.scheduleLocked()
	b.mu.Unlock()

	b.dispatch(batches)
}

// FlushAsync forces the current partial batch to execute and returns a Future
// for requests that were pending at the flush barrier. Requests accepted later
// are not included.
func (b *CopyBatcher[T]) FlushAsync() *Future[struct{}] {
	b.mu.Lock()
	b.enqueueLocked(b.detachActiveLocked(CopyBatchFlushReasonManual))
	batches := b.scheduleLocked()
	futures := make([]*Future[int64], 0, len(b.pending))
	for request := range b.pending {
		futures = append(futures, request.result)
	}
	b.mu.Unlock()

	b.dispatch(batches)
	return RunFuture(func() (struct{}, error) {
		errs := make([]error, 0, len(futures))
		for _, future := range futures {
			if _, err := future.Await(context.Background()); err != nil {
				errs = append(errs, err)
			}
		}
		return struct{}{}, errors.Join(errs...)
	})
}

// Flush forces the current partial batch to execute and waits for requests
// that were pending at the flush barrier. Requests accepted later are not
// included.
func (b *CopyBatcher[T]) Flush(ctx context.Context) error {
	_, err := b.FlushAsync().Await(ctx)
	return err
}
