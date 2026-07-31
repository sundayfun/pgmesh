package pgmesh

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultCopyBatchFlushTimeout is used when CopyBatchConfig.FlushTimeout is zero.
const DefaultCopyBatchFlushTimeout = time.Millisecond

// ErrCopyBatchCountMismatch reports a successful physical COPY whose returned
// row count does not match the number of rows supplied to it.
var ErrCopyBatchCountMismatch = errors.New("pgmesh: copy batch row count mismatch")

// CopyBatchConfig controls how rows submitted by concurrent callers are
// coalesced into physical COPY operations.
type CopyBatchConfig struct {
	// BatchSize is the maximum number of rows in one physical COPY. Zero leaves
	// the timed batch unbounded by row count.
	BatchSize int
	// FlushTimeout is measured from the first row in a partial batch. Zero uses
	// DefaultCopyBatchFlushTimeout.
	FlushTimeout time.Duration
}

// CopyBatchFlushReason identifies the boundary that made a physical COPY
// batch ready for execution.
type CopyBatchFlushReason string

// Physical COPY batch flush reasons.
const (
	CopyBatchFlushReasonSize      CopyBatchFlushReason = "size"
	CopyBatchFlushReasonTimeout   CopyBatchFlushReason = "timeout"
	CopyBatchFlushReasonExplicit  CopyBatchFlushReason = "explicit"
	CopyBatchFlushReasonImmediate CopyBatchFlushReason = "immediate"
)

// CopyBatchObservation describes one completed physical COPY batch. Rows is
// the attempted physical batch size. Submissions is the number of logical
// submission fragments represented in that batch.
type CopyBatchObservation struct {
	Rows          int
	Submissions   int
	FlushReason   CopyBatchFlushReason
	QueueDuration time.Duration
	Duration      time.Duration
	Err           error
}

// CopyBatchObserver receives completed physical COPY batch observations.
type CopyBatchObserver func(context.Context, CopyBatchObservation)

// Validate validates the configuration without creating a batcher.
func (c CopyBatchConfig) Validate() error {
	if c.BatchSize < 0 {
		return errors.New("pgmesh: copy batch size must not be negative")
	}
	if c.FlushTimeout < 0 {
		return errors.New("pgmesh: copy batch flush timeout must not be negative")
	}
	return nil
}

func (c CopyBatchConfig) normalized() CopyBatchConfig {
	if c.FlushTimeout == 0 {
		c.FlushTimeout = DefaultCopyBatchFlushTimeout
	}
	return c
}

type futureResult[T any] struct {
	value T
	err   error
}

// Future is a repeatable, concurrency-safe handle for an asynchronous result.
// Canceling an Await call only stops that wait; it does not cancel the work.
type Future[T any] struct {
	done chan struct{}
	once sync.Once

	result futureResult[T]
}

func newFuture[T any]() *Future[T] {
	var zero T
	return &Future[T]{
		done: make(chan struct{}),
		once: sync.Once{},
		result: futureResult[T]{
			value: zero,
			err:   nil,
		},
	}
}

func (f *Future[T]) resolve(value T, err error) {
	f.once.Do(func() {
		f.result = futureResult[T]{value: value, err: err}
		close(f.done)
	})
}

// ResolvedFuture returns a Future that has already completed.
func ResolvedFuture[T any](value T, err error) *Future[T] {
	future := newFuture[T]()
	future.resolve(value, err)
	return future
}

// RunFuture starts fn asynchronously and resolves the returned Future with its
// result. The function owns any cancellation semantics for the work itself.
func RunFuture[T any](fn func() (T, error)) *Future[T] {
	future := newFuture[T]()
	go func() {
		value, err := fn()
		future.resolve(value, err)
	}()
	return future
}

// Await waits for the Future or for ctx to be canceled. A later Await may still
// retrieve the completed operation after an earlier wait was canceled.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.result.value, f.result.err
	default:
	}

	select {
	case <-f.done:
		return f.result.value, f.result.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// CopyBatchExecutor performs one physical COPY for rows on a single target.
type CopyBatchExecutor[T any] func(context.Context, []T) (int64, error)

type copyBatchSubmission[T any] struct {
	batcher   *CopyBatcher[T]
	id        uint64
	future    *Future[int64]
	totalRows int

	mu        sync.Mutex
	remaining int
	errors    []error
}

func (s *copyBatchSubmission[T]) addFragment() {
	s.mu.Lock()
	s.remaining++
	s.mu.Unlock()
}

func (s *copyBatchSubmission[T]) completeFragment(err error) {
	s.mu.Lock()
	if err != nil {
		s.errors = append(s.errors, err)
	}
	s.remaining--
	if s.remaining != 0 {
		s.mu.Unlock()
		return
	}
	joined := errors.Join(s.errors...)
	s.mu.Unlock()

	if joined != nil {
		s.future.resolve(0, joined)
	} else {
		s.future.resolve(int64(s.totalRows), nil)
	}

	s.batcher.mu.Lock()
	delete(s.batcher.outstanding, s.id)
	s.batcher.mu.Unlock()
}

type copyBatchPart[T any] struct {
	submission *copyBatchSubmission[T]
}

type physicalCopyBatch[T any] struct {
	ctx         context.Context
	rows        []T
	parts       []copyBatchPart[T]
	timer       *time.Timer
	createdAt   time.Time
	flushReason CopyBatchFlushReason
}

// CopyBatcher coalesces rows for one generated COPY query and one physical
// database target. It starts a drain goroutine only while physical batches are
// ready and executes at most one physical COPY at a time.
type CopyBatcher[T any] struct {
	mu sync.Mutex

	config    CopyBatchConfig
	execute   CopyBatchExecutor[T]
	observers []CopyBatchObserver
	active    *physicalCopyBatch[T]
	ready     []*physicalCopyBatch[T]
	draining  bool

	nextSubmissionID uint64
	outstanding      map[uint64]*Future[int64]
}

// NewCopyBatcher validates config and creates a batcher for one physical COPY
// target. execute must not be nil.
func NewCopyBatcher[T any](
	config CopyBatchConfig,
	execute CopyBatchExecutor[T],
	observers ...CopyBatchObserver,
) (*CopyBatcher[T], error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if execute == nil {
		return nil, errors.New("pgmesh: copy batch executor is nil")
	}
	return &CopyBatcher[T]{
		mu:               sync.Mutex{},
		config:           config.normalized(),
		execute:          execute,
		observers:        append([]CopyBatchObserver(nil), observers...),
		active:           nil,
		ready:            nil,
		draining:         false,
		nextSubmissionID: 0,
		outstanding:      make(map[uint64]*Future[int64]),
	}, nil
}

// Submit accepts rows for asynchronous COPY. Once Submit returns a pending
// Future, canceling ctx no longer cancels the accepted write. Callers must not
// mutate rows or referenced row data until the Future resolves.
func (b *CopyBatcher[T]) Submit(ctx context.Context, rows []T) *Future[int64] {
	if len(rows) == 0 {
		return ResolvedFuture[int64](0, nil)
	}
	if err := ctx.Err(); err != nil {
		return ResolvedFuture[int64](0, err)
	}

	future := newFuture[int64]()
	submission := &copyBatchSubmission[T]{
		batcher:   b,
		id:        0,
		future:    future,
		totalRows: len(rows),
		mu:        sync.Mutex{},
		remaining: 0,
		errors:    nil,
	}

	b.mu.Lock()
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		future.resolve(0, err)
		return future
	}
	b.nextSubmissionID++
	submission.id = b.nextSubmissionID
	b.outstanding[submission.id] = future

	remaining := rows
	startDrain := false
	for len(remaining) > 0 {
		if b.active == nil {
			b.active = &physicalCopyBatch[T]{
				ctx:         context.WithoutCancel(ctx),
				rows:        nil,
				parts:       nil,
				timer:       nil,
				createdAt:   time.Now(),
				flushReason: "",
			}
			active := b.active
			active.timer = time.AfterFunc(b.config.FlushTimeout, func() {
				b.flushTimedBatch(active)
			})
		}

		take := len(remaining)
		if b.config.BatchSize > 0 {
			available := b.config.BatchSize - len(b.active.rows)
			if take > available {
				take = available
			}
		}
		b.active.rows = append(b.active.rows, remaining[:take]...)
		b.active.parts = append(b.active.parts, copyBatchPart[T]{submission: submission})
		submission.addFragment()
		remaining = remaining[take:]

		if b.config.BatchSize > 0 && len(b.active.rows) == b.config.BatchSize {
			batch := b.detachActiveLocked(CopyBatchFlushReasonSize)
			if b.enqueueReadyLocked(batch) {
				startDrain = true
			}
		}
	}
	b.mu.Unlock()

	if startDrain {
		go b.drain()
	}
	return future
}

// SubmitImmediate accepts rows as one physical COPY without coalescing them
// with other submissions. It shares the batcher's FIFO execution queue and is
// therefore included in Flush barriers. Callers must not mutate rows or
// referenced row data until the Future resolves.
func (b *CopyBatcher[T]) SubmitImmediate(ctx context.Context, rows []T) *Future[int64] {
	if len(rows) == 0 {
		return ResolvedFuture[int64](0, nil)
	}
	if err := ctx.Err(); err != nil {
		return ResolvedFuture[int64](0, err)
	}

	future := newFuture[int64]()
	submission := &copyBatchSubmission[T]{
		batcher:   b,
		id:        0,
		future:    future,
		totalRows: len(rows),
		mu:        sync.Mutex{},
		remaining: 0,
		errors:    nil,
	}

	b.mu.Lock()
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		future.resolve(0, err)
		return future
	}
	b.nextSubmissionID++
	submission.id = b.nextSubmissionID
	b.outstanding[submission.id] = future
	submission.addFragment()
	startDrain := b.enqueueReadyLocked(&physicalCopyBatch[T]{
		ctx:         context.WithoutCancel(ctx),
		rows:        append([]T(nil), rows...),
		parts:       []copyBatchPart[T]{{submission: submission}},
		timer:       nil,
		createdAt:   time.Now(),
		flushReason: CopyBatchFlushReasonImmediate,
	})
	b.mu.Unlock()

	if startDrain {
		go b.drain()
	}
	return future
}

func (b *CopyBatcher[T]) flushTimedBatch(batch *physicalCopyBatch[T]) {
	b.mu.Lock()
	if b.active != batch {
		b.mu.Unlock()
		return
	}
	b.active = nil
	batch.timer = nil
	batch.flushReason = CopyBatchFlushReasonTimeout
	startDrain := b.enqueueReadyLocked(batch)
	b.mu.Unlock()
	if startDrain {
		go b.drain()
	}
}

func (b *CopyBatcher[T]) detachActiveLocked(
	reason CopyBatchFlushReason,
) *physicalCopyBatch[T] {
	batch := b.active
	b.active = nil
	if batch != nil && batch.timer != nil {
		batch.timer.Stop()
		batch.timer = nil
	}
	if batch != nil {
		batch.flushReason = reason
	}
	return batch
}

func (b *CopyBatcher[T]) enqueueReadyLocked(batch *physicalCopyBatch[T]) bool {
	if batch == nil || len(batch.rows) == 0 {
		return false
	}
	b.ready = append(b.ready, batch)
	if b.draining {
		return false
	}
	b.draining = true
	return true
}

func (b *CopyBatcher[T]) drain() {
	for {
		b.mu.Lock()
		if len(b.ready) == 0 {
			b.draining = false
			b.mu.Unlock()
			return
		}
		batch := b.ready[0]
		b.ready[0] = nil
		b.ready = b.ready[1:]
		b.mu.Unlock()

		started := time.Now()
		count, err := b.execute(batch.ctx, batch.rows)
		if err == nil && count != int64(len(batch.rows)) {
			err = fmt.Errorf(
				"%w: got %d, want %d",
				ErrCopyBatchCountMismatch,
				count,
				len(batch.rows),
			)
		}
		observation := CopyBatchObservation{
			Rows:          len(batch.rows),
			Submissions:   len(batch.parts),
			FlushReason:   batch.flushReason,
			QueueDuration: started.Sub(batch.createdAt),
			Duration:      time.Since(started),
			Err:           err,
		}
		for _, observer := range b.observers {
			if observer != nil {
				observer(batch.ctx, observation)
			}
		}
		for _, part := range batch.parts {
			part.submission.completeFragment(err)
		}
	}
}

// FlushAsync forces the current partial batch to execute and returns a Future
// for submissions that were outstanding at the flush barrier. Submissions
// accepted later are not included.
func (b *CopyBatcher[T]) FlushAsync() *Future[struct{}] {
	b.mu.Lock()
	batch := b.detachActiveLocked(CopyBatchFlushReasonExplicit)
	startDrain := b.enqueueReadyLocked(batch)
	futures := make([]*Future[int64], 0, len(b.outstanding))
	for _, future := range b.outstanding {
		futures = append(futures, future)
	}
	b.mu.Unlock()

	if startDrain {
		go b.drain()
	}
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

// Flush forces the current partial batch to execute and waits for submissions
// that were outstanding at the flush barrier. Submissions accepted later are
// not included.
func (b *CopyBatcher[T]) Flush(ctx context.Context) error {
	_, err := b.FlushAsync().Await(ctx)
	return err
}
