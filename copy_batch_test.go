package pgmesh_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
)

type cancelOnSecondErrContext struct {
	context.Context

	calls atomic.Int32
}

func (c *cancelOnSecondErrContext) Err() error {
	if c.calls.Add(1) == 1 {
		return nil
	}
	return context.Canceled
}

func awaitCopyRows(t *testing.T, started <-chan []int) []int {
	t.Helper()
	select {
	case rows := <-started:
		return rows
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for physical COPY execution")
		return nil
	}
}

func TestCopyBatchConfigValidation(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Millisecond, pgmesh.DefaultCopyBatchLinger)
	assert.Equal(t, 32, pgmesh.DefaultCopyBatchMaxConcurrentBatches)
	copyBatch := func(_ context.Context, rows []int) (int64, error) {
		return int64(len(rows)), nil
	}
	tests := []struct {
		name              string
		config            pgmesh.CopyBatchConfig
		copyBatch         pgmesh.CopyBatchFunc[int]
		wantValidationErr string
		wantConstructor   error
	}{
		{name: "zero values use defaults", copyBatch: copyBatch},
		{
			name:              "negative maximum rows per batch",
			config:            pgmesh.CopyBatchConfig{MaxRowsPerBatch: -1},
			copyBatch:         copyBatch,
			wantValidationErr: "rows per batch",
		},
		{
			name:              "negative linger",
			config:            pgmesh.CopyBatchConfig{Linger: -time.Nanosecond},
			copyBatch:         copyBatch,
			wantValidationErr: "linger",
		},
		{
			name:              "negative concurrent batches",
			config:            pgmesh.CopyBatchConfig{MaxConcurrentBatches: -1},
			copyBatch:         copyBatch,
			wantValidationErr: "concurrent",
		},
		{
			name:            "nil copy function",
			copyBatch:       nil,
			wantConstructor: pgmesh.ErrNilCopyBatchFunc,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			validationErr := test.config.Validate()
			if test.wantValidationErr != "" {
				require.ErrorContains(t, validationErr, test.wantValidationErr)
			} else {
				require.NoError(t, validationErr)
			}

			batcher, err := pgmesh.NewCopyBatcher(test.config, test.copyBatch)
			switch {
			case test.wantConstructor != nil:
				require.ErrorIs(t, err, test.wantConstructor)
				assert.Nil(t, batcher)
			case test.wantValidationErr != "":
				require.ErrorContains(t, err, test.wantValidationErr)
				assert.Nil(t, batcher)
			default:
				require.NoError(t, err)
				assert.NotNil(t, batcher)
			}
		})
	}
}

func TestCopyBatcherSubmissionEdges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		context func(*testing.T) context.Context
		rows    []int
		want    int64
		wantErr error
	}{
		{
			name:    "empty submission resolves without a copy",
			context: func(t *testing.T) context.Context { return t.Context() },
			rows:    nil,
		},
		{
			name: "already canceled submission",
			context: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				return ctx
			},
			rows:    []int{1},
			wantErr: context.Canceled,
		},
		{
			name: "canceled before admission",
			context: func(t *testing.T) context.Context {
				return &cancelOnSecondErrContext{Context: t.Context(), calls: atomic.Int32{}}
			},
			rows:    []int{1},
			wantErr: context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			batcher, err := pgmesh.NewCopyBatcher(
				pgmesh.CopyBatchConfig{},
				func(_ context.Context, rows []int) (int64, error) {
					calls.Add(1)
					return int64(len(rows)), nil
				},
			)
			require.NoError(t, err)

			got, err := batcher.Submit(test.context(t), test.rows).Await(t.Context())
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				assert.Zero(t, got)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.want, got)
			}
			assert.Zero(t, calls.Load())
		})
	}
}

func TestCopyBatcherFlushEdges(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("copy failed")
	tests := []struct {
		name       string
		submitRows []int
		copyErr    error
		wantErr    error
		wantCalls  int32
	}{
		{name: "idle flush succeeds"},
		{
			name:       "flush returns pending copy error",
			submitRows: []int{1},
			copyErr:    sentinel,
			wantErr:    sentinel,
			wantCalls:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			batcher, err := pgmesh.NewCopyBatcher(
				pgmesh.CopyBatchConfig{Linger: time.Hour},
				func(_ context.Context, rows []int) (int64, error) {
					calls.Add(1)
					if test.copyErr != nil {
						return 0, test.copyErr
					}
					return int64(len(rows)), nil
				},
				nil,
			)
			require.NoError(t, err)
			if test.submitRows != nil {
				batcher.Submit(t.Context(), test.submitRows)
			}

			err = batcher.Flush(t.Context())
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, test.wantCalls, calls.Load())
		})
	}
}

func TestCopyBatcherCoalescesAndSplitsByRows(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var copies [][]int
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			MaxRowsPerBatch:      3,
			Linger:               time.Hour,
			MaxConcurrentBatches: 1,
		},
		func(_ context.Context, rows []int) (int64, error) {
			mu.Lock()
			copies = append(copies, append([]int(nil), rows...))
			mu.Unlock()
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	first := batcher.Submit(t.Context(), []int{1, 2})
	second := batcher.Submit(t.Context(), []int{3, 4, 5})
	require.NoError(t, batcher.Flush(t.Context()))

	firstCount, err := first.Await(t.Context())
	require.NoError(t, err)
	secondCount, err := second.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(2), firstCount)
	assert.Equal(t, int64(3), secondCount)
	mu.Lock()
	assert.Equal(t, [][]int{{1, 2, 3}, {4, 5}}, copies)
	mu.Unlock()
}

func TestCopyBatcherUsesDefaultMaxConcurrentBatches(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testCopyBatcherUsesDefaultMaxConcurrentBatches)
}

func testCopyBatcherUsesDefaultMaxConcurrentBatches(t *testing.T) {
	started := make(chan struct{}, pgmesh.DefaultCopyBatchMaxConcurrentBatches+1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{MaxRowsPerBatch: 1, Linger: time.Hour},
		func(_ context.Context, rows []int) (int64, error) {
			started <- struct{}{}
			<-release
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	futures := make([]*pgmesh.Future[int64], 0, pgmesh.DefaultCopyBatchMaxConcurrentBatches+1)
	for index := range pgmesh.DefaultCopyBatchMaxConcurrentBatches + 1 {
		futures = append(futures, batcher.Submit(t.Context(), []int{index}))
	}

	for range pgmesh.DefaultCopyBatchMaxConcurrentBatches {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the default COPY workers")
		}
	}
	synctest.Wait()
	select {
	case <-started:
		t.Fatal("started more than the default maximum concurrent COPY operations")
	default:
	}

	release <- struct{}{}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("queued COPY did not start when a worker became available")
	}
	releaseOnce.Do(func() { close(release) })
	for _, future := range futures {
		_, err = future.Await(t.Context())
		require.NoError(t, err)
	}
}

func TestCopyBatcherMergesQueuedLingerExpiredBatchesUpToRowLimit(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testCopyBatcherMergesQueuedLingerExpiredBatchesUpToRowLimit)
}

func testCopyBatcherMergesQueuedLingerExpiredBatchesUpToRowLimit(t *testing.T) {
	const linger = 5 * time.Millisecond
	started := make(chan []int, 3)
	releaseFirst := make(chan struct{})
	var calls int
	var mu sync.Mutex
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			MaxRowsPerBatch:      5,
			Linger:               linger,
			MaxConcurrentBatches: 1,
		},
		func(_ context.Context, rows []int) (int64, error) {
			mu.Lock()
			calls++
			call := calls
			mu.Unlock()
			started <- append([]int(nil), rows...)
			if call == 1 {
				<-releaseFirst
			}
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	first := batcher.Submit(t.Context(), []int{1})
	assert.Equal(t, []int{1}, awaitCopyRows(t, started))

	second := batcher.Submit(t.Context(), []int{2, 3})
	synctest.Sleep(3 * linger)
	third := batcher.Submit(t.Context(), []int{4, 5})
	synctest.Sleep(3 * linger)
	fourth := batcher.Submit(t.Context(), []int{6, 7})
	synctest.Sleep(3 * linger)

	close(releaseFirst)
	assert.Equal(t, []int{2, 3, 4, 5}, awaitCopyRows(t, started))
	assert.Equal(t, []int{6, 7}, awaitCopyRows(t, started))

	for _, item := range []struct {
		future *pgmesh.Future[int64]
		count  int64
	}{
		{future: first, count: 1},
		{future: second, count: 2},
		{future: third, count: 2},
		{future: fourth, count: 2},
	} {
		count, awaitErr := item.future.Await(t.Context())
		require.NoError(t, awaitErr)
		assert.Equal(t, item.count, count)
	}
}

func TestCopyBatcherDoesNotMergeAcrossUnbatchedCopy(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testCopyBatcherDoesNotMergeAcrossUnbatchedCopy)
}

func testCopyBatcherDoesNotMergeAcrossUnbatchedCopy(t *testing.T) {
	const linger = 5 * time.Millisecond
	started := make(chan []int, 4)
	releaseFirst := make(chan struct{})
	var calls int
	var mu sync.Mutex
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			MaxRowsPerBatch:      10,
			Linger:               linger,
			MaxConcurrentBatches: 1,
		},
		func(_ context.Context, rows []int) (int64, error) {
			mu.Lock()
			calls++
			call := calls
			mu.Unlock()
			started <- append([]int(nil), rows...)
			if call == 1 {
				<-releaseFirst
			}
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	first := batcher.Submit(t.Context(), []int{1})
	assert.Equal(t, []int{1}, awaitCopyRows(t, started))
	second := batcher.Submit(t.Context(), []int{2})
	synctest.Sleep(3 * linger)
	unbatched := batcher.SubmitUnbatched(t.Context(), []int{3})
	third := batcher.Submit(t.Context(), []int{4})
	synctest.Sleep(3 * linger)

	close(releaseFirst)
	assert.Equal(t, []int{2}, awaitCopyRows(t, started))
	assert.Equal(t, []int{3}, awaitCopyRows(t, started))
	assert.Equal(t, []int{4}, awaitCopyRows(t, started))

	for _, future := range []*pgmesh.Future[int64]{first, second, unbatched, third} {
		_, err = future.Await(t.Context())
		require.NoError(t, err)
	}
}

func TestCopyBatcherUsesDefaultLinger(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testCopyBatcherUsesDefaultLinger)
}

func testCopyBatcherUsesDefaultLinger(t *testing.T) {
	called := make(chan []int, 1)
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{},
		func(_ context.Context, rows []int) (int64, error) {
			called <- append([]int(nil), rows...)
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	future := batcher.Submit(t.Context(), []int{1})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	count, err := future.Await(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Equal(t, []int{1}, <-called)
}

func TestCopyBatcherObservesPhysicalBatchBoundaries(t *testing.T) {
	t.Parallel()
	synctest.Test(t, testCopyBatcherObservesPhysicalBatchBoundaries)
}

func testCopyBatcherObservesPhysicalBatchBoundaries(t *testing.T) {
	observations := make(chan pgmesh.CopyBatchObservation, 4)
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{MaxRowsPerBatch: 2, Linger: time.Hour},
		func(_ context.Context, rows []int) (int64, error) {
			return int64(len(rows)), nil
		},
		func(_ context.Context, observation pgmesh.CopyBatchObservation) {
			observations <- observation
		},
	)
	require.NoError(t, err)

	first := batcher.Submit(t.Context(), []int{1})
	second := batcher.Submit(t.Context(), []int{2})
	_, err = first.Await(t.Context())
	require.NoError(t, err)
	_, err = second.Await(t.Context())
	require.NoError(t, err)
	sized := <-observations
	assert.Equal(t, 2, sized.RowCount)
	assert.Equal(t, 2, sized.SubmissionCount)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonFull, sized.Reason)
	assert.GreaterOrEqual(t, sized.QueueDuration, time.Duration(0))
	assert.GreaterOrEqual(t, sized.ExecutionDuration, time.Duration(0))
	require.NoError(t, sized.Err)

	_, err = batcher.SubmitUnbatched(t.Context(), []int{3, 4, 5}).Await(t.Context())
	require.NoError(t, err)
	unbatched := <-observations
	assert.Equal(t, 3, unbatched.RowCount)
	assert.Equal(t, 1, unbatched.SubmissionCount)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonUnbatched, unbatched.Reason)

	pending := batcher.Submit(t.Context(), []int{6})
	require.NoError(t, batcher.Flush(t.Context()))
	_, err = pending.Await(t.Context())
	require.NoError(t, err)
	manual := <-observations
	assert.Equal(t, 1, manual.RowCount)
	assert.Equal(t, 1, manual.SubmissionCount)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonManual, manual.Reason)

	lingering, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{Linger: time.Millisecond},
		func(_ context.Context, rows []int) (int64, error) {
			return int64(len(rows)), nil
		},
		func(_ context.Context, observation pgmesh.CopyBatchObservation) {
			observations <- observation
		},
	)
	require.NoError(t, err)
	_, err = lingering.Submit(t.Context(), []int{7}).Await(t.Context())
	require.NoError(t, err)
	linger := <-observations
	assert.Equal(t, pgmesh.CopyBatchFlushReasonLinger, linger.Reason)
}

func TestCopyBatcherSharesErrorsAndRejectsCountMismatch(t *testing.T) {
	t.Parallel()

	copyErr := errors.New("copy failed")
	failing, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{MaxRowsPerBatch: 2},
		func(context.Context, []int) (int64, error) { return 0, copyErr },
	)
	require.NoError(t, err)
	first := failing.Submit(t.Context(), []int{1})
	second := failing.Submit(t.Context(), []int{2})
	_, err = first.Await(t.Context())
	require.ErrorIs(t, err, copyErr)
	_, err = second.Await(t.Context())
	require.ErrorIs(t, err, copyErr)

	mismatch, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{MaxRowsPerBatch: 1},
		func(context.Context, []int) (int64, error) { return 0, nil },
	)
	require.NoError(t, err)
	_, err = mismatch.Submit(t.Context(), []int{3}).Await(t.Context())
	assert.ErrorIs(t, err, pgmesh.ErrCopyBatchCountMismatch)
}

func TestCopyBatcherFlushIsABarrier(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, 2)
	releaseFirst := make(chan struct{})
	var calls int
	var mu sync.Mutex
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{Linger: time.Hour},
		func(_ context.Context, rows []int) (int64, error) {
			mu.Lock()
			calls++
			call := calls
			mu.Unlock()
			started <- struct{}{}
			if call == 1 {
				<-releaseFirst
			}
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	first := batcher.Submit(t.Context(), []int{1})
	flushDone := make(chan error, 1)
	go func() { flushDone <- batcher.Flush(t.Context()) }()
	<-started
	second := batcher.Submit(t.Context(), []int{2})
	close(releaseFirst)
	require.NoError(t, <-flushDone)
	_, err = first.Await(t.Context())
	require.NoError(t, err)

	select {
	case <-started:
		t.Fatal("submission after the flush barrier was flushed")
	default:
	}
	require.NoError(t, batcher.Flush(t.Context()))
	<-started
	_, err = second.Await(t.Context())
	require.NoError(t, err)
}

func TestCopyBatcherRejectsCanceledSubmission(t *testing.T) {
	t.Parallel()

	var called bool
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{},
		func(context.Context, []int) (int64, error) {
			called = true
			return 1, nil
		},
	)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = batcher.Submit(ctx, []int{1}).Await(t.Context())
	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, called)
}
