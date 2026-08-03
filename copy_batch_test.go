package pgmesh_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
)

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

	assert.Equal(t, 32, pgmesh.DefaultCopyBatchMaxConcurrentCopies)
	require.NoError(t, (pgmesh.CopyBatchConfig{}).Validate())
	require.ErrorContains(t, (pgmesh.CopyBatchConfig{BatchSize: -1}).Validate(), "size")
	require.ErrorContains(
		t,
		(pgmesh.CopyBatchConfig{FlushTimeout: -time.Nanosecond}).Validate(),
		"timeout",
	)
	require.ErrorContains(
		t,
		(pgmesh.CopyBatchConfig{MaxConcurrentCopies: -1}).Validate(),
		"concurrent",
	)
	_, err := pgmesh.NewCopyBatcher[int](pgmesh.CopyBatchConfig{}, nil)
	assert.ErrorContains(t, err, "executor is nil")
}

func TestFutureAwaitCancellationDoesNotCancelWork(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	future := pgmesh.RunFuture(func() (int, error) {
		<-release
		return 42, nil
	})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	result, err := future.Await(canceled)
	assert.Zero(t, result)
	require.ErrorIs(t, err, context.Canceled)

	close(release)
	result, err = future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, result)
	result, err = future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, result)
}

func TestCopyBatcherCoalescesAndSplitsByRows(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var copies [][]int
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			BatchSize:           3,
			FlushTimeout:        time.Hour,
			MaxConcurrentCopies: 1,
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

func TestCopyBatcherUsesDefaultMaxConcurrentCopies(t *testing.T) {
	t.Parallel()

	started := make(chan struct{}, pgmesh.DefaultCopyBatchMaxConcurrentCopies+1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{BatchSize: 1, FlushTimeout: time.Hour},
		func(_ context.Context, rows []int) (int64, error) {
			started <- struct{}{}
			<-release
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	futures := make([]*pgmesh.Future[int64], 0, pgmesh.DefaultCopyBatchMaxConcurrentCopies+1)
	for index := range pgmesh.DefaultCopyBatchMaxConcurrentCopies + 1 {
		futures = append(futures, batcher.Submit(t.Context(), []int{index}))
	}

	for range pgmesh.DefaultCopyBatchMaxConcurrentCopies {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the default COPY workers")
		}
	}
	select {
	case <-started:
		t.Fatal("started more than the default maximum concurrent COPY operations")
	case <-time.After(20 * time.Millisecond):
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

func TestCopyBatcherMergesQueuedTimeoutBatchesUpToBatchSize(t *testing.T) {
	t.Parallel()

	const flushTimeout = 5 * time.Millisecond
	started := make(chan []int, 3)
	releaseFirst := make(chan struct{})
	var calls int
	var mu sync.Mutex
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			BatchSize:           5,
			FlushTimeout:        flushTimeout,
			MaxConcurrentCopies: 1,
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
	time.Sleep(3 * flushTimeout)
	third := batcher.Submit(t.Context(), []int{4, 5})
	time.Sleep(3 * flushTimeout)
	fourth := batcher.Submit(t.Context(), []int{6, 7})
	time.Sleep(3 * flushTimeout)

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

func TestCopyBatcherDoesNotMergeAcrossImmediateBatch(t *testing.T) {
	t.Parallel()

	const flushTimeout = 5 * time.Millisecond
	started := make(chan []int, 4)
	releaseFirst := make(chan struct{})
	var calls int
	var mu sync.Mutex
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{
			BatchSize:           10,
			FlushTimeout:        flushTimeout,
			MaxConcurrentCopies: 1,
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
	time.Sleep(3 * flushTimeout)
	immediate := batcher.SubmitImmediate(t.Context(), []int{3})
	third := batcher.Submit(t.Context(), []int{4})
	time.Sleep(3 * flushTimeout)

	close(releaseFirst)
	assert.Equal(t, []int{2}, awaitCopyRows(t, started))
	assert.Equal(t, []int{3}, awaitCopyRows(t, started))
	assert.Equal(t, []int{4}, awaitCopyRows(t, started))

	for _, future := range []*pgmesh.Future[int64]{first, second, immediate, third} {
		_, err = future.Await(t.Context())
		require.NoError(t, err)
	}
}

func TestCopyBatcherUsesDefaultTimeout(t *testing.T) {
	t.Parallel()

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

	observations := make(chan pgmesh.CopyBatchObservation, 4)
	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{BatchSize: 2, FlushTimeout: time.Hour},
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
	assert.Equal(t, 2, sized.Rows)
	assert.Equal(t, 2, sized.Submissions)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonSize, sized.FlushReason)
	assert.GreaterOrEqual(t, sized.QueueDuration, time.Duration(0))
	assert.GreaterOrEqual(t, sized.Duration, time.Duration(0))
	require.NoError(t, sized.Err)

	_, err = batcher.SubmitImmediate(t.Context(), []int{3, 4, 5}).Await(t.Context())
	require.NoError(t, err)
	immediate := <-observations
	assert.Equal(t, 3, immediate.Rows)
	assert.Equal(t, 1, immediate.Submissions)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonImmediate, immediate.FlushReason)

	pending := batcher.Submit(t.Context(), []int{6})
	require.NoError(t, batcher.Flush(t.Context()))
	_, err = pending.Await(t.Context())
	require.NoError(t, err)
	explicit := <-observations
	assert.Equal(t, 1, explicit.Rows)
	assert.Equal(t, 1, explicit.Submissions)
	assert.Equal(t, pgmesh.CopyBatchFlushReasonExplicit, explicit.FlushReason)

	timed, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{FlushTimeout: time.Millisecond},
		func(_ context.Context, rows []int) (int64, error) {
			return int64(len(rows)), nil
		},
		func(_ context.Context, observation pgmesh.CopyBatchObservation) {
			observations <- observation
		},
	)
	require.NoError(t, err)
	_, err = timed.Submit(t.Context(), []int{7}).Await(t.Context())
	require.NoError(t, err)
	timeout := <-observations
	assert.Equal(t, pgmesh.CopyBatchFlushReasonTimeout, timeout.FlushReason)
}

func TestCopyBatcherSharesErrorsAndRejectsCountMismatch(t *testing.T) {
	t.Parallel()

	copyErr := errors.New("copy failed")
	failing, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{BatchSize: 2},
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
		pgmesh.CopyBatchConfig{BatchSize: 1},
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
		pgmesh.CopyBatchConfig{FlushTimeout: time.Hour},
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
