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

func TestCopyBatchConfigValidation(t *testing.T) {
	t.Parallel()

	require.NoError(t, (pgmesh.CopyBatchConfig{}).Validate())
	require.ErrorContains(t, (pgmesh.CopyBatchConfig{BatchSize: -1}).Validate(), "size")
	require.ErrorContains(
		t,
		(pgmesh.CopyBatchConfig{FlushTimeout: -time.Nanosecond}).Validate(),
		"timeout",
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
		pgmesh.CopyBatchConfig{BatchSize: 3, FlushTimeout: time.Hour},
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
