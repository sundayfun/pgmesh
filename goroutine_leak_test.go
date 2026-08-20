package pgmesh_test

import (
	"bytes"
	"context"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
)

func TestAsyncOperationsDoNotLeakGoroutines(t *testing.T) { //nolint:paralleltest // The profile measures process-wide goroutines.
	leakProfile := pprof.Lookup("goroutineleak")
	require.NotNil(t, leakProfile)

	value, err := pgmesh.RunFuture(func() (int, error) {
		return 42, nil
	}).Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 42, value)

	batcher, err := pgmesh.NewCopyBatcher(
		pgmesh.CopyBatchConfig{MaxRowsPerBatch: 1},
		func(_ context.Context, rows []int) (int64, error) {
			return int64(len(rows)), nil
		},
	)
	require.NoError(t, err)

	count, err := batcher.Submit(t.Context(), []int{1}).Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	require.NoError(t, batcher.Flush(t.Context()))

	var profile bytes.Buffer
	require.NoError(t, leakProfile.WriteTo(&profile, 1))
	assert.Zero(t, leakProfile.Count(), profile.String())
}
