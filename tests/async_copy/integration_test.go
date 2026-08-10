//go:build integration

package asynccopy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresAsyncCopyBatchesAndFlushes(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(
		t,
		fixture.WithCopyUsersBatching(pgmesh.CopyBatchConfig{
			MaxRowsPerBatch: 8,
			Linger:          time.Hour,
		}),
	)

	futures := make([]*pgmesh.Future[int64], 0, 4)
	for index, tenantID := range []int64{2, 3, 4, 5} {
		futures = append(futures, queries.Users().CopyUsersAsync(
			t.Context(),
			[]*fixture.CopyUsersT{{
				TenantKey: storetest.TenantKey(tenantID),
				ID:        int64(270 + index),
				Name:      "async-copy",
			}},
		))
	}
	require.NoError(t, queries.Users().FlushCopyUsers(t.Context()))
	for _, future := range futures {
		count, err := future.Await(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	}

	assert.Equal(t, "async-copy", harness.UserName(t, "shard0-primary", 270, 2))
	assert.Equal(t, "async-copy", harness.UserName(t, "shard1-primary", 271, 3))
	assert.Equal(t, "async-copy", harness.UserName(t, "shard0-primary", 272, 4))
	assert.Equal(t, "async-copy", harness.UserName(t, "shard1-primary", 273, 5))
}
