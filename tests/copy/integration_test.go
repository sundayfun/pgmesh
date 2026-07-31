//go:build integration

package copytest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresCopyGroupsRowsByPhysicalShard(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	count, err := queries.Users().CopyUsers(t.Context(), []*fixture.CopyUsersT{
		{TenantKey: storetest.TenantKey(2), ID: 250, Name: "even-two"},
		{TenantKey: storetest.TenantKey(3), ID: 251, Name: "odd"},
		{TenantKey: storetest.TenantKey(4), ID: 252, Name: "even-four"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	assert.Equal(t, "even-two", harness.UserName(t, "shard0-primary", 250, 2))
	assert.Equal(t, "even-four", harness.UserName(t, "shard0-primary", 252, 4))
	assert.Equal(t, "odd", harness.UserName(t, "shard1-primary", 251, 3))
	harness.AssertUserAbsent(t, "shard1-primary", 250, 2)
	harness.AssertUserAbsent(t, "shard0-primary", 251, 3)
}
