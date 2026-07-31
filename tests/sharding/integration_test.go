//go:build integration

package sharding

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresWritesRouteByVirtualShard(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	_, err := queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(2), ID: 200, Name: "even"},
	)
	require.NoError(t, err)
	_, err = queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(3), ID: 201, Name: "odd"},
	)
	require.NoError(t, err)

	assert.Equal(t, "even", harness.UserName(t, "shard0-primary", 200, 2))
	harness.AssertUserAbsent(t, "shard1-primary", 200, 2)
	assert.Equal(t, "odd", harness.UserName(t, "shard1-primary", 201, 3))
	harness.AssertUserAbsent(t, "shard0-primary", 201, 3)
}
