//go:build integration

package readreplicas

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresShardedReadReplicas(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	harness.Insert(t, "shard0-primary", 100, 2, "primary")
	harness.Insert(t, "shard0-replica0", 100, 2, "replica0")
	harness.Insert(t, "shard0-replica1", 100, 2, "replica1")
	harness.Insert(t, "shard1-primary", 101, 3, "shard1-primary")

	first, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 100},
	)
	require.NoError(t, err)
	second, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 100},
	)
	require.NoError(t, err)
	strong, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 100},
		fixture.ReadFromPrimary(),
	)
	require.NoError(t, err)
	fallback, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(3), ID: 101},
	)
	require.NoError(t, err)

	assert.Equal(t, "replica0", first.Name)
	assert.Equal(t, "replica1", second.Name)
	assert.Equal(t, "primary", strong.Name)
	assert.Equal(t, "shard1-primary", fallback.Name)
}

func TestPostgresSingletonReadReplicas(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries, err := fixture.NewStore(
		t.Context(),
		fixture.Singleton(
			harness.Pool("shard0-primary"),
			fixture.WithDatabaseName("singleton-replicas"),
			fixture.WithReadReplicas(
				harness.Pool("shard0-replica0"),
				harness.Pool("shard0-replica1"),
			),
		),
	)
	require.NoError(t, err)

	harness.Insert(t, "shard0-primary", 150, 2, "primary")
	harness.Insert(t, "shard0-replica0", 150, 2, "replica0")
	harness.Insert(t, "shard0-replica1", 150, 2, "replica1")

	first, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 150},
	)
	require.NoError(t, err)
	second, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 150},
	)
	require.NoError(t, err)
	strong, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 150},
		fixture.ReadFromPrimary(),
	)
	require.NoError(t, err)

	assert.Equal(t, "replica0", first.Name)
	assert.Equal(t, "replica1", second.Name)
	assert.Equal(t, "primary", strong.Name)
}
