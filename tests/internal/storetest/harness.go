//go:build integration

package storetest

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/tests/internal/testdb"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

type tenantResolver struct{}

func (tenantResolver) TenantKey(key fixture.TenantKey) uint64 {
	return uint64(key.TenantID) //nolint:gosec // Integration fixtures use non-negative tenant IDs.
}

func (tenantResolver) MessageKey(key fixture.MessageKey) uint64 {
	return uint64(key.UserID) //nolint:gosec // Integration fixtures use non-negative user IDs.
}

// TenantKey builds the generated routing key used by the shared fixture.
func TenantKey(tenantID int64) fixture.TenantKey {
	return fixture.TenantKey{TenantID: tenantID}
}

// Harness owns connections to the isolated PostgreSQL test topology.
type Harness struct {
	pools map[string]*pgxpool.Pool
}

// New opens every configured integration endpoint and registers cleanup.
func New(t *testing.T) *Harness {
	t.Helper()
	if !testdb.Enabled() {
		t.Skipf("set %s=1 and start tests/docker-compose.yaml", testdb.IntegrationEnv)
	}

	endpoints := testdb.DefaultEndpoints()
	dsns := make(map[string]string, len(endpoints))
	for _, endpoint := range endpoints {
		dsn, err := endpoint.DSN()
		require.NoError(t, err, "resolve DSN for %s", endpoint.Name)
		dsns[endpoint.Name] = dsn
	}

	pools := make(map[string]*pgxpool.Pool, len(dsns))
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Close()
		}
	})
	for name, dsn := range dsns {
		pool, err := testdb.OpenPool(t.Context(), dsn)
		require.NoError(t, err, "open pool for %s", name)
		pools[name] = pool
	}

	return &Harness{pools: pools}
}

// NewShardedStore builds the shared two-shard topology without write mirrors.
func (h *Harness) NewShardedStore(
	t *testing.T,
	options ...fixture.StoreOption,
) fixture.Store {
	t.Helper()
	return h.newShardedStoreWithMirrors(t, false, options...)
}

// NewMirroredShardedStore adds the shard-zero write mirror.
func (h *Harness) NewMirroredShardedStore(
	t *testing.T,
	options ...fixture.StoreOption,
) fixture.Store {
	t.Helper()
	return h.newShardedStoreWithMirrors(t, true, options...)
}

func (h *Harness) newShardedStoreWithMirrors(
	t *testing.T,
	withMirrors bool,
	options ...fixture.StoreOption,
) fixture.Store {
	t.Helper()
	topologyOptions := []fixture.ShardedOption{
		fixture.WithReplicaSet(
			"shard0",
			h.pools["shard0-primary"],
			h.pools["shard0-replica0"],
			h.pools["shard0-replica1"],
		),
		fixture.WithReplicaSet("shard1", h.pools["shard1-primary"]),
	}
	if withMirrors {
		topologyOptions = append(
			topologyOptions,
			fixture.WithReplicaSet("shard0-mirror", h.pools["shard0-mirror"]),
			fixture.WithVirtualShardMapping("shard0", []uint64{0}, "shard0-mirror"),
		)
	} else {
		topologyOptions = append(
			topologyOptions,
			fixture.WithVirtualShardMapping("shard0", []uint64{0}),
		)
	}
	topologyOptions = append(
		topologyOptions,
		fixture.WithVirtualShardMapping("shard1", []uint64{1}),
	)

	queries, err := fixture.NewStore(
		t.Context(),
		fixture.Sharded(
			2,
			pgmesh.NewModuloShardHasher[uint64](2),
			tenantResolver{},
			topologyOptions...,
		),
		options...,
	)
	require.NoError(t, err)
	return queries
}

// Pool returns a named endpoint pool for feature-specific setup and assertions.
func (h *Harness) Pool(name string) *pgxpool.Pool {
	return h.pools[name]
}

// Reset truncates the shared fixture tables on every endpoint.
func (h *Harness) Reset(t *testing.T) {
	t.Helper()
	for name, pool := range h.pools {
		_, err := pool.Exec(t.Context(), "TRUNCATE TABLE analyses, users")
		require.NoError(t, err, "truncate %s", name)
	}
}

// Insert adds a user marker directly to one endpoint.
func (h *Harness) Insert(
	t *testing.T,
	database string,
	id int64,
	tenantID int64,
	name string,
) {
	t.Helper()
	_, err := h.pools[database].Exec(
		t.Context(),
		"INSERT INTO users (id, tenant_id, name) VALUES ($1, $2, $3)",
		id,
		tenantID,
		name,
	)
	require.NoError(t, err)
}

// UserName reads a user marker directly from one endpoint.
func (h *Harness) UserName(
	t *testing.T,
	database string,
	id int64,
	tenantID int64,
) string {
	t.Helper()
	var name string
	err := h.pools[database].QueryRow(
		t.Context(),
		"SELECT name FROM users WHERE id = $1 AND tenant_id = $2",
		id,
		tenantID,
	).Scan(&name)
	require.NoError(t, err, "read user from %s", database)
	return name
}

// AssertUserAbsent verifies that a routed write did not reach an endpoint.
func (h *Harness) AssertUserAbsent(
	t *testing.T,
	database string,
	id int64,
	tenantID int64,
) {
	t.Helper()
	var ignored int64
	err := h.pools[database].QueryRow(
		t.Context(),
		"SELECT id FROM users WHERE id = $1 AND tenant_id = $2",
		id,
		tenantID,
	).Scan(&ignored)
	require.ErrorIs(t, err, pgx.ErrNoRows, "user unexpectedly exists in %s", database)
}
