//go:build integration

package commands_test

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/command_shapes/store"
	"github.com/sundayfun/pgmesh/tests/internal/testdb"
)

type commandHarness struct {
	primaryTx pgx.Tx
	mirrorTx  pgx.Tx
	commands  store.Commands
}

func newCommandHarness(t *testing.T) *commandHarness {
	t.Helper()
	if !testdb.Enabled() {
		t.Skipf("set %s=1 and start tests/docker-compose.yaml", testdb.IntegrationEnv)
	}

	primaryPool := openCommandPool(t, "shard0-primary")
	mirrorPool := openCommandPool(t, "shard0-mirror")
	primaryTx, err := primaryPool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { testdb.Rollback(t, primaryTx) })
	mirrorTx, err := mirrorPool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { testdb.Rollback(t, mirrorTx) })
	_, err = primaryTx.Exec(t.Context(), "TRUNCATE TABLE users")
	require.NoError(t, err)
	_, err = mirrorTx.Exec(t.Context(), "TRUNCATE TABLE users")
	require.NoError(t, err)

	queries, err := store.NewStore(
		t.Context(),
		store.Singleton(
			primaryTx,
			store.WithDatabaseName("commands"),
			store.WithWriteMirrors(mirrorTx),
		),
	)
	require.NoError(t, err)
	return &commandHarness{
		primaryTx: primaryTx,
		mirrorTx:  mirrorTx,
		commands:  queries.Commands(),
	}
}

func (h *commandHarness) insert(t *testing.T, id, tenantID int64, name string) {
	t.Helper()
	for _, tx := range []pgx.Tx{h.primaryTx, h.mirrorTx} {
		_, err := tx.Exec(
			t.Context(),
			"INSERT INTO users (id, tenant_id, name) VALUES ($1, $2, $3)",
			id,
			tenantID,
			name,
		)
		require.NoError(t, err)
	}
}

func openCommandPool(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	for _, endpoint := range testdb.DefaultEndpoints() {
		if endpoint.Name != name {
			continue
		}
		dsn, err := endpoint.DSN()
		require.NoError(t, err)
		pool, err := testdb.OpenPool(t.Context(), dsn)
		require.NoError(t, err)
		t.Cleanup(pool.Close)
		return pool
	}
	require.FailNowf(t, "integration endpoint is not configured", "endpoint %q", name)
	return nil
}

func assertCommandUserName(t *testing.T, tx pgx.Tx, id int64, want string) {
	t.Helper()
	var name string
	err := tx.QueryRow(t.Context(), "SELECT name FROM users WHERE id = $1", id).Scan(&name)
	require.NoError(t, err)
	assert.Equal(t, want, name)
}

func assertCommandUserAbsent(t *testing.T, tx pgx.Tx, id int64) {
	t.Helper()
	var ignored int64
	err := tx.QueryRow(t.Context(), "SELECT id FROM users WHERE id = $1", id).Scan(&ignored)
	require.ErrorIs(t, err, pgx.ErrNoRows)
}
