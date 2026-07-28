//go:build integration

package separatepackage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/generate/separate_package/store"
	"github.com/sundayfun/pgmesh/tests/integration/testdb"
)

func TestSeparatePackageStoreAgainstPostgres(t *testing.T) {
	if !testdb.Enabled() {
		t.Skipf("set %s=1 and start tests/integration/docker-compose.yaml", testdb.IntegrationEnv)
	}

	dsn, err := testdb.PrimaryEndpoint().DSN()
	require.NoError(t, err)
	pool, err := testdb.OpenPool(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	queries, err := store.NewStore(
		t.Context(),
		store.Singleton(tx, store.WithDatabaseName("separate-package")),
	)
	require.NoError(t, err)

	created, err := queries.Users().CreateUser(
		t.Context(),
		&store.CreateUserParams{ID: 99001, TenantID: 99002, Name: "separate"},
	)
	require.NoError(t, err)
	require.NotNil(t, created)

	got, err := queries.Users().GetUser(
		t.Context(),
		&store.GetUserParams{TenantID: created.TenantID, ID: created.ID},
	)
	require.NoError(t, err)
	assert.Equal(t, created, got)

	listed, err := queries.Users().ListUsersByIDs(
		t.Context(),
		[]*store.ListUsersByIDsT{{
			ID:       created.ID,
			TenantID: created.TenantID,
		}},
		store.ReadFromPrimary(),
	)
	require.NoError(t, err)
	assert.Equal(t, []*store.User{created}, listed)
}
