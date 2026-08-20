//go:build integration

package separatepackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/testdb"
	"github.com/sundayfun/pgmesh/tests/separate_package/store"
)

func TestSeparatePackageStoreAgainstPostgres(t *testing.T) {
	if !testdb.Enabled() {
		t.Skipf("set %s=1 and start tests/docker-compose.yaml", testdb.IntegrationEnv)
	}

	dsn, err := testdb.PrimaryEndpoint().DSN()
	require.NoError(t, err)
	pool, err := testdb.OpenPool(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { testdb.Rollback(t, tx) })
	queries, err := store.NewStore(
		t.Context(),
		store.Singleton(tx, store.WithDatabaseName("separate-package")),
	)
	require.NoError(t, err)

	created, err := queries.Users().CreateUser(
		t.Context(),
		&store.CreateUserT{
			TenantID: 99002,
			ID:       99001,
			Name:     "separate",
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created)

	got, err := queries.Users().GetUser(
		t.Context(),
		&store.GetUserT{
			TenantID: created.TenantID,
			ID:       created.ID,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, created, got)

	listed, err := queries.Users().ListUsersByIDs(
		t.Context(),
		[]*store.ListUsersByIDsT{{
			TenantID: created.TenantID,
			ID:       created.ID,
		}},
		store.ReadFromPrimary(),
	)
	require.NoError(t, err)
	assert.Equal(t, []*store.User{created}, listed)
}
