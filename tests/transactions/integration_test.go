//go:build integration

package transactions

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	"github.com/sundayfun/pgmesh/tests/internal/testdb"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresTransactionPinsOneShardPrimary(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)
	tx, err := harness.Pool("shard0-primary").Begin(t.Context())
	require.NoError(t, err)
	defer testdb.Rollback(t, tx)

	created, err := queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{
			TenantKey: storetest.TenantKey(2),
			ID:        390,
			Name:      "transactional",
		},
		fixture.WithTx(tx),
	)
	require.NoError(t, err)
	assert.Equal(t, "transactional", created.Name)

	inside, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 390},
		fixture.WithTx(tx),
	)
	require.NoError(t, err)
	assert.Equal(t, created, inside)
	require.NoError(t, tx.Commit(t.Context()))

	assert.Equal(t, "transactional", harness.UserName(t, "shard0-primary", 390, 2))
	harness.AssertUserAbsent(t, "shard1-primary", 390, 2)
}
