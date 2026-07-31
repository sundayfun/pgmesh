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

// TestPostgresMixedTransactionCopyAndMirrorSuppression is intentionally the
// one cross-feature runtime test. Focused tests cover each component; this test
// proves they compose without widening a transaction beyond one physical shard.
func TestPostgresMixedTransactionCopyAndMirrorSuppression(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewMirroredShardedStore(t)
	tx, err := harness.Pool("shard0-primary").Begin(t.Context())
	require.NoError(t, err)
	defer testdb.Rollback(t, tx)

	created, err := queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(2), ID: 400, Name: "transactional"},
		fixture.WithTx(tx),
	)
	require.NoError(t, err)
	assert.Equal(t, "transactional", created.Name)

	copyCount, err := queries.Users().CopyUsers(
		t.Context(),
		[]*fixture.CopyUsersT{
			{TenantKey: storetest.TenantKey(2), ID: 401, Name: "copy-two"},
			{TenantKey: storetest.TenantKey(4), ID: 402, Name: "copy-four"},
		},
		fixture.WithTx(tx),
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), copyCount)

	inside, err := queries.Users().GetUser(
		t.Context(),
		&fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 400},
		fixture.WithTx(tx),
	)
	require.NoError(t, err)
	assert.Equal(t, "transactional", inside.Name)
	require.NoError(t, tx.Commit(t.Context()))

	assert.Equal(t, "transactional", harness.UserName(t, "shard0-primary", 400, 2))
	assert.Equal(t, "copy-two", harness.UserName(t, "shard0-primary", 401, 2))
	assert.Equal(t, "copy-four", harness.UserName(t, "shard0-primary", 402, 4))
	harness.AssertUserAbsent(t, "shard0-mirror", 400, 2)
	harness.AssertUserAbsent(t, "shard0-mirror", 401, 2)
	harness.AssertUserAbsent(t, "shard0-mirror", 402, 4)
}
