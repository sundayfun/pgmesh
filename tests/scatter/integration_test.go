//go:build integration

package scatter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh"
	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	"github.com/sundayfun/pgmesh/tests/internal/testdb"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresAllShardQueriesMergeAndAggregate(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	harness.Insert(t, "shard0-primary", 260, 2, "scatter")
	harness.Insert(t, "shard1-primary", 261, 3, "scatter")

	users, err := queries.Users().ListAllUsers(t.Context(), fixture.ReadFromPrimary())
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, []int64{260, 261}, []int64{users[0].ID, users[1].ID})

	count, err := queries.Users().DeleteAllUsersByName(t.Context(), "scatter")
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
	harness.AssertUserAbsent(t, "shard0-primary", 260, 2)
	harness.AssertUserAbsent(t, "shard1-primary", 261, 3)

	tx, err := harness.Pool("shard0-primary").Begin(t.Context())
	require.NoError(t, err)
	defer testdb.Rollback(t, tx)
	err = queries.Users().DeleteAllUsers(t.Context(), fixture.WithTx(tx))
	require.ErrorIs(t, err, pgmesh.ErrCrossShardTransaction)
}
