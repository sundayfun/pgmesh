//go:build integration

package commands_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/sundayfun/pgmesh/tests/command_shapes/internal"
	"github.com/sundayfun/pgmesh/tests/command_shapes/store"
)

func TestPostgresGeneratedBatchCommands(t *testing.T) {
	harness := newCommandHarness(t)

	batch := harness.commands.BatchInsertCommandUsers(
		t.Context(),
		[]*db.BatchInsertCommandUsersParams{
			{ID: 710, TenantID: 71, Name: "batch-a"},
			{ID: 711, TenantID: 72, Name: "batch-b"},
		},
	)
	var batchErrors []error
	batch.Exec(func(_ int, err error) {
		batchErrors = append(batchErrors, err)
	})
	require.Len(t, batchErrors, 2)
	for _, batchErr := range batchErrors {
		require.NoError(t, batchErr)
	}
	assertCommandUserName(t, harness.primaryTx, 710, "batch-a")
	assertCommandUserName(t, harness.primaryTx, 711, "batch-b")
	assertCommandUserAbsent(t, harness.mirrorTx, 710)
	assertCommandUserAbsent(t, harness.mirrorTx, 711)

	batchGet := harness.commands.BatchGetCommandUser(
		t.Context(),
		[]int64{710, 711},
		store.ReadFromPrimary(),
	)
	var batchGetNames []string
	batchGet.QueryRow(func(_ int, user *db.User, err error) {
		require.NoError(t, err)
		batchGetNames = append(batchGetNames, user.Name)
	})
	assert.Equal(t, []string{"batch-a", "batch-b"}, batchGetNames)

	batchList := harness.commands.BatchListCommandUsersByTenant(
		t.Context(),
		[]int64{71, 72},
		store.ReadFromPrimary(),
	)
	var batchListNames []string
	batchList.Query(func(_ int, users []*db.User, err error) {
		require.NoError(t, err)
		require.Len(t, users, 1)
		batchListNames = append(batchListNames, users[0].Name)
	})
	assert.Equal(t, []string{"batch-a", "batch-b"}, batchListNames)
}
