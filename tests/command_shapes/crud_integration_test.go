//go:build integration

package commands_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresGeneratedCRUDCommands(t *testing.T) {
	harness := newCommandHarness(t)
	harness.insert(t, 700, 70, "first")
	harness.insert(t, 701, 70, "second")

	got, err := harness.commands.GetCommandUser(t.Context(), 700)
	require.NoError(t, err)
	assert.Equal(t, "first", got.Name)
	users, err := harness.commands.ListCommandUsers(t.Context())
	require.NoError(t, err)
	require.Len(t, users, 2)

	tag, err := harness.commands.TouchCommandUser(t.Context(), 700)
	require.NoError(t, err)
	assert.Equal(t, int64(1), tag.RowsAffected())
	require.NoError(t, harness.commands.DeleteCommandUser(t.Context(), 701))
	assertCommandUserAbsent(t, harness.primaryTx, 701)
	assertCommandUserAbsent(t, harness.mirrorTx, 701)

	deleted, err := harness.commands.DeleteCommandUsersByTenant(t.Context(), 70)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	assertCommandUserAbsent(t, harness.primaryTx, 700)
	assertCommandUserAbsent(t, harness.mirrorTx, 700)
}
