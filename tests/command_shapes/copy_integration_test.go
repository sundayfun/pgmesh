//go:build integration

package commands_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/sundayfun/pgmesh/tests/command_shapes/internal"
)

func TestPostgresGeneratedCopyCommand(t *testing.T) {
	harness := newCommandHarness(t)

	copyCount, err := harness.commands.CopyCommandUsers(
		t.Context(),
		[]*db.CopyCommandUsersParams{
			{ID: 700, TenantID: 70, Name: "copy-a"},
			{ID: 701, TenantID: 70, Name: "copy-b"},
		},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(2), copyCount)
	assertCommandUserName(t, harness.primaryTx, 700, "copy-a")
	assertCommandUserName(t, harness.mirrorTx, 700, "copy-a")
	assertCommandUserName(t, harness.primaryTx, 701, "copy-b")
	assertCommandUserName(t, harness.mirrorTx, 701, "copy-b")
}
