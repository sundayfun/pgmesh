package commands_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/sundayfun/pgmesh/tests/command_shapes/internal"
	"github.com/sundayfun/pgmesh/tests/command_shapes/store"
)

func TestGeneratedCommandStore(t *testing.T) {
	t.Parallel()

	params := db.CopyCommandUsersParams{
		ID:       10,
		TenantID: 20,
		Name:     "generated",
	}
	user := db.User(params)

	assert.Equal(t, params.ID, user.ID)
	assert.Equal(t, params.TenantID, user.TenantID)
	assert.Equal(t, params.Name, user.Name)

	_, err := store.NewStore(context.Background(), nil)
	require.ErrorContains(t, err, "topology is nil")
}
