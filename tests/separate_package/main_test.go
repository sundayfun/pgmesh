package separatepackage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sundayfun/pgmesh/tests/separate_package/store"
)

func TestExportedSQLCTypes(t *testing.T) {
	t.Parallel()

	params := store.CreateUserParams{
		ID:       10,
		TenantID: 20,
		Name:     "re-exported",
	}
	user := store.User(params)

	assert.Equal(t, params.ID, user.ID)
	assert.Equal(t, params.TenantID, user.TenantID)
	assert.Equal(t, params.Name, user.Name)
}
