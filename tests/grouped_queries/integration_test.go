//go:build integration

package groupedqueries

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresGroupedLookupRestoresInputOrder(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewShardedStore(t)

	harness.Insert(t, "shard0-primary", 240, 2, "even-zero")
	harness.Insert(t, "shard0-primary", 242, 4, "even-two")
	harness.Insert(t, "shard1-primary", 241, 3, "odd-one")
	harness.Insert(t, "shard1-primary", 243, 5, "odd-three")

	users, err := queries.Users().ListUsersByIDs(
		t.Context(),
		[]*fixture.ListUsersByIDsT{
			{TenantKey: storetest.TenantKey(5), ID: 243},
			{TenantKey: storetest.TenantKey(2), ID: 240},
			{TenantKey: storetest.TenantKey(3), ID: 299},
			{TenantKey: storetest.TenantKey(3), ID: 241},
			{TenantKey: storetest.TenantKey(4), ID: 242},
			{TenantKey: storetest.TenantKey(2), ID: 240},
		},
		fixture.ReadFromPrimary(),
	)
	require.NoError(t, err)
	require.Len(t, users, 4)
	assert.Equal(t, []int64{243, 240, 241, 242}, []int64{
		users[0].ID,
		users[1].ID,
		users[2].ID,
		users[3].ID,
	})
}
