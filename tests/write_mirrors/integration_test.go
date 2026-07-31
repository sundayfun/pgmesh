//go:build integration

package writemirrors

import (
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

func TestPostgresSingletonWriteMirror(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries, err := fixture.NewStore(
		t.Context(),
		fixture.Singleton(
			harness.Pool("shard0-primary"),
			fixture.WithDatabaseName("singleton-mirror"),
			fixture.WithWriteMirrors(harness.Pool("shard0-mirror")),
		),
	)
	require.NoError(t, err)

	_, err = queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(2), ID: 151, Name: "mirrored"},
	)
	require.NoError(t, err)

	assert.Equal(t, "mirrored", harness.UserName(t, "shard0-primary", 151, 2))
	assert.Equal(t, "mirrored", harness.UserName(t, "shard0-mirror", 151, 2))
}

func TestPostgresShardedWriteMirrorAppliesOnlyToMappedShard(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewMirroredShardedStore(t)

	_, err := queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(2), ID: 200, Name: "even"},
	)
	require.NoError(t, err)
	_, err = queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(3), ID: 201, Name: "odd"},
	)
	require.NoError(t, err)

	assert.Equal(t, "even", harness.UserName(t, "shard0-primary", 200, 2))
	assert.Equal(t, "even", harness.UserName(t, "shard0-mirror", 200, 2))
	assert.Equal(t, "odd", harness.UserName(t, "shard1-primary", 201, 3))
	harness.AssertUserAbsent(t, "shard0-mirror", 201, 3)
}

func TestPostgresRoutedCopyMirrorsOnlyMappedShard(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewMirroredShardedStore(t)

	count, err := queries.Users().CopyUsers(t.Context(), []*fixture.CopyUsersT{
		{TenantKey: storetest.TenantKey(2), ID: 250, Name: "even-two"},
		{TenantKey: storetest.TenantKey(3), ID: 251, Name: "odd"},
		{TenantKey: storetest.TenantKey(4), ID: 252, Name: "even-four"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	assert.Equal(t, "even-two", harness.UserName(t, "shard0-mirror", 250, 2))
	assert.Equal(t, "even-four", harness.UserName(t, "shard0-mirror", 252, 4))
	harness.AssertUserAbsent(t, "shard0-mirror", 251, 3)
}

func TestPostgresMirrorFailurePreservesPrimaryResult(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewMirroredShardedStore(t)
	harness.Insert(t, "shard0-mirror", 300, 2, "existing")

	user, err := queries.Users().CreateUser(
		t.Context(),
		&fixture.CreateUserT{TenantKey: storetest.TenantKey(2), ID: 300, Name: "primary-result"},
	)
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "23505", pgErr.Code)
	require.NotNil(t, user)
	assert.Equal(t, "primary-result", user.Name)
	assert.Equal(t, "primary-result", harness.UserName(t, "shard0-primary", 300, 2))
	assert.Equal(t, "existing", harness.UserName(t, "shard0-mirror", 300, 2))
}

func TestPostgresMirrorMissingUpdateRowIsIgnored(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	queries := harness.NewMirroredShardedStore(t)
	harness.Insert(t, "shard0-primary", 350, 2, "before")

	user, err := queries.Users().UpdateUserName(
		t.Context(),
		&fixture.UpdateUserNameT{TenantKey: storetest.TenantKey(2), ID: 350, Name: "after"},
	)
	require.NoError(t, err)
	require.NotNil(t, user)
	assert.Equal(t, "after", user.Name)
	assert.Equal(t, "after", harness.UserName(t, "shard0-primary", 350, 2))
	harness.AssertUserAbsent(t, "shard0-mirror", 350, 2)
}
