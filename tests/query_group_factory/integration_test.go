//go:build integration

package querygroupfactory

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sundayfun/pgmesh/tests/internal/storetest"
	fixture "github.com/sundayfun/pgmesh/tests/same_package"
)

type cachedUserKey struct {
	tenantID int64
	id       int64
}

type cachedUsersStore struct {
	fixture.Users

	mu    sync.Mutex
	users map[cachedUserKey]*fixture.User
	hits  int
}

func (s *cachedUsersStore) GetUser(
	ctx context.Context,
	arg *fixture.GetUserT,
	options ...fixture.QueryOption,
) (*fixture.User, error) {
	if len(options) != 0 {
		return s.Users.GetUser(ctx, arg, options...)
	}

	key := cachedUserKey{tenantID: arg.TenantID, id: arg.ID}
	s.mu.Lock()
	user := s.users[key]
	if user != nil {
		s.hits++
	}
	s.mu.Unlock()
	if user != nil {
		return user, nil
	}

	user, err := s.Users.GetUser(ctx, arg, fixture.ReadFromPrimary())
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.users[key] = user
	s.mu.Unlock()
	return user, nil
}

func (s *cachedUsersStore) UpdateUserName(
	ctx context.Context,
	arg *fixture.UpdateUserNameT,
	options ...fixture.QueryOption,
) (*fixture.User, error) {
	user, err := s.Users.UpdateUserName(ctx, arg, options...)
	if err != nil {
		return user, err
	}

	key := cachedUserKey{tenantID: arg.TenantID, id: arg.ID}
	s.mu.Lock()
	delete(s.users, key)
	s.mu.Unlock()
	return user, nil
}

func (s *cachedUsersStore) cacheHits() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits
}

func TestPostgresQueryGroupFactoryCacheBehavior(t *testing.T) {
	harness := storetest.New(t)
	harness.Reset(t)
	harness.Insert(t, "shard0-primary", 90, 2, "before")

	factoryCalls := 0
	var cachedUsers *cachedUsersStore
	queries := harness.NewShardedStore(
		t,
		fixture.WithUsersFactory(func(internalStore fixture.Users) fixture.Users {
			factoryCalls++
			cachedUsers = &cachedUsersStore{
				Users: internalStore,
				mu:    sync.Mutex{},
				users: make(map[cachedUserKey]*fixture.User),
				hits:  0,
			}
			return cachedUsers
		}),
	)

	require.Equal(t, 1, factoryCalls)
	firstUsers := queries.Users()
	assert.Same(t, firstUsers, queries.Users())
	assert.NotSame(t, cachedUsers, firstUsers, "generated telemetry retains a stable facade")

	arg := &fixture.GetUserT{TenantKey: storetest.TenantKey(2), ID: 90}
	first, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "before", first.Name)
	assert.Zero(t, cachedUsers.cacheHits())

	cached, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "before", cached.Name)
	assert.Equal(t, 1, cachedUsers.cacheHits())

	updated, err := queries.Users().UpdateUserName(
		t.Context(),
		&fixture.UpdateUserNameT{TenantKey: storetest.TenantKey(2), ID: 90, Name: "after"},
	)
	require.NoError(t, err)
	assert.Equal(t, "after", updated.Name)

	refreshed, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "after", refreshed.Name)
	assert.Equal(t, 1, cachedUsers.cacheHits())

	_, err = harness.Pool("shard0-primary").Exec(
		t.Context(),
		"UPDATE users SET name = 'fresh' WHERE id = 90 AND tenant_id = 2",
	)
	require.NoError(t, err)

	stale, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "after", stale.Name)
	assert.Equal(t, 2, cachedUsers.cacheHits())

	strong, err := queries.Users().GetUser(t.Context(), arg, fixture.ReadFromPrimary())
	require.NoError(t, err)
	assert.Equal(t, "fresh", strong.Name)
	assert.Equal(t, 2, cachedUsers.cacheHits())
}
