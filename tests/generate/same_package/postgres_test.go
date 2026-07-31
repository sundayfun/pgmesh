//go:build integration

package samepackage_test

import (
	"context"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sundayfun/pgmesh"
	fixture "github.com/sundayfun/pgmesh/tests/generate/same_package"
	"github.com/sundayfun/pgmesh/tests/integration/testdb"
)

type tenantResolver struct{}

func (tenantResolver) TenantKey(key fixture.TenantKey) uint64 {
	return uint64(key.TenantID)
}

func (tenantResolver) MessageKey(key fixture.MessageKey) uint64 {
	return uint64(key.UserID)
}

func tenantKey(tenantID int64) fixture.TenantKey {
	return fixture.TenantKey{TenantID: tenantID}
}

type postgresHarness struct {
	pools map[string]*pgxpool.Pool
}

type postgresCase struct {
	*postgresHarness
	queries fixture.Store
}

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

	key := cachedUserKey{tenantID: arg.TenantKey.TenantID, id: arg.ID}
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

	key := cachedUserKey{tenantID: arg.TenantKey.TenantID, id: arg.ID}
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

func newPostgresHarness(t *testing.T) *postgresHarness {
	t.Helper()
	if !testdb.Enabled() {
		t.Skipf("set %s=1 and start tests/integration/docker-compose.yaml", testdb.IntegrationEnv)
	}

	endpoints := testdb.DefaultEndpoints()
	dsns := make(map[string]string, len(endpoints))
	for _, endpoint := range endpoints {
		dsn, err := endpoint.DSN()
		require.NoError(t, err, "resolve DSN for %s", endpoint.Name)
		dsns[endpoint.Name] = dsn
	}

	pools := make(map[string]*pgxpool.Pool, len(dsns))
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Close()
		}
	})
	for name, dsn := range dsns {
		pool, err := testdb.OpenPool(t.Context(), dsn)
		require.NoError(t, err, "open pool for %s", name)
		pools[name] = pool
	}

	return &postgresHarness{pools: pools}
}

func (h *postgresHarness) newShardedStore(
	t *testing.T,
	options ...fixture.StoreOption,
) fixture.Store {
	t.Helper()
	queries, err := fixture.NewStore(
		t.Context(),
		fixture.Sharded(
			2,
			pgmesh.ModularShardHashFor[uint64](2),
			tenantResolver{},
			fixture.WithReplicaSet(
				"shard0",
				h.pools["shard0-primary"],
				h.pools["shard0-replica0"],
				h.pools["shard0-replica1"],
			),
			fixture.WithReplicaSet("shard1", h.pools["shard1-primary"]),
			fixture.WithReplicaSet("shard0-mirror", h.pools["shard0-mirror"]),
			fixture.WithVShardMapping("shard0", []uint64{0}, "shard0-mirror"),
			fixture.WithVShardMapping("shard1", []uint64{1}),
		),
		options...,
	)
	require.NoError(t, err)
	return queries
}

func (h *postgresHarness) reset(t *testing.T) {
	t.Helper()
	for name, pool := range h.pools {
		_, err := pool.Exec(t.Context(), "TRUNCATE TABLE analyses, users")
		require.NoError(t, err, "truncate %s", name)
	}
}

func (h *postgresHarness) insert(t *testing.T, database string, id, tenantID int64, name string) {
	t.Helper()
	_, err := h.pools[database].Exec(
		t.Context(),
		"INSERT INTO users (id, tenant_id, name) VALUES ($1, $2, $3)",
		id,
		tenantID,
		name,
	)
	require.NoError(t, err)
}

func (h *postgresHarness) userName(t *testing.T, database string, id, tenantID int64) string {
	t.Helper()
	var name string
	err := h.pools[database].QueryRow(
		t.Context(),
		"SELECT name FROM users WHERE id = $1 AND tenant_id = $2",
		id,
		tenantID,
	).Scan(&name)
	require.NoError(t, err, "read user from %s", database)
	return name
}

func (h *postgresHarness) assertUserAbsent(t *testing.T, database string, id, tenantID int64) {
	t.Helper()
	var ignored int64
	err := h.pools[database].QueryRow(
		t.Context(),
		"SELECT id FROM users WHERE id = $1 AND tenant_id = $2",
		id,
		tenantID,
	).Scan(&ignored)
	require.ErrorIs(t, err, pgx.ErrNoRows, "user unexpectedly exists in %s", database)
}

func TestPostgresStoreFactoryIntegration(t *testing.T) {
	harness := newPostgresHarness(t)
	harness.reset(t)
	harness.insert(t, "shard0-primary", 90, 2, "before")

	factoryCalls := 0
	var cachedUsers *cachedUsersStore
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	queries := harness.newShardedStore(
		t,
		fixture.WithTracerProvider(tracerProvider),
		fixture.WithUsersFactory(func(internalStore fixture.Users) fixture.Users {
			factoryCalls++
			cachedUsers = &cachedUsersStore{
				Users: internalStore,
				users: make(map[cachedUserKey]*fixture.User),
			}
			return cachedUsers
		}),
	)

	require.Equal(t, 1, factoryCalls)
	firstUsers := queries.Users()
	assert.Same(t, firstUsers, queries.Users())
	assert.NotSame(t, cachedUsers, firstUsers, "generated telemetry retains a stable facade")

	arg := &fixture.GetUserT{TenantKey: tenantKey(2), ID: 90}
	first, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "before", first.Name)
	assert.Zero(t, cachedUsers.cacheHits())

	updated, err := queries.Users().UpdateUserName(
		t.Context(),
		&fixture.UpdateUserNameT{TenantKey: tenantKey(2), ID: 90, Name: "after"},
	)
	require.NoError(t, err)
	assert.Equal(t, "after", updated.Name)
	assert.Equal(t, "after", harness.userName(t, "shard0-primary", 90, 2))

	refreshed, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "after", refreshed.Name)
	assert.Zero(t, cachedUsers.cacheHits())

	_, err = harness.pools["shard0-primary"].Exec(
		t.Context(),
		"UPDATE users SET name = 'fresh' WHERE id = 90 AND tenant_id = 2",
	)
	require.NoError(t, err)

	cached, err := queries.Users().GetUser(t.Context(), arg)
	require.NoError(t, err)
	assert.Equal(t, "after", cached.Name)
	assert.Equal(t, 1, cachedUsers.cacheHits())

	fresh, err := queries.Users().GetUser(t.Context(), arg, fixture.ReadFromPrimary())
	require.NoError(t, err)
	assert.Equal(t, "fresh", fresh.Name)
	assert.Equal(t, 1, cachedUsers.cacheHits())

	spans := recorder.Ended()
	require.Len(t, spans, 9)
	storeSpans := make([]sdktrace.ReadOnlySpan, 0, 5)
	querySpans := make([]sdktrace.ReadOnlySpan, 0, 4)
	for _, span := range spans {
		switch {
		case strings.HasPrefix(span.Name(), "pgmesh.query.wrapper."):
			storeSpans = append(storeSpans, span)
		case strings.HasPrefix(span.Name(), "pgmesh.query.physical."):
			querySpans = append(querySpans, span)
		default:
			require.Failf(t, "unexpected span", "%s", span.Name())
		}
	}
	require.Len(t, storeSpans, 5)
	require.Len(t, querySpans, 4)

	internalExecutions := make([]bool, 0, len(storeSpans))
	storeSpanIDs := make(map[string]struct{}, len(storeSpans))
	for _, span := range storeSpans {
		storeSpanIDs[span.SpanContext().SpanID().String()] = struct{}{}
		attributes := make(map[string]any, len(span.Attributes()))
		for _, item := range span.Attributes() {
			attributes[string(item.Key)] = item.Value.AsInterface()
		}
		executed, ok := attributes[pgmesh.AttributeWrapperDelegated].(bool)
		require.True(t, ok)
		internalExecutions = append(internalExecutions, executed)
		if !executed {
			assert.NotContains(t, attributes, pgmesh.AttributeShardName)
			assert.NotContains(t, attributes, pgmesh.AttributeRouteMode)
		}
	}
	assert.Equal(t, []bool{true, true, true, false, true}, internalExecutions)
	for _, span := range querySpans {
		attributes := make(map[string]any, len(span.Attributes()))
		for _, item := range span.Attributes() {
			attributes[string(item.Key)] = item.Value.AsInterface()
		}
		assert.NotContains(t, attributes, pgmesh.AttributeWrapperDelegated)
		assert.Contains(t, storeSpanIDs, span.Parent().SpanID().String())
	}
}

func TestPostgresTopologyIntegration(t *testing.T) {
	harness := newPostgresHarness(t)

	tests := []struct {
		name string
		run  func(*testing.T, *postgresCase)
	}{
		{
			name: "round robin replicas and primary fallback",
			run: func(t *testing.T, h *postgresCase) {
				h.insert(t, "shard0-primary", 100, 2, "primary")
				h.insert(t, "shard0-replica0", 100, 2, "replica0")
				h.insert(t, "shard0-replica1", 100, 2, "replica1")
				h.insert(t, "shard1-primary", 101, 3, "shard1-primary")

				first, err := h.queries.Users().GetUser(t.Context(), &fixture.GetUserT{TenantKey: tenantKey(2), ID: 100})
				require.NoError(t, err)
				second, err := h.queries.Users().GetUser(t.Context(), &fixture.GetUserT{TenantKey: tenantKey(2), ID: 100})
				require.NoError(t, err)
				strong, err := h.queries.Users().GetUser(
					t.Context(),
					&fixture.GetUserT{TenantKey: tenantKey(2), ID: 100},
					fixture.ReadFromPrimary(),
				)
				require.NoError(t, err)
				fallback, err := h.queries.Users().GetUser(t.Context(), &fixture.GetUserT{TenantKey: tenantKey(3), ID: 101})
				require.NoError(t, err)

				assert.Equal(t, "replica0", first.Name)
				assert.Equal(t, "replica1", second.Name)
				assert.Equal(t, "primary", strong.Name)
				assert.Equal(t, "shard1-primary", fallback.Name)
			},
		},
		{
			name: "singleton routes replicas primary reads and mirrors",
			run: func(t *testing.T, h *postgresCase) {
				queries, err := fixture.NewStore(
					t.Context(),
					fixture.Singleton(
						h.pools["shard0-primary"],
						fixture.WithDatabaseName("singleton"),
						fixture.WithReadReplicas(
							h.pools["shard0-replica0"],
							h.pools["shard0-replica1"],
						),
						fixture.WithWriteMirrors(h.pools["shard0-mirror"]),
					),
				)
				require.NoError(t, err)

				h.insert(t, "shard0-primary", 150, 2, "primary")
				h.insert(t, "shard0-replica0", 150, 2, "replica0")
				h.insert(t, "shard0-replica1", 150, 2, "replica1")

				first, err := queries.Users().GetUser(
					t.Context(),
					&fixture.GetUserT{TenantKey: tenantKey(2), ID: 150},
				)
				require.NoError(t, err)
				second, err := queries.Users().GetUser(
					t.Context(),
					&fixture.GetUserT{TenantKey: tenantKey(2), ID: 150},
				)
				require.NoError(t, err)
				strong, err := queries.Users().GetUser(
					t.Context(),
					&fixture.GetUserT{TenantKey: tenantKey(2), ID: 150},
					fixture.ReadFromPrimary(),
				)
				require.NoError(t, err)
				_, err = queries.Users().CreateUser(
					t.Context(),
					&fixture.CreateUserT{TenantKey: tenantKey(2), ID: 151, Name: "mirrored"},
				)
				require.NoError(t, err)

				assert.Equal(t, "replica0", first.Name)
				assert.Equal(t, "replica1", second.Name)
				assert.Equal(t, "primary", strong.Name)
				assert.Equal(t, "mirrored", h.userName(t, "shard0-primary", 151, 2))
				assert.Equal(t, "mirrored", h.userName(t, "shard0-mirror", 151, 2))
				h.assertUserAbsent(t, "shard0-replica0", 151, 2)
				h.assertUserAbsent(t, "shard0-replica1", 151, 2)
			},
		},
		{
			name: "writes route by virtual shard and mirror only shard zero",
			run: func(t *testing.T, h *postgresCase) {
				_, err := h.queries.Users().CreateUser(t.Context(), &fixture.CreateUserT{TenantKey: tenantKey(2), ID: 200, Name: "even"})
				require.NoError(t, err)
				_, err = h.queries.Users().CreateUser(t.Context(), &fixture.CreateUserT{TenantKey: tenantKey(3), ID: 201, Name: "odd"})
				require.NoError(t, err)

				assert.Equal(t, "even", h.userName(t, "shard0-primary", 200, 2))
				assert.Equal(t, "even", h.userName(t, "shard0-mirror", 200, 2))
				h.assertUserAbsent(t, "shard0-replica0", 200, 2)
				h.assertUserAbsent(t, "shard0-replica1", 200, 2)
				assert.Equal(t, "odd", h.userName(t, "shard1-primary", 201, 3))
				h.assertUserAbsent(t, "shard0-primary", 201, 3)
				h.assertUserAbsent(t, "shard0-mirror", 201, 3)
			},
		},
		{
			name: "grouped copy sends one physical batch per shard",
			run: func(t *testing.T, h *postgresCase) {
				count, err := h.queries.Users().CopyUsers(t.Context(), []*fixture.CopyUsersT{
					{TenantKey: tenantKey(2), ID: 250, Name: "even-two"},
					{TenantKey: tenantKey(3), ID: 251, Name: "odd"},
					{TenantKey: tenantKey(4), ID: 252, Name: "even-four"},
				})
				require.NoError(t, err)
				assert.Equal(t, int64(3), count)

				assert.Equal(t, "even-two", h.userName(t, "shard0-primary", 250, 2))
				assert.Equal(t, "even-four", h.userName(t, "shard0-primary", 252, 4))
				assert.Equal(t, "even-two", h.userName(t, "shard0-mirror", 250, 2))
				assert.Equal(t, "even-four", h.userName(t, "shard0-mirror", 252, 4))
				assert.Equal(t, "odd", h.userName(t, "shard1-primary", 251, 3))
				h.assertUserAbsent(t, "shard1-primary", 250, 2)
				h.assertUserAbsent(t, "shard0-primary", 251, 3)
			},
		},
		{
			name: "async copy batches concurrent callers and drains explicitly",
			run: func(t *testing.T, h *postgresCase) {
				queries := h.newShardedStore(
					t,
					fixture.WithCopyUsersBatching(pgmesh.CopyBatchConfig{
						BatchSize:    8,
						FlushTimeout: time.Hour,
					}),
				)
				futures := make([]*pgmesh.Future[int64], 0, 4)
				for index, tenantID := range []int64{2, 3, 4, 5} {
					futures = append(futures, queries.Users().EnqueueCopyUsers(
						t.Context(),
						[]*fixture.CopyUsersT{{
							TenantKey: tenantKey(tenantID),
							ID:        int64(270 + index),
							Name:      "async-copy",
						}},
					))
				}
				require.NoError(t, queries.Users().FlushCopyUsers(t.Context()))
				for _, future := range futures {
					count, err := future.Await(t.Context())
					require.NoError(t, err)
					assert.Equal(t, int64(1), count)
				}

				assert.Equal(t, "async-copy", h.userName(t, "shard0-primary", 270, 2))
				assert.Equal(t, "async-copy", h.userName(t, "shard0-mirror", 270, 2))
				assert.Equal(t, "async-copy", h.userName(t, "shard1-primary", 271, 3))
				assert.Equal(t, "async-copy", h.userName(t, "shard0-primary", 272, 4))
				assert.Equal(t, "async-copy", h.userName(t, "shard0-mirror", 272, 4))
				assert.Equal(t, "async-copy", h.userName(t, "shard1-primary", 273, 5))
			},
		},
		{
			name: "grouped many routes lookup values and restores input order",
			run: func(t *testing.T, h *postgresCase) {
				h.insert(t, "shard0-primary", 240, 2, "even-zero")
				h.insert(t, "shard0-primary", 242, 4, "even-two")
				h.insert(t, "shard1-primary", 241, 3, "odd-one")
				h.insert(t, "shard1-primary", 243, 5, "odd-three")

				users, err := h.queries.Users().ListUsersByIDs(
					t.Context(),
					[]*fixture.ListUsersByIDsT{
						{TenantKey: tenantKey(5), ID: 243},
						{TenantKey: tenantKey(2), ID: 240},
						{TenantKey: tenantKey(3), ID: 299},
						{TenantKey: tenantKey(3), ID: 241},
						{TenantKey: tenantKey(4), ID: 242},
						{TenantKey: tenantKey(2), ID: 240},
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
			},
		},
		{
			name: "all-shards reads merge and writes affect every physical shard",
			run: func(t *testing.T, h *postgresCase) {
				h.insert(t, "shard0-primary", 260, 2, "scatter")
				h.insert(t, "shard0-mirror", 260, 2, "scatter")
				h.insert(t, "shard1-primary", 261, 3, "scatter")

				users, err := h.queries.Users().ListAllUsers(
					t.Context(),
					fixture.ReadFromPrimary(),
				)
				require.NoError(t, err)
				require.Len(t, users, 2)
				assert.Equal(t, []int64{260, 261}, []int64{users[0].ID, users[1].ID})

				count, err := h.queries.Users().DeleteAllUsersByName(t.Context(), "scatter")
				require.NoError(t, err)
				assert.Equal(t, int64(2), count)
				h.assertUserAbsent(t, "shard0-primary", 260, 2)
				h.assertUserAbsent(t, "shard0-mirror", 260, 2)
				h.assertUserAbsent(t, "shard1-primary", 261, 3)

				tx, err := h.pools["shard0-primary"].Begin(t.Context())
				require.NoError(t, err)
				defer func() { _ = tx.Rollback(context.Background()) }()
				err = h.queries.Users().DeleteAllUsers(t.Context(), fixture.WithTx(tx))
				require.ErrorIs(t, err, pgmesh.ErrCrossShardTransaction)
			},
		},
		{
			name: "mirror error preserves committed primary result",
			run: func(t *testing.T, h *postgresCase) {
				h.insert(t, "shard0-mirror", 300, 2, "existing")

				user, err := h.queries.Users().CreateUser(
					t.Context(),
					&fixture.CreateUserT{TenantKey: tenantKey(2), ID: 300, Name: "primary-result"},
				)
				require.Error(t, err)
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, "23505", pgErr.Code)
				require.NotNil(t, user)
				assert.Equal(t, "primary-result", user.Name)
				assert.Equal(t, "primary-result", h.userName(t, "shard0-primary", 300, 2))
				assert.Equal(t, "existing", h.userName(t, "shard0-mirror", 300, 2))
			},
		},
		{
			name: "missing mirror update row is ignored",
			run: func(t *testing.T, h *postgresCase) {
				h.insert(t, "shard0-primary", 350, 2, "before")

				user, err := h.queries.Users().UpdateUserName(
					t.Context(),
					&fixture.UpdateUserNameT{TenantKey: tenantKey(2), ID: 350, Name: "after"},
				)

				require.NoError(t, err)
				require.NotNil(t, user)
				assert.Equal(t, "after", user.Name)
				assert.Equal(t, "after", h.userName(t, "shard0-primary", 350, 2))
				h.assertUserAbsent(t, "shard0-mirror", 350, 2)
			},
		},
		{
			name: "analysis scans nullable network and range types",
			run: func(t *testing.T, h *postgresCase) {
				_, err := h.pools["shard0-primary"].Exec(
					t.Context(),
					`INSERT INTO analyses (id, tenant_id, summary, state, source, active_window)
					 VALUES
					 (360, 2, 'ready', 'complete', '192.0.2.10', '[2026-01-02 03:04:05+00,2026-01-03 03:04:05+00)'),
					 (361, 2, NULL, NULL, '2001:db8::10', '[2026-02-02 03:04:05+00,2026-02-03 03:04:05+00)')`,
				)
				require.NoError(t, err)

				populated, err := h.queries.Analyses().GetAnalysis(
					t.Context(),
					&fixture.GetAnalysisT{TenantKey: tenantKey(2), ID: 360},
					fixture.ReadFromPrimary(),
				)
				require.NoError(t, err)
				require.NotNil(t, populated.Description)
				assert.Equal(t, "ready", *populated.Description)
				assert.True(t, populated.State.Valid)
				assert.Equal(t, fixture.AnalysisStateComplete, populated.State.AnalysisState)
				assert.Equal(t, netip.MustParseAddr("192.0.2.10"), populated.Source)
				assert.True(t, populated.ActiveWindow.Valid)
				assert.Equal(t, "2026-01-02T03:04:05Z", populated.ActiveWindow.Lower.Time.UTC().Format(time.RFC3339))
				assert.Equal(t, "2026-01-03T03:04:05Z", populated.ActiveWindow.Upper.Time.UTC().Format(time.RFC3339))

				nullable, err := h.queries.Analyses().GetAnalysis(
					t.Context(),
					&fixture.GetAnalysisT{TenantKey: tenantKey(2), ID: 361},
					fixture.ReadFromPrimary(),
				)
				require.NoError(t, err)
				assert.Nil(t, nullable.Description)
				assert.False(t, nullable.State.Valid)
				assert.Equal(t, netip.MustParseAddr("2001:db8::10"), nullable.Source)
				assert.True(t, nullable.ActiveWindow.Valid)
			},
		},
		{
			name: "transaction pins primary and disables mirror",
			run: func(t *testing.T, h *postgresCase) {
				tx, err := h.pools["shard0-primary"].Begin(t.Context())
				require.NoError(t, err)
				defer func() { _ = tx.Rollback(context.Background()) }()

				created, err := h.queries.Users().CreateUser(
					t.Context(),
					&fixture.CreateUserT{TenantKey: tenantKey(2), ID: 400, Name: "transactional"},
					fixture.WithTx(tx),
				)
				require.NoError(t, err)
				assert.Equal(t, "transactional", created.Name)
				copyCount, err := h.queries.Users().CopyUsers(
					t.Context(),
					[]*fixture.CopyUsersT{
						{TenantKey: tenantKey(2), ID: 401, Name: "copy-two"},
						{TenantKey: tenantKey(4), ID: 402, Name: "copy-four"},
					},
					fixture.WithTx(tx),
				)
				require.NoError(t, err)
				assert.Equal(t, int64(2), copyCount)
				inside, err := h.queries.Users().GetUser(
					t.Context(),
					&fixture.GetUserT{TenantKey: tenantKey(2), ID: 400},
					fixture.WithTx(tx),
				)
				require.NoError(t, err)
				assert.Equal(t, "transactional", inside.Name)
				require.NoError(t, tx.Commit(t.Context()))

				assert.Equal(t, "transactional", h.userName(t, "shard0-primary", 400, 2))
				assert.Equal(t, "copy-two", h.userName(t, "shard0-primary", 401, 2))
				assert.Equal(t, "copy-four", h.userName(t, "shard0-primary", 402, 4))
				h.assertUserAbsent(t, "shard0-mirror", 400, 2)
				h.assertUserAbsent(t, "shard0-mirror", 401, 2)
				h.assertUserAbsent(t, "shard0-mirror", 402, 4)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness.reset(t)
			test.run(t, &postgresCase{
				postgresHarness: harness,
				queries:         harness.newShardedStore(t),
			})
		})
	}
}
