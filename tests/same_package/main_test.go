package samepackage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/sundayfun/pgmesh"
)

type callLog struct {
	mu    sync.Mutex
	calls []string
}

func (l *callLog) add(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, name)
}

func (l *callLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

type fakeDB struct {
	name         string
	log          *callLog
	rowErr       error
	queryErr     error
	execErr      error
	copyErr      error
	rowsAffected int64
	users        []*User

	mu         sync.Mutex
	copyCalls  int
	copiedRows [][]any
	queriedIDs [][]int64
	ignoreIDs  bool
}

func (db *fakeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	db.log.add(db.name)
	return pgconn.NewCommandTag(fmt.Sprintf("DELETE %d", db.rowsAffected)), db.execErr
}

func (db *fakeDB) Query(_ context.Context, _ string, args ...any) (pgx.Rows, error) {
	db.log.add(db.name)
	if db.queryErr != nil {
		return nil, db.queryErr
	}
	if db.users == nil {
		return nil, errors.New("fake rows are not configured")
	}
	users := db.users
	if len(args) == 1 {
		if ids, ok := args[0].([]int64); ok {
			db.mu.Lock()
			db.queriedIDs = append(db.queriedIDs, append([]int64(nil), ids...))
			db.mu.Unlock()
			if !db.ignoreIDs {
				requested := make(map[int64]struct{}, len(ids))
				for _, id := range ids {
					requested[id] = struct{}{}
				}
				users = nil
				for _, user := range db.users {
					if _, ok := requested[user.ID]; ok {
						users = append(users, user)
					}
				}
			}
		}
	}
	return &fakeRows{users: users}, nil
}

func (db *fakeDB) QueryRow(context.Context, string, ...any) pgx.Row {
	db.log.add(db.name)
	return fakeRow{err: db.rowErr}
}

func (db *fakeDB) CopyFrom(
	_ context.Context,
	_ pgx.Identifier,
	_ []string,
	source pgx.CopyFromSource,
) (int64, error) {
	db.log.add(db.name)
	db.mu.Lock()
	db.copyCalls++
	db.mu.Unlock()
	if db.copyErr != nil {
		return 0, db.copyErr
	}
	var count int64
	for source.Next() {
		values, err := source.Values()
		if err != nil {
			return 0, err
		}
		db.mu.Lock()
		db.copiedRows = append(db.copiedRows, append([]any(nil), values...))
		db.mu.Unlock()
		count++
	}
	if err := source.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func (db *fakeDB) copyCallCount() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.copyCalls
}

func (db *fakeDB) copied() [][]any {
	db.mu.Lock()
	defer db.mu.Unlock()
	rows := make([][]any, len(db.copiedRows))
	for index := range db.copiedRows {
		rows[index] = append([]any(nil), db.copiedRows[index]...)
	}
	return rows
}

func (db *fakeDB) queried() [][]int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	result := make([][]int64, len(db.queriedIDs))
	for index := range db.queriedIDs {
		result[index] = append([]int64(nil), db.queriedIDs[index]...)
	}
	return result
}

type fakeRow struct {
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) == 3 {
		id, ok := dest[0].(*int64)
		if !ok {
			return fmt.Errorf("destination 0 has type %T, want *int64", dest[0])
		}
		tenantID, ok := dest[1].(*int64)
		if !ok {
			return fmt.Errorf("destination 1 has type %T, want *int64", dest[1])
		}
		name, ok := dest[2].(*string)
		if !ok {
			return fmt.Errorf("destination 2 has type %T, want *string", dest[2])
		}
		*id = 10
		*tenantID = 20
		*name = "user"
	}
	return nil
}

type fakeRows struct {
	users  []*User
	index  int
	closed bool
}

func (r *fakeRows) Close() {
	r.closed = true
}

func (r *fakeRows) Err() error {
	return nil
}

func (r *fakeRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}

func (r *fakeRows) Next() bool {
	if r.index >= len(r.users) {
		r.Close()
		return false
	}
	r.index++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.users) {
		return errors.New("fake rows scan called without current row")
	}
	user := r.users[r.index-1]
	if len(dest) != 3 {
		return fmt.Errorf("got %d destinations, want 3", len(dest))
	}
	id, ok := dest[0].(*int64)
	if !ok {
		return fmt.Errorf("destination 0 has type %T, want *int64", dest[0])
	}
	tenantID, ok := dest[1].(*int64)
	if !ok {
		return fmt.Errorf("destination 1 has type %T, want *int64", dest[1])
	}
	name, ok := dest[2].(*string)
	if !ok {
		return fmt.Errorf("destination 2 has type %T, want *string", dest[2])
	}
	*id = user.ID
	*tenantID = user.TenantID
	*name = user.Name
	return nil
}

func (r *fakeRows) Values() ([]any, error) {
	if r.index == 0 || r.index > len(r.users) {
		return nil, errors.New("fake rows values called without current row")
	}
	user := r.users[r.index-1]
	return []any{user.ID, user.TenantID, user.Name}, nil
}

func (r *fakeRows) RawValues() [][]byte {
	return nil
}

func (r *fakeRows) Conn() *pgx.Conn {
	return nil
}

type fakeTx struct {
	*fakeDB
}

func (tx *fakeTx) Begin(context.Context) (pgx.Tx, error) {
	return tx, nil
}

func (tx *fakeTx) Commit(context.Context) error {
	return nil
}

func (tx *fakeTx) Rollback(context.Context) error {
	return nil
}

func (tx *fakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (tx *fakeTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (tx *fakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

func (tx *fakeTx) Conn() *pgx.Conn {
	return nil
}

type tenantResolver struct{}

func (tenantResolver) TenantKey(TenantKey) uint64 {
	return 0
}

func (tenantResolver) MessageKey(MessageKey) uint64 {
	return 0
}

type identityTenantResolver struct{}

func (identityTenantResolver) TenantKey(key TenantKey) uint64 {
	return uint64(key.TenantID) //nolint:gosec // Test fixtures use nonnegative IDs.
}

func (identityTenantResolver) MessageKey(key MessageKey) uint64 {
	return uint64(key.UserID) //nolint:gosec // Test fixtures use nonnegative IDs.
}

type identityShardHasher struct{}

func (identityShardHasher) Hash(key uint64) uint64 {
	return key
}

type recordingTenantResolver struct {
	tenantID *int64
}

func (r recordingTenantResolver) TenantKey(key TenantKey) uint64 {
	*r.tenantID = key.TenantID
	return 0
}

func (recordingTenantResolver) MessageKey(MessageKey) uint64 {
	return 0
}

type recordingMessageKeyResolver struct {
	userID          int64
	toUserOrGroupID int64
	inGroup         bool
}

func (recordingMessageKeyResolver) TenantKey(TenantKey) uint64 {
	return 0
}

func (r *recordingMessageKeyResolver) MessageKey(key MessageKey) uint64 {
	r.userID = key.UserID
	r.toUserOrGroupID = key.ToUserOrGroupID
	r.inGroup = key.InGroup
	return 0
}

type usersStoreWrapper struct {
	Users

	listed []*User
}

func (s *usersStoreWrapper) ListAllUsers(
	context.Context,
	...QueryOption,
) ([]*User, error) {
	return s.listed, nil
}

func buildTestStore(t *testing.T, primary, replica *fakeDB, mirrors ...*fakeDB) Store {
	t.Helper()

	options := []ShardedOption{WithReplicaSet("main", primary, replica)}
	mirrorNames := make([]string, 0, len(mirrors))
	for index, mirror := range mirrors {
		if mirror != nil {
			name := fmt.Sprintf("mirror-%d", index)
			options = append(options, WithReplicaSet(name, mirror))
			mirrorNames = append(mirrorNames, name)
		}
	}
	options = append(options, WithVShardMapping("main", []uint64{0}, mirrorNames...))
	store, err := NewStore(
		t.Context(),
		Sharded(
			1,
			pgmesh.ConstantShardHashFor[uint64](0),
			tenantResolver{},
			options...,
		),
	)
	require.NoError(t, err)
	return store
}

func TestGeneratedStoreFactoriesWrapSelectedGroupsOnce(t *testing.T) {
	t.Parallel()

	topologies := []struct {
		name   string
		create func(*fakeDB) Topology
	}{
		{
			name: "singleton",
			create: func(primary *fakeDB) Topology {
				return Singleton(primary)
			},
		},
		{
			name: "sharded",
			create: func(primary *fakeDB) Topology {
				return Sharded(
					1,
					pgmesh.ConstantShardHashFor[uint64](0),
					tenantResolver{},
					WithReplicaSet("main", primary),
					WithVShardMapping("main", []uint64{0}),
				)
			},
		},
	}

	for _, topology := range topologies {
		t.Run(topology.name, func(t *testing.T) {
			t.Parallel()

			log := &callLog{}
			primary := &fakeDB{
				name:  "primary",
				log:   log,
				users: []*User{{ID: 1, TenantID: 2, Name: "database"}},
			}
			cached := []*User{{ID: 10, TenantID: 20, Name: "cached"}}
			factoryCalls := 0
			var wrapper *usersStoreWrapper

			store, err := NewStore(
				t.Context(),
				topology.create(primary),
				WithUsersFactory(func(internalStore Users) Users {
					factoryCalls++
					wrapper = &usersStoreWrapper{Users: internalStore, listed: cached}
					return wrapper
				}),
			)
			require.NoError(t, err)
			assert.Equal(t, 1, factoryCalls)
			firstUsers := store.Users()
			assert.Same(t, firstUsers, store.Users())
			assert.NotSame(t, wrapper, firstUsers, "generated telemetry retains a stable facade")
			firstAnalyses := store.Analyses()
			secondAnalyses := store.Analyses()
			assert.Same(t, firstAnalyses, secondAnalyses)

			listed, err := store.Users().ListAllUsers(t.Context())
			require.NoError(t, err)
			assert.Equal(t, cached, listed)
			assert.Empty(t, log.snapshot())

			user, err := store.Users().GetUser(
				t.Context(),
				&GetUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1},
			)
			require.NoError(t, err)
			assert.Equal(t, int64(10), user.ID)
			assert.Equal(t, []string{"primary"}, log.snapshot())
		})
	}
}

func TestGeneratedStoreFactoryOptionsUseLastValue(t *testing.T) {
	t.Parallel()

	primary := &fakeDB{name: "primary", log: &callLog{}}
	firstCalls := 0
	secondCalls := 0
	firstFactory := WithUsersFactory(func(internalStore Users) Users {
		firstCalls++
		return &usersStoreWrapper{Users: internalStore}
	})
	var secondWrapper *usersStoreWrapper
	secondFactory := WithUsersFactory(func(internalStore Users) Users {
		secondCalls++
		secondWrapper = &usersStoreWrapper{Users: internalStore}
		return secondWrapper
	})

	store, err := NewStore(
		t.Context(),
		Singleton(primary),
		firstFactory,
		secondFactory,
	)
	require.NoError(t, err)
	assert.Zero(t, firstCalls)
	assert.Equal(t, 1, secondCalls)
	users := store.Users()
	assert.NotSame(t, secondWrapper, users, "generated telemetry wraps the selected factory")
	assert.Same(t, users, store.Users())

	cleared, err := NewStore(
		t.Context(),
		Singleton(primary),
		firstFactory,
		WithUsersFactory(nil),
	)
	require.NoError(t, err)
	assert.Zero(t, firstCalls)
	_, internal := cleared.Users().(*groupedMeshStore[uint8])
	assert.True(t, internal)
}

func TestGeneratedStoreFactoryRunsOnlyAfterSuccessfulTopologyBuild(t *testing.T) {
	t.Parallel()

	factoryCalls := 0
	_, err := NewStore(
		t.Context(),
		Singleton(nil),
		WithUsersFactory(func(internalStore Users) Users {
			factoryCalls++
			return internalStore
		}),
	)
	require.ErrorContains(t, err, "database primary is nil")
	assert.Zero(t, factoryCalls)
}

func buildTwoShardStore(
	t *testing.T,
	hasher pgmesh.ShardHasher[uint64],
	shardBPrimary *fakeDB,
	shardBReplica *fakeDB,
	shardAPrimary *fakeDB,
	shardAReplica *fakeDB,
	shardBMirror *fakeDB,
	storeOptions ...StoreOption,
) Store {
	t.Helper()

	shardBReplicas := []DBTX(nil)
	if shardBReplica != nil {
		shardBReplicas = append(shardBReplicas, shardBReplica)
	}
	shardAReplicas := []DBTX(nil)
	if shardAReplica != nil {
		shardAReplicas = append(shardAReplicas, shardAReplica)
	}
	options := []ShardedOption{
		WithReplicaSet("shard-b", shardBPrimary, shardBReplicas...),
		WithReplicaSet("shard-a", shardAPrimary, shardAReplicas...),
	}
	mirrors := []string(nil)
	if shardBMirror != nil {
		options = append(options, WithReplicaSet("shard-b-mirror", shardBMirror))
		mirrors = append(mirrors, "shard-b-mirror")
	}
	options = append(
		options,
		WithVShardMapping("shard-b", []uint64{0, 2}, mirrors...),
		WithVShardMapping("shard-a", []uint64{1, 3}),
	)
	store, err := NewStore(
		t.Context(),
		Sharded(4, hasher, identityTenantResolver{}, options...),
		storeOptions...,
	)
	require.NoError(t, err)
	return store
}

func TestGeneratedAllShardsReadMergesInPhysicalOrder(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		&fakeDB{name: "shard-b-primary", log: log, users: []*User{{ID: 200}}},
		&fakeDB{name: "shard-b-replica", log: log, users: []*User{{ID: 20}}},
		&fakeDB{name: "shard-a-primary", log: log, users: []*User{{ID: 100}}},
		&fakeDB{name: "shard-a-replica", log: log, users: []*User{{ID: 10}}},
		nil,
	)

	users, err := store.Users().ListAllUsers(t.Context())
	require.NoError(t, err)
	require.Len(t, users, 2)
	assert.Equal(t, []int64{20, 10}, []int64{users[0].ID, users[1].ID})
	assert.ElementsMatch(t, []string{"shard-b-replica", "shard-a-replica"}, log.snapshot())

	primaryUsers, err := store.Users().ListAllUsers(t.Context(), ReadFromPrimary())
	require.NoError(t, err)
	require.Len(t, primaryUsers, 2)
	assert.Equal(t, []int64{200, 100}, []int64{primaryUsers[0].ID, primaryUsers[1].ID})
	assert.ElementsMatch(
		t,
		[]string{"shard-b-primary", "shard-a-primary"},
		log.snapshot()[2:],
	)
}

func TestGeneratedAllShardsAttemptsEveryTargetAndZerosFailures(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	queryErr := errors.New("shard-b unavailable")
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		&fakeDB{name: "shard-b-primary", log: log},
		&fakeDB{name: "shard-b-replica", log: log, queryErr: queryErr},
		&fakeDB{name: "shard-a-primary", log: log},
		&fakeDB{name: "shard-a-replica", log: log, users: []*User{{ID: 10}}},
		nil,
	)

	users, err := store.Users().ListAllUsers(t.Context())
	require.ErrorIs(t, err, queryErr)
	assert.Nil(t, users)
	require.ErrorContains(t, err, `replica set "shard-b"`)
	assert.ElementsMatch(t, []string{"shard-b-replica", "shard-a-replica"}, log.snapshot())
}

func TestGeneratedAllShardsWritesSumRowsAndRejectTransactions(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	shardB := &fakeDB{name: "shard-b-primary", log: log, rowsAffected: 2}
	shardA := &fakeDB{name: "shard-a-primary", log: log, rowsAffected: 3}
	mirror := &fakeDB{name: "shard-b-mirror", log: log, rowsAffected: 2}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		shardB,
		nil,
		shardA,
		nil,
		mirror,
	)

	count, err := store.Users().DeleteAllUsersByName(t.Context(), "obsolete")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
	assert.ElementsMatch(
		t,
		[]string{"shard-b-primary", "shard-b-mirror", "shard-a-primary"},
		log.snapshot(),
	)

	writeErr := errors.New("shard-b write failed")
	shardB.execErr = writeErr
	before := len(log.snapshot())
	count, err = store.Users().DeleteAllUsersByName(t.Context(), "obsolete")
	require.ErrorIs(t, err, writeErr)
	require.ErrorContains(t, err, `replica set "shard-b"`)
	assert.Zero(t, count)
	assert.ElementsMatch(
		t,
		[]string{"shard-b-primary", "shard-a-primary"},
		log.snapshot()[before:],
	)

	before = len(log.snapshot())
	err = store.Users().DeleteAllUsers(t.Context(), WithTx(&fakeTx{fakeDB: &fakeDB{
		name: "tx",
		log:  log,
	}}))
	require.ErrorIs(t, err, pgmesh.ErrCrossShardTransaction)
	assert.Len(t, log.snapshot(), before)
}

func TestGeneratedGroupedManyPartitionsAndRestoresInputOrder(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	shardBPrimary := &fakeDB{name: "shard-b-primary", log: log, users: []*User{{ID: 12}, {ID: 10}}}
	shardBReplica := &fakeDB{name: "shard-b-replica", log: log, users: []*User{{ID: 12}, {ID: 10}}}
	shardAPrimary := &fakeDB{name: "shard-a-primary", log: log, users: []*User{{ID: 21}, {ID: 20}}}
	shardAReplica := &fakeDB{name: "shard-a-replica", log: log, users: []*User{{ID: 21}, {ID: 20}}}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		shardBPrimary,
		shardBReplica,
		shardAPrimary,
		shardAReplica,
		nil,
	)

	users, err := store.Users().ListUsersByIDs(t.Context(), []*ListUsersByIDsT{
		{TenantKey: TenantKey{TenantID: 1}, ID: 20},
		{TenantKey: TenantKey{TenantID: 0}, ID: 10},
		{TenantKey: TenantKey{TenantID: 3}, ID: 99},
		{TenantKey: TenantKey{TenantID: 3}, ID: 21},
		{TenantKey: TenantKey{TenantID: 2}, ID: 10},
		{TenantKey: TenantKey{TenantID: 2}, ID: 12},
	})
	require.NoError(t, err)
	require.Len(t, users, 4)
	assert.Equal(t, []int64{20, 10, 21, 12}, []int64{
		users[0].ID,
		users[1].ID,
		users[2].ID,
		users[3].ID,
	})
	assert.ElementsMatch(t, []string{"shard-b-replica", "shard-a-replica"}, log.snapshot())
	assert.Equal(t, [][]int64{{10, 12}}, shardBReplica.queried())
	assert.Equal(t, [][]int64{{20, 99, 21}}, shardAReplica.queried())
	assert.Empty(t, shardBPrimary.queried())
	assert.Empty(t, shardAPrimary.queried())
}

func TestGeneratedGroupedManyPrimaryAndTransactionRouting(t *testing.T) {
	t.Parallel()

	t.Run("primary option uses every selected primary", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		shardBPrimary := &fakeDB{name: "shard-b-primary", log: log, users: []*User{{ID: 10}}}
		shardAPrimary := &fakeDB{name: "shard-a-primary", log: log, users: []*User{{ID: 20}}}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			shardBPrimary,
			&fakeDB{name: "shard-b-replica", log: log, users: []*User{{ID: 10}}},
			shardAPrimary,
			&fakeDB{name: "shard-a-replica", log: log, users: []*User{{ID: 20}}},
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{
				{TenantKey: TenantKey{TenantID: 1}, ID: 20},
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
			},
			ReadFromPrimary(),
		)
		require.NoError(t, err)
		assert.Equal(t, []int64{20, 10}, []int64{users[0].ID, users[1].ID})
		assert.ElementsMatch(t, []string{"shard-b-primary", "shard-a-primary"}, log.snapshot())
	})

	t.Run("multiple physical shards reject transaction before dispatch", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			nil,
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 1}, ID: 20},
			},
			WithTx(&fakeTx{fakeDB: &fakeDB{name: "tx", log: log, users: []*User{}}}),
		)
		require.ErrorIs(t, err, pgmesh.ErrCrossShardTransaction)
		assert.Nil(t, users)
		assert.Empty(t, log.snapshot())
	})

	t.Run("one physical shard uses transaction", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		txDB := &fakeDB{name: "tx", log: log, users: []*User{{ID: 12}, {ID: 10}}}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			nil,
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 2}, ID: 12},
			},
			WithTx(&fakeTx{fakeDB: txDB}),
		)
		require.NoError(t, err)
		assert.Equal(t, []int64{10, 12}, []int64{users[0].ID, users[1].ID})
		assert.Equal(t, []string{"tx"}, log.snapshot())
		assert.Equal(t, [][]int64{{10, 12}}, txDB.queried())
	})
}

func TestGeneratedGroupedManyPreflightAndFailureBehavior(t *testing.T) {
	t.Parallel()

	t.Run("empty input is a no-op", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			nil,
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(t.Context(), nil)
		require.NoError(t, err)
		assert.Nil(t, users)
		assert.Empty(t, log.snapshot())
	})

	t.Run("nil item dispatches nothing", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			nil,
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{nil},
		)
		require.ErrorContains(t, err, "input 0")
		require.ErrorContains(t, err, "nil")
		assert.Nil(t, users)
		assert.Empty(t, log.snapshot())
	})

	t.Run("routing failure dispatches nothing", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			identityShardHasher{},
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			nil,
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 4}, ID: 11},
			},
		)
		require.ErrorIs(t, err, pgmesh.ErrVShardOutOfRange)
		require.ErrorContains(t, err, "input 1")
		assert.Nil(t, users)
		assert.Empty(t, log.snapshot())
	})

	t.Run("query errors attempt every group and discard rows", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		queryErr := errors.New("shard-b unavailable")
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			&fakeDB{name: "shard-b-replica", log: log, queryErr: queryErr},
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			&fakeDB{name: "shard-a-replica", log: log, users: []*User{{ID: 20}}},
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 1}, ID: 20},
			},
		)
		require.ErrorIs(t, err, queryErr)
		require.ErrorContains(t, err, `replica set "shard-b"`)
		assert.Nil(t, users)
		assert.ElementsMatch(t, []string{"shard-b-replica", "shard-a-replica"}, log.snapshot())
	})

	t.Run("unrequested result is rejected", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log, users: []*User{}},
			&fakeDB{name: "shard-b-replica", log: log, users: []*User{{ID: 999}}, ignoreIDs: true},
			&fakeDB{name: "shard-a-primary", log: log, users: []*User{}},
			nil,
			nil,
		)

		users, err := store.Users().ListUsersByIDs(
			t.Context(),
			[]*ListUsersByIDsT{{TenantKey: TenantKey{TenantID: 0}, ID: 10}},
		)
		require.ErrorContains(t, err, "unrequested lookup key")
		assert.Nil(t, users)
		assert.Equal(t, []string{"shard-b-replica"}, log.snapshot())
	})
}

func TestGeneratedGroupedCopyPartitionsByPhysicalShard(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	shardB := &fakeDB{name: "shard-b-primary", log: log}
	shardA := &fakeDB{name: "shard-a-primary", log: log}
	mirror := &fakeDB{name: "shard-b-mirror", log: log}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		shardB,
		nil,
		shardA,
		nil,
		mirror,
	)

	count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersT{
		{TenantKey: TenantKey{TenantID: 0}, ID: 10, Name: "b0"},
		{TenantKey: TenantKey{TenantID: 1}, ID: 11, Name: "a1"},
		{TenantKey: TenantKey{TenantID: 2}, ID: 12, Name: "b2"},
		{TenantKey: TenantKey{TenantID: 3}, ID: 13, Name: "a3"},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)
	assert.ElementsMatch(
		t,
		[]string{"shard-b-primary", "shard-b-mirror", "shard-a-primary"},
		log.snapshot(),
	)
	assert.Equal(t, [][]any{
		{int64(10), int64(0), "b0"},
		{int64(12), int64(2), "b2"},
	}, shardB.copied())
	assert.Equal(t, shardB.copied(), mirror.copied())
	assert.Equal(t, [][]any{
		{int64(11), int64(1), "a1"},
		{int64(13), int64(3), "a3"},
	}, shardA.copied())
}

func TestGeneratedGroupedCopyPreflightsRoutesAndTransactions(t *testing.T) {
	t.Parallel()

	t.Run("routing failure dispatches nothing", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			identityShardHasher{},
			&fakeDB{name: "shard-b-primary", log: log},
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			nil,
		)

		count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersT{
			{TenantKey: TenantKey{TenantID: 0}, ID: 10},
			{TenantKey: TenantKey{TenantID: 4}, ID: 11},
		})
		require.ErrorIs(t, err, pgmesh.ErrVShardOutOfRange)
		require.ErrorContains(t, err, "input 1")
		assert.Zero(t, count)
		assert.Empty(t, log.snapshot())
	})

	t.Run("multi-shard transaction dispatches nothing", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log},
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			nil,
		)
		tx := &fakeTx{fakeDB: &fakeDB{name: "tx", log: log}}

		count, err := store.Users().CopyUsers(
			t.Context(),
			[]*CopyUsersT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 1}, ID: 11},
			},
			WithTx(tx),
		)
		require.ErrorIs(t, err, pgmesh.ErrCrossShardTransaction)
		assert.Zero(t, count)
		assert.Empty(t, log.snapshot())
	})

	t.Run("one physical shard uses transaction", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		primary := &fakeDB{name: "shard-b-primary", log: log}
		mirror := &fakeDB{name: "shard-b-mirror", log: log}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			primary,
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			mirror,
		)
		txDB := &fakeDB{name: "tx", log: log}

		count, err := store.Users().CopyUsers(
			t.Context(),
			[]*CopyUsersT{
				{TenantKey: TenantKey{TenantID: 0}, ID: 10},
				{TenantKey: TenantKey{TenantID: 2}, ID: 12},
			},
			WithTx(&fakeTx{fakeDB: txDB}),
		)
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
		assert.Equal(t, []string{"tx"}, log.snapshot())
		assert.Len(t, txDB.copied(), 2)
		assert.Empty(t, primary.copied())
		assert.Empty(t, mirror.copied())
	})

	t.Run("empty input is a no-op", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			&fakeDB{name: "shard-b-primary", log: log},
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			nil,
		)

		count, err := store.Users().CopyUsers(t.Context(), nil)
		require.NoError(t, err)
		assert.Zero(t, count)
		assert.Empty(t, log.snapshot())
	})
}

func TestGeneratedGroupedCopyAttemptsEveryGroupAndZerosFailures(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	copyErr := errors.New("shard-b copy failed")
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		&fakeDB{name: "shard-b-primary", log: log, copyErr: copyErr},
		nil,
		&fakeDB{name: "shard-a-primary", log: log},
		nil,
		nil,
	)

	count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersT{
		{TenantKey: TenantKey{TenantID: 0}, ID: 10},
		{TenantKey: TenantKey{TenantID: 1}, ID: 11},
	})
	require.ErrorIs(t, err, copyErr)
	require.ErrorContains(t, err, `replica set "shard-b"`)
	assert.Zero(t, count)
	assert.ElementsMatch(t, []string{"shard-b-primary", "shard-a-primary"}, log.snapshot())
}

func TestGeneratedAsyncCopyCoalescesConcurrentCallersPerPhysicalShard(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	primary := &fakeDB{name: "shard-b-primary", log: log}
	mirror := &fakeDB{name: "shard-b-mirror", log: log}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		primary,
		nil,
		&fakeDB{name: "shard-a-primary", log: log},
		nil,
		mirror,
		WithCopyUsersBatching(pgmesh.CopyBatchConfig{
			BatchSize:    8,
			FlushTimeout: time.Hour,
		}),
	)

	futures := make(chan *pgmesh.Future[int64], 8)
	var callers sync.WaitGroup
	for index := range 8 {
		callers.Go(func() {
			futures <- store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
				TenantKey: TenantKey{TenantID: 0},
				ID:        int64(100 + index),
				Name:      fmt.Sprintf("user-%d", index),
			}})
		})
	}
	callers.Wait()
	close(futures)
	for future := range futures {
		count, err := future.Await(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	}

	assert.Equal(t, 1, primary.copyCallCount())
	assert.Equal(t, 1, mirror.copyCallCount())
	assert.Len(t, primary.copied(), 8)
	assert.ElementsMatch(t, primary.copied(), mirror.copied())
}

func TestGeneratedAsyncCopyPartitionsAndSplitsSubmissions(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	shardB := &fakeDB{name: "shard-b-primary", log: log}
	shardA := &fakeDB{name: "shard-a-primary", log: log}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		shardB,
		nil,
		shardA,
		nil,
		nil,
		WithCopyUsersBatching(pgmesh.CopyBatchConfig{
			BatchSize:    2,
			FlushTimeout: time.Hour,
		}),
	)

	future := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{
		{TenantKey: TenantKey{TenantID: 0}, ID: 10},
		{TenantKey: TenantKey{TenantID: 2}, ID: 12},
		{TenantKey: TenantKey{TenantID: 0}, ID: 14},
		{TenantKey: TenantKey{TenantID: 1}, ID: 11},
	})
	require.NoError(t, store.Users().FlushCopyUsers(t.Context()))
	count, err := future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(4), count)
	assert.Equal(t, 2, shardB.copyCallCount())
	assert.Equal(t, 1, shardA.copyCallCount())
	copied := shardB.copied()
	ids := make([]int64, 0, len(copied))
	for _, row := range copied {
		id, ok := row[0].(int64)
		require.True(t, ok)
		ids = append(ids, id)
	}
	assert.Equal(t, []int64{10, 12, 14}, ids)
}

func TestGeneratedAsyncCopyImmediateFallbackAndCancellation(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	primary := &fakeDB{name: "shard-b-primary", log: log}
	store := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		primary,
		nil,
		&fakeDB{name: "shard-a-primary", log: log},
		nil,
		nil,
	)

	first := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
		TenantKey: TenantKey{TenantID: 0}, ID: 10,
	}})
	second := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
		TenantKey: TenantKey{TenantID: 2}, ID: 12,
	}})
	for _, future := range []*pgmesh.Future[int64]{first, second} {
		count, err := future.Await(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	}
	assert.Equal(t, 2, primary.copyCallCount())

	batchedPrimary := &fakeDB{name: "batched-primary", log: log}
	batched := buildTwoShardStore(
		t,
		pgmesh.ModularShardHashFor[uint64](4),
		batchedPrimary,
		nil,
		&fakeDB{name: "batched-shard-a", log: log},
		nil,
		nil,
		WithCopyUsersBatching(pgmesh.CopyBatchConfig{FlushTimeout: time.Hour}),
	)
	pending := batched.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
		TenantKey: TenantKey{TenantID: 0}, ID: 20,
	}})
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := pending.Await(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, batched.Users().FlushCopyUsers(t.Context()))
	count, err := pending.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Equal(t, 1, batchedPrimary.copyCallCount())
}

func TestGeneratedAsyncCopyPreflightAndSharedFailures(t *testing.T) {
	t.Parallel()

	t.Run("route failure accepts no writes", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		primary := &fakeDB{name: "shard-b-primary", log: log}
		store := buildTwoShardStore(
			t,
			identityShardHasher{},
			primary,
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			nil,
			WithCopyUsersBatching(pgmesh.CopyBatchConfig{}),
		)

		future := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{
			{TenantKey: TenantKey{TenantID: 0}, ID: 10},
			{TenantKey: TenantKey{TenantID: 4}, ID: 14},
		})
		count, err := future.Await(t.Context())
		require.ErrorIs(t, err, pgmesh.ErrVShardOutOfRange)
		require.ErrorContains(t, err, "input 1")
		assert.Zero(t, count)
		assert.Zero(t, primary.copyCallCount())
	})

	t.Run("coalesced failure reaches every caller", func(t *testing.T) {
		t.Parallel()

		log := &callLog{}
		copyErr := errors.New("copy unavailable")
		primary := &fakeDB{name: "shard-b-primary", log: log, copyErr: copyErr}
		store := buildTwoShardStore(
			t,
			pgmesh.ModularShardHashFor[uint64](4),
			primary,
			nil,
			&fakeDB{name: "shard-a-primary", log: log},
			nil,
			nil,
			WithCopyUsersBatching(pgmesh.CopyBatchConfig{BatchSize: 2}),
		)

		first := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
			TenantKey: TenantKey{TenantID: 0}, ID: 10,
		}})
		second := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
			TenantKey: TenantKey{TenantID: 2}, ID: 12,
		}})
		for _, future := range []*pgmesh.Future[int64]{first, second} {
			count, err := future.Await(t.Context())
			require.ErrorIs(t, err, copyErr)
			require.ErrorContains(t, err, `replica set "shard-b"`)
			assert.Zero(t, count)
		}
		assert.Equal(t, 1, primary.copyCallCount())
	})
}

func TestGeneratedCopyBatchConfigValidation(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	_, err := NewStore(
		t.Context(),
		Singleton(&fakeDB{name: "primary", log: log}),
		WithCopyUsersBatching(pgmesh.CopyBatchConfig{BatchSize: -1}),
	)
	require.ErrorContains(t, err, "configure CopyUsers copy batching")
	require.ErrorContains(t, err, "must not be negative")
}

func TestGeneratedAsyncCopyTelemetryEndsWhenFutureResolves(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	log := &callLog{}
	store, err := NewStore(
		t.Context(),
		Singleton(&fakeDB{name: "primary", log: log}),
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithCopyUsersBatching(pgmesh.CopyBatchConfig{FlushTimeout: time.Hour}),
	)
	require.NoError(t, err)

	future := store.Users().EnqueueCopyUsers(t.Context(), []*CopyUsersT{{
		TenantKey: TenantKey{TenantID: 2}, ID: 20,
	}})
	assert.Empty(t, recorder.Ended())
	require.NoError(t, store.Users().FlushCopyUsers(t.Context()))
	count, err := future.Await(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	spans := recorder.Ended()
	require.Len(t, spans, 2)
	assert.Equal(t, "pgmesh.query.physical.Users.CopyUsers", spans[0].Name())
	attributes := make(map[attribute.Key]attribute.Value)
	for _, item := range spans[0].Attributes() {
		attributes[item.Key] = item.Value
	}
	assert.Equal(t, "default", attributes[attribute.Key(pgmesh.AttributeShardName)].AsString())
	assert.Equal(t, "primary", attributes[attribute.Key(pgmesh.AttributeNodeName)].AsString())
	assert.Equal(t, "primary", attributes[attribute.Key(pgmesh.AttributeRouteMode)].AsString())
	assert.Equal(t, "pgmesh.query.logical.Users.EnqueueCopyUsers", spans[1].Name())

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	rows := telemetryIntHistogram(t, metrics, pgmesh.MetricCopyBatchRows)
	require.Len(t, rows.DataPoints, 1)
	assert.Equal(t, uint64(1), rows.DataPoints[0].Count)
	assert.Equal(t, int64(1), rows.DataPoints[0].Sum)
	batchAttributes := telemetryAttributeMap(rows.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "Users", batchAttributes[pgmesh.AttributeStoreName].AsString())
	assert.Equal(t, "CopyUsers", batchAttributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "default", batchAttributes[pgmesh.AttributeShardName].AsString())
	assert.Equal(t, "primary", batchAttributes[pgmesh.AttributeNodeName].AsString())
	assert.Equal(t, "primary", batchAttributes[pgmesh.AttributeNodeRole].AsString())
	assert.Equal(t, "primary", batchAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.Equal(
		t,
		string(pgmesh.CopyBatchFlushReasonExplicit),
		batchAttributes[pgmesh.AttributeCopyBatchFlushReason].AsString(),
	)

	submissions := telemetryIntHistogram(t, metrics, pgmesh.MetricCopyBatchSubmissions)
	require.Len(t, submissions.DataPoints, 1)
	assert.Equal(t, int64(1), submissions.DataPoints[0].Sum)
	flushes := telemetryIntCounter(t, metrics, pgmesh.MetricCopyBatchFlushes)
	require.Len(t, flushes.DataPoints, 1)
	assert.Equal(t, int64(1), flushes.DataPoints[0].Value)
	copyDuration := telemetryHistogram(t, metrics, pgmesh.MetricCopyBatchDuration)
	require.Len(t, copyDuration.DataPoints, 1)
	assert.Equal(t, uint64(1), copyDuration.DataPoints[0].Count)
	queueDuration := telemetryHistogram(t, metrics, pgmesh.MetricCopyQueueDuration)
	require.Len(t, queueDuration.DataPoints, 1)
	assert.Equal(t, uint64(1), queueDuration.DataPoints[0].Count)
}

func TestGeneratedRoutingOnlyShardArgument(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	var resolvedTenantID int64
	store, err := NewStore(
		t.Context(),
		Sharded(
			1,
			pgmesh.ConstantShardHashFor[uint64](0),
			recordingTenantResolver{tenantID: &resolvedTenantID},
			WithReplicaSet(
				"main",
				&fakeDB{name: "primary", log: log},
				&fakeDB{name: "replica", log: log},
			),
			WithVShardMapping("main", []uint64{0}),
		),
	)
	require.NoError(t, err)

	row, err := store.Analyses().GetTenantUserAnalysis(
		t.Context(),
		&GetTenantUserAnalysisT{
			TenantKey:  TenantKey{TenantID: 42},
			UserID:     10,
			AnalysisID: 20,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, row)
	assert.Equal(t, int64(42), resolvedTenantID)
	assert.Equal(t, []string{"replica"}, log.snapshot())
}

func TestGeneratedP2PShardArgumentUsesMessageModel(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	resolver := &recordingMessageKeyResolver{}
	store, err := NewStore(
		t.Context(),
		Sharded(
			1,
			pgmesh.ConstantShardHashFor[uint64](0),
			resolver,
			WithReplicaSet(
				"main",
				&fakeDB{name: "primary", log: log},
				&fakeDB{name: "replica", log: log},
			),
			WithVShardMapping("main", []uint64{0}),
		),
	)
	require.NoError(t, err)

	_, err = store.QueryMessage().ListP2PMessageIDsByChat(
		t.Context(),
		&ListP2PMessageIDsByChatT{
			MessageKey: MessageKey{
				UserID:          11,
				ToUserOrGroupID: 22,
				InGroup:         false,
			},
		},
	)
	require.ErrorContains(t, err, "fake rows are not configured")
	assert.Equal(t, int64(11), resolver.userID)
	assert.Equal(t, int64(22), resolver.toUserOrGroupID)
	assert.False(t, resolver.inGroup)
	assert.Equal(t, []string{"replica"}, log.snapshot())
}

func TestGeneratedTopologyOptionsCloneInputs(t *testing.T) {
	t.Parallel()

	log := &callLog{}
	primary := &fakeDB{name: "primary", log: log}
	replica := &fakeDB{name: "replica", log: log}
	mirror := &fakeDB{name: "mirror", log: log}
	replicas := []DBTX{replica}
	vshards := []uint64{0}
	mirrorNames := []string{"mirror"}
	replicaSetOption := WithReplicaSet("main", primary, replicas...)
	mappingOption := WithVShardMapping("main", vshards, mirrorNames...)

	replicas[0] = nil
	vshards[0] = 1
	mirrorNames[0] = "missing"

	store, err := NewStore(
		t.Context(),
		Sharded(
			1,
			pgmesh.ConstantShardHashFor[uint64](0),
			tenantResolver{},
			replicaSetOption,
			WithReplicaSet("mirror", mirror),
			mappingOption,
		),
	)
	require.NoError(t, err)

	_, err = store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2})
	require.NoError(t, err)
	_, err = store.Users().CreateUser(t.Context(), &CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"})
	require.NoError(t, err)
	assert.Equal(t, []string{"replica", "primary", "mirror"}, log.snapshot())
}

func TestGeneratedStoreTelemetryWiring(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	callLog := &callLog{}
	mirrorErr := errors.New("mirror unavailable")
	store, err := NewStore(
		t.Context(),
		Sharded(
			1,
			pgmesh.ConstantShardHashFor[uint64](0),
			tenantResolver{},
			WithReplicaSet(
				"main",
				&fakeDB{name: "primary", log: callLog},
				&fakeDB{name: "replica", log: callLog},
			),
			WithReplicaSet("mirror", &fakeDB{name: "mirror", log: callLog, rowErr: mirrorErr}),
			WithVShardMapping("main", []uint64{0}, "mirror"),
		),
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithLogger(logger),
	)
	require.NoError(t, err)

	_, err = store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2})
	require.NoError(t, err)
	_, err = store.Users().GetUser(
		t.Context(),
		&GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2},
		ReadFromPrimary(),
	)
	require.NoError(t, err)
	tx := &fakeTx{fakeDB: &fakeDB{name: "tx", log: callLog}}
	_, err = store.Users().GetUser(
		t.Context(),
		&GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2},
		WithTx(tx),
	)
	require.NoError(t, err)
	user, err := store.Users().CreateUser(
		t.Context(),
		&CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"},
	)
	require.ErrorIs(t, err, mirrorErr)
	require.NotNil(t, user)

	type spanExpectation struct {
		query  string
		kind   string
		mode   string
		node   string
		role   string
		status codes.Code
	}
	expectedSpans := []spanExpectation{
		{query: "GetUser", kind: "read", mode: "read", node: "replica-0", role: "read_replica"},
		{query: "GetUser", kind: "read", mode: "primary", node: "primary", role: "primary"},
		{query: "GetUser", kind: "read", mode: "transaction", node: "transaction", role: "transaction"},
		{
			query: "CreateUser", kind: "write", mode: "primary", node: "primary", role: "primary",
			status: codes.Error,
		},
	}
	spans := recorder.Ended()
	require.Len(t, spans, len(expectedSpans)*2)
	for index, expected := range expectedSpans {
		physical := spans[index*2]
		operation := spans[index*2+1]
		assert.Equal(t, "pgmesh.query.physical.Users."+expected.query, physical.Name())
		attributes := telemetryAttributeMap(physical.Attributes())
		assert.Equal(t, "Users", attributes[pgmesh.AttributeStoreName].AsString())
		assert.Equal(t, expected.query, attributes[pgmesh.AttributeQueryName].AsString())
		assert.Equal(t, expected.kind, attributes[pgmesh.AttributeQueryKind].AsString())
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeWrapperDelegated))
		assert.Equal(t, "main", attributes[pgmesh.AttributeShardName].AsString())
		assert.Equal(t, expected.node, attributes[pgmesh.AttributeNodeName].AsString())
		assert.Equal(t, expected.role, attributes[pgmesh.AttributeNodeRole].AsString())
		assert.Equal(t, expected.mode, attributes[pgmesh.AttributeRouteMode].AsString())
		assert.Equal(t, expected.status, physical.Status().Code)
		assert.Equal(t, "pgmesh.query.logical.Users."+expected.query, operation.Name())
		operationAttributes := telemetryAttributeMap(operation.Attributes())
		assert.Equal(t, "single", operationAttributes[pgmesh.AttributeRouteScope].AsString())
		assert.NotContains(t, operationAttributes, attribute.Key(pgmesh.AttributeShardName))
		assert.Equal(t, expected.status, operation.Status().Code)
	}

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 2)
	histogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryPhysicalDuration)
	require.Len(t, histogram.DataPoints, len(expectedSpans))
	var measurementCount uint64
	for _, point := range histogram.DataPoints {
		measurementCount += point.Count
		attributes := telemetryAttributeMap(point.Attributes.ToSlice())
		assert.Equal(t, "Users", attributes[pgmesh.AttributeStoreName].AsString())
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeWrapperDelegated))
	}
	assert.Equal(t, uint64(len(expectedSpans)), measurementCount)
	operationHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryLogicalDuration)
	var operationCount uint64
	for _, point := range operationHistogram.DataPoints {
		operationCount += point.Count
		attributes := telemetryAttributeMap(point.Attributes.ToSlice())
		assert.Equal(t, "single", attributes[pgmesh.AttributeRouteScope].AsString())
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeShardName))
	}
	assert.Equal(t, uint64(len(expectedSpans)), operationCount)

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, len(expectedSpans)*2)
	for index, expected := range expectedSpans {
		var queryRecord map[string]any
		require.NoError(t, json.Unmarshal([]byte(logLines[index*2]), &queryRecord))
		assert.Equal(t, "pgmesh physical query completed", queryRecord["msg"])
		assert.Equal(t, "Users", queryRecord["store_name"])
		assert.Equal(t, expected.query, queryRecord["query_name"])
		assert.Equal(t, expected.mode, queryRecord["route_mode"])
		assert.Equal(t, expected.status == codes.Error, queryRecord["failed"])
		var operationRecord map[string]any
		require.NoError(t, json.Unmarshal([]byte(logLines[index*2+1]), &operationRecord))
		assert.Equal(t, "pgmesh logical query completed", operationRecord["msg"])
		assert.Equal(t, "single", operationRecord["route_scope"])
		assert.Equal(t, expected.status == codes.Error, operationRecord["failed"])
	}
}

func TestGeneratedFanoutTelemetryRecordsEveryShardAndReplica(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	callLog := &callLog{}
	store, err := NewStore(
		t.Context(),
		Sharded(
			2,
			pgmesh.ModularShardHashFor[uint64](2),
			tenantResolver{},
			WithReplicaSet(
				"shard-0",
				&fakeDB{name: "primary-0", log: callLog},
				&fakeDB{name: "replica-0", log: callLog, users: []*User{}},
			),
			WithReplicaSet(
				"shard-1",
				&fakeDB{name: "primary-1", log: callLog},
				&fakeDB{name: "replica-1", log: callLog, users: []*User{}},
			),
			WithVShardMapping("shard-0", []uint64{0}),
			WithVShardMapping("shard-1", []uint64{1}),
		),
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
	)
	require.NoError(t, err)

	_, err = store.Users().ListAllUsers(t.Context())
	require.NoError(t, err)

	spans := recorder.Ended()
	require.Len(t, spans, 3)
	physicalTargets := make(map[string]string, 2)
	for _, span := range spans[:2] {
		assert.Equal(t, "pgmesh.query.physical.Users.ListAllUsers", span.Name())
		attributes := telemetryAttributeMap(span.Attributes())
		shardName := attributes[pgmesh.AttributeShardName].AsString()
		physicalTargets[shardName] = attributes[pgmesh.AttributeNodeName].AsString()
		assert.Equal(t, "read_replica", attributes[pgmesh.AttributeNodeRole].AsString())
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShard))
	}
	assert.Equal(t, map[string]string{"shard-0": "replica-0", "shard-1": "replica-0"}, physicalTargets)
	operation := spans[2]
	assert.Equal(t, "pgmesh.query.logical.Users.ListAllUsers", operation.Name())
	operationAttributes := telemetryAttributeMap(operation.Attributes())
	assert.Equal(t, "fanout", operationAttributes[pgmesh.AttributeRouteScope].AsString())
	assert.Equal(t, int64(2), operationAttributes[pgmesh.AttributeRouteShardCount].AsInt64())
	assert.NotContains(t, operationAttributes, attribute.Key(pgmesh.AttributeShardName))

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	queryHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryPhysicalDuration)
	require.Len(t, queryHistogram.DataPoints, 2)
	metricTargets := make(map[string]string, 2)
	for _, point := range queryHistogram.DataPoints {
		attributes := telemetryAttributeMap(point.Attributes.ToSlice())
		shardName := attributes[pgmesh.AttributeShardName].AsString()
		metricTargets[shardName] = attributes[pgmesh.AttributeNodeName].AsString()
		assert.NotContains(t, attributes, attribute.Key(pgmesh.AttributeVirtualShard))
	}
	assert.Equal(t, physicalTargets, metricTargets)
	operationHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryLogicalDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	assert.Equal(t, uint64(1), operationHistogram.DataPoints[0].Count)
}

func TestGeneratedStoreFactoryTelemetrySeparatesCacheAndInternalQuerySignals(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { require.NoError(t, tracerProvider.Shutdown(context.Background())) })
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, meterProvider.Shutdown(context.Background())) })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logOutput, &slog.HandlerOptions{Level: slog.LevelDebug}))

	callLog := &callLog{}
	cached := []*User{{ID: 10, TenantID: 20, Name: "cached"}}
	store, err := NewStore(
		t.Context(),
		Singleton(&fakeDB{name: "primary", log: callLog}),
		WithTracerProvider(tracerProvider),
		WithMeterProvider(meterProvider),
		WithLogger(logger),
		WithUsersFactory(func(internalStore Users) Users {
			return &usersStoreWrapper{Users: internalStore, listed: cached}
		}),
	)
	require.NoError(t, err)

	listed, err := store.Users().ListAllUsers(t.Context())
	require.NoError(t, err)
	assert.Equal(t, cached, listed)
	_, err = store.Users().GetUser(
		t.Context(),
		&GetUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"primary"}, callLog.snapshot())

	spans := recorder.Ended()
	require.Len(t, spans, 4)
	assert.Equal(t, "pgmesh.query.wrapper.Users.ListAllUsers", spans[0].Name())
	hitAttributes := telemetryAttributeMap(spans[0].Attributes())
	assert.Equal(t, "ListAllUsers", hitAttributes[pgmesh.AttributeQueryName].AsString())
	assert.False(t, hitAttributes[pgmesh.AttributeWrapperDelegated].AsBool())
	assert.NotContains(t, hitAttributes, attribute.Key(pgmesh.AttributeShardName))
	assert.NotContains(t, hitAttributes, attribute.Key(pgmesh.AttributeRouteMode))
	assert.Equal(t, "pgmesh.query.physical.Users.GetUser", spans[1].Name())
	queryAttributes := telemetryAttributeMap(spans[1].Attributes())
	assert.Equal(t, "GetUser", queryAttributes[pgmesh.AttributeQueryName].AsString())
	assert.NotContains(t, queryAttributes, attribute.Key(pgmesh.AttributeWrapperDelegated))
	assert.Equal(t, "default", queryAttributes[pgmesh.AttributeShardName].AsString())
	assert.Equal(t, "primary", queryAttributes[pgmesh.AttributeNodeName].AsString())
	assert.Equal(t, "read", queryAttributes[pgmesh.AttributeRouteMode].AsString())
	assert.Equal(t, "pgmesh.query.logical.Users.GetUser", spans[2].Name())
	operationAttributes := telemetryAttributeMap(spans[2].Attributes())
	assert.Equal(t, "single", operationAttributes[pgmesh.AttributeRouteScope].AsString())
	assert.NotContains(t, operationAttributes, attribute.Key(pgmesh.AttributeShardName))
	assert.Equal(t, "pgmesh.query.wrapper.Users.GetUser", spans[3].Name())
	missAttributes := telemetryAttributeMap(spans[3].Attributes())
	assert.Equal(t, "GetUser", missAttributes[pgmesh.AttributeQueryName].AsString())
	assert.True(t, missAttributes[pgmesh.AttributeWrapperDelegated].AsBool())
	assert.NotContains(t, missAttributes, attribute.Key(pgmesh.AttributeShardName))
	assert.NotContains(t, missAttributes, attribute.Key(pgmesh.AttributeRouteMode))
	assert.Equal(t, spans[3].SpanContext().SpanID(), spans[2].Parent().SpanID())
	assert.Equal(t, spans[2].SpanContext().SpanID(), spans[1].Parent().SpanID())

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 3)
	storeHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryWrapperDuration)
	require.Len(t, storeHistogram.DataPoints, 2)
	metricExecutions := make(map[string]bool, 2)
	for _, point := range storeHistogram.DataPoints {
		attributes := telemetryAttributeMap(point.Attributes.ToSlice())
		metricExecutions[attributes[pgmesh.AttributeQueryName].AsString()] = attributes[pgmesh.AttributeWrapperDelegated].AsBool()
	}
	assert.Equal(t, map[string]bool{"ListAllUsers": false, "GetUser": true}, metricExecutions)
	queryHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryPhysicalDuration)
	require.Len(t, queryHistogram.DataPoints, 1)
	queryMetricAttributes := telemetryAttributeMap(queryHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "GetUser", queryMetricAttributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "default", queryMetricAttributes[pgmesh.AttributeShardName].AsString())
	assert.Equal(t, "primary", queryMetricAttributes[pgmesh.AttributeNodeName].AsString())
	assert.NotContains(t, queryMetricAttributes, attribute.Key(pgmesh.AttributeWrapperDelegated))
	operationHistogram := telemetryHistogram(t, metrics, pgmesh.MetricQueryLogicalDuration)
	require.Len(t, operationHistogram.DataPoints, 1)
	operationMetricAttributes := telemetryAttributeMap(operationHistogram.DataPoints[0].Attributes.ToSlice())
	assert.Equal(t, "GetUser", operationMetricAttributes[pgmesh.AttributeQueryName].AsString())
	assert.Equal(t, "single", operationMetricAttributes[pgmesh.AttributeRouteScope].AsString())

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, 4)
	var hitLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[0]), &hitLog))
	assert.Equal(t, "pgmesh query wrapper completed", hitLog["msg"])
	assert.Equal(t, false, hitLog["wrapper_delegated"])
	var queryLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[1]), &queryLog))
	assert.Equal(t, "pgmesh physical query completed", queryLog["msg"])
	assert.Equal(t, "default", queryLog["shard"])
	assert.NotContains(t, queryLog, "wrapper_delegated")
	var operationLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[2]), &operationLog))
	assert.Equal(t, "pgmesh logical query completed", operationLog["msg"])
	var missLog map[string]any
	require.NoError(t, json.Unmarshal([]byte(logLines[3]), &missLog))
	assert.Equal(t, "pgmesh query wrapper completed", missLog["msg"])
	assert.Equal(t, true, missLog["wrapper_delegated"])
	assert.NotContains(t, missLog, "replica_set")
}

func telemetryAttributeMap(items []attribute.KeyValue) map[attribute.Key]attribute.Value {
	attributes := make(map[attribute.Key]attribute.Value, len(items))
	for _, item := range items {
		attributes[item.Key] = item.Value
	}
	return attributes
}

func telemetryHistogram(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Histogram[float64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			histogram, ok := measurement.Data.(metricdata.Histogram[float64])
			require.True(t, ok)
			return histogram
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Histogram[float64]{}
}

func telemetryIntHistogram(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Histogram[int64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			histogram, ok := measurement.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			return histogram
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Histogram[int64]{}
}

func telemetryIntCounter(
	t *testing.T,
	metrics metricdata.ResourceMetrics,
	name string,
) metricdata.Sum[int64] {
	t.Helper()

	for _, scope := range metrics.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			if measurement.Name != name {
				continue
			}
			counter, ok := measurement.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			return counter
		}
	}
	require.FailNow(t, "metric not found", name)
	return metricdata.Sum[int64]{}
}

func TestGeneratedStoreBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "primary and replica selection",
			run: func(t *testing.T) {
				log := &callLog{}
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
				)

				_, err := store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2})
				require.NoError(t, err)
				_, err = store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2}, ReadFromPrimary())
				require.NoError(t, err)
				assert.Equal(t, []string{"replica", "primary"}, log.snapshot())
			},
		},
		{
			name: "nil query option",
			run: func(t *testing.T) {
				log := &callLog{}
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
				)

				_, err := store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2}, nil)
				require.NoError(t, err)
				assert.Equal(t, []string{"replica"}, log.snapshot())
			},
		},
		{
			name: "missing mirror row is ignored",
			run: func(t *testing.T) {
				log := &callLog{}
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
					&fakeDB{name: "mirror", log: log, rowErr: pgx.ErrNoRows},
					&fakeDB{name: "mirror-after-missing", log: log},
				)

				user, err := store.Users().CreateUser(t.Context(), &CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"})
				require.NoError(t, err)
				require.NotNil(t, user)
				assert.Equal(t, []string{"primary", "mirror", "mirror-after-missing"}, log.snapshot())
			},
		},
		{
			name: "mirrors succeed in order",
			run: func(t *testing.T) {
				log := &callLog{}
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
					&fakeDB{name: "mirror0", log: log},
					&fakeDB{name: "mirror1", log: log},
				)

				_, err := store.Users().CreateUser(t.Context(), &CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"})
				require.NoError(t, err)
				assert.Equal(t, []string{"primary", "mirror0", "mirror1"}, log.snapshot())
			},
		},
		{
			name: "mirror failure preserves primary result",
			run: func(t *testing.T) {
				log := &callLog{}
				mirrorErr := errors.New("mirror unavailable")
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
					&fakeDB{name: "mirror", log: log, rowErr: mirrorErr},
					&fakeDB{name: "mirror-not-called", log: log},
				)

				user, err := store.Users().CreateUser(t.Context(), &CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"})
				require.ErrorIs(t, err, mirrorErr)
				require.NotNil(t, user, "primary result must be retained when a mirror fails")
				assert.Equal(t, []string{"primary", "mirror"}, log.snapshot())
			},
		},
		{
			name: "primary failure skips mirrors",
			run: func(t *testing.T) {
				log := &callLog{}
				primaryErr := errors.New("primary unavailable")
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log, rowErr: primaryErr},
					&fakeDB{name: "replica", log: log},
					&fakeDB{name: "mirror", log: log},
				)

				user, err := store.Users().CreateUser(t.Context(), &CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"})
				require.ErrorIs(t, err, primaryErr)
				assert.Nil(t, user)
				assert.Equal(t, []string{"primary"}, log.snapshot())
			},
		},
		{
			name: "transaction pins primary and drops mirrors",
			run: func(t *testing.T) {
				log := &callLog{}
				store := buildTestStore(
					t,
					&fakeDB{name: "primary", log: log},
					&fakeDB{name: "replica", log: log},
					&fakeDB{name: "mirror", log: log},
				)
				tx := &fakeTx{fakeDB: &fakeDB{name: "tx", log: log}}

				_, err := store.Users().GetUser(t.Context(), &GetUserT{TenantKey: TenantKey{TenantID: 1}, ID: 2}, WithTx(tx))
				require.NoError(t, err)
				_, err = store.Users().CreateUser(
					t.Context(),
					&CreateUserT{TenantKey: TenantKey{TenantID: 2}, ID: 1, Name: "user"},
					WithTx(tx),
				)
				require.NoError(t, err)
				assert.Equal(t, []string{"tx", "tx"}, log.snapshot())
			},
		},
		{
			name: "invalid database configurations",
			run: func(t *testing.T) {
				log := &callLog{}
				primary := &fakeDB{name: "primary", log: log}
				tests := []struct {
					name    string
					config  Topology
					options []StoreOption
					want    string
				}{
					{name: "nil topology", want: "topology is nil"},
					{name: "nil primary", config: Singleton(nil), want: "database primary is nil"},
					{
						name:   "nil singleton option",
						config: Singleton(primary, nil),
						want:   "singleton option 0 is nil",
					},
					{
						name:    "nil store option",
						config:  Singleton(primary),
						options: []StoreOption{nil},
						want:    "store option 0 is nil",
					},
					{
						name:   "nil replica",
						config: Singleton(primary, WithReadReplicas(nil)),
						want:   "database replica 0 is nil",
					},
					{
						name:   "nil mirror",
						config: Singleton(primary, WithWriteMirrors(nil)),
						want:   "database mirror 0 is nil",
					},
				}
				for _, test := range tests {
					t.Run(test.name, func(t *testing.T) {
						_, err := NewStore(t.Context(), test.config, test.options...)
						require.ErrorContains(t, err, test.want)
					})
				}
			},
		},
		{
			name: "invalid sharded configurations",
			run: func(t *testing.T) {
				_, err := NewStore(t.Context(), Sharded[uint64](0, nil, nil))
				require.ErrorContains(t, err, "shard resolver is nil")

				_, err = NewStore(
					t.Context(),
					Sharded(
						1,
						pgmesh.ConstantShardHashFor[uint64](0),
						tenantResolver{},
						nil,
					),
				)
				require.ErrorContains(t, err, "sharded option 0 is nil")

				_, err = NewStore(
					t.Context(),
					Sharded(
						1,
						pgmesh.ConstantShardHashFor[uint64](0),
						tenantResolver{},
						WithReplicaSet("main", nil),
						WithVShardMapping("main", []uint64{0}),
					),
				)
				require.ErrorContains(t, err, "database node")
				require.ErrorContains(t, err, "is nil")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.run(t)
		})
	}
}
