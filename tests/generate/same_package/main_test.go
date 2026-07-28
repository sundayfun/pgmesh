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

func (tenantResolver) Tenant(int64) uint64 {
	return 0
}

func (tenantResolver) MessageKey(int64, int64, bool) uint64 {
	return 0
}

type identityTenantResolver struct{}

func (identityTenantResolver) Tenant(tenantID int64) uint64 {
	return uint64(tenantID) //nolint:gosec // Test fixtures use nonnegative IDs.
}

func (identityTenantResolver) MessageKey(userID, _ int64, _ bool) uint64 {
	return uint64(userID) //nolint:gosec // Test fixtures use nonnegative IDs.
}

type identityShardHasher struct{}

func (identityShardHasher) Hash(key uint64) uint64 {
	return key
}

type recordingTenantResolver struct {
	tenantID *int64
}

func (r recordingTenantResolver) Tenant(tenantID int64) uint64 {
	*r.tenantID = tenantID
	return 0
}

func (recordingTenantResolver) MessageKey(int64, int64, bool) uint64 {
	return 0
}

type recordingMessageKeyResolver struct {
	userID          int64
	toUserOrGroupID int64
	inGroup         bool
}

func (recordingMessageKeyResolver) Tenant(int64) uint64 {
	return 0
}

func (r *recordingMessageKeyResolver) MessageKey(
	userID int64,
	toUserOrGroupID int64,
	inGroup bool,
) uint64 {
	r.userID = userID
	r.toUserOrGroupID = toUserOrGroupID
	r.inGroup = inGroup
	return 0
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

func buildTwoShardStore(
	t *testing.T,
	hasher pgmesh.ShardHasher[uint64],
	shardBPrimary *fakeDB,
	shardBReplica *fakeDB,
	shardAPrimary *fakeDB,
	shardAReplica *fakeDB,
	shardBMirror *fakeDB,
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
		{ID: 20, TenantID: 1},
		{ID: 10, TenantID: 0},
		{ID: 99, TenantID: 3},
		{ID: 21, TenantID: 3},
		{ID: 10, TenantID: 2},
		{ID: 12, TenantID: 2},
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
			[]*ListUsersByIDsT{{ID: 20, TenantID: 1}, {ID: 10, TenantID: 0}},
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
			[]*ListUsersByIDsT{{ID: 10, TenantID: 0}, {ID: 20, TenantID: 1}},
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
			[]*ListUsersByIDsT{{ID: 10, TenantID: 0}, {ID: 12, TenantID: 2}},
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
			[]*ListUsersByIDsT{{ID: 10, TenantID: 0}, {ID: 11, TenantID: 4}},
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
			[]*ListUsersByIDsT{{ID: 10, TenantID: 0}, {ID: 20, TenantID: 1}},
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
			[]*ListUsersByIDsT{{ID: 10, TenantID: 0}},
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

	count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersParams{
		{ID: 10, TenantID: 0, Name: "b0"},
		{ID: 11, TenantID: 1, Name: "a1"},
		{ID: 12, TenantID: 2, Name: "b2"},
		{ID: 13, TenantID: 3, Name: "a3"},
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

		count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersParams{
			{ID: 10, TenantID: 0},
			{ID: 11, TenantID: 4},
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
			[]*CopyUsersParams{{ID: 10, TenantID: 0}, {ID: 11, TenantID: 1}},
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
			[]*CopyUsersParams{{ID: 10, TenantID: 0}, {ID: 12, TenantID: 2}},
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

	count, err := store.Users().CopyUsers(t.Context(), []*CopyUsersParams{
		{ID: 10, TenantID: 0},
		{ID: 11, TenantID: 1},
	})
	require.ErrorIs(t, err, copyErr)
	require.ErrorContains(t, err, `replica set "shard-b"`)
	assert.Zero(t, count)
	assert.ElementsMatch(t, []string{"shard-b-primary", "shard-a-primary"}, log.snapshot())
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
			UserID:     10,
			AnalysisID: 20,
			TenantID:   42,
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
			UserID:          11,
			ToUserOrGroupID: 22,
			InGroup:         false,
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

	_, err = store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2})
	require.NoError(t, err)
	_, err = store.Users().CreateUser(t.Context(), &CreateUserParams{ID: 1, TenantID: 2, Name: "user"})
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

	_, err = store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2})
	require.NoError(t, err)
	_, err = store.Users().GetUser(
		t.Context(),
		&GetUserParams{TenantID: 1, ID: 2},
		ReadFromPrimary(),
	)
	require.NoError(t, err)
	tx := &fakeTx{fakeDB: &fakeDB{name: "tx", log: callLog}}
	_, err = store.Users().GetUser(
		t.Context(),
		&GetUserParams{TenantID: 1, ID: 2},
		WithTx(tx),
	)
	require.NoError(t, err)
	user, err := store.Users().CreateUser(
		t.Context(),
		&CreateUserParams{ID: 1, TenantID: 2, Name: "user"},
	)
	require.ErrorIs(t, err, mirrorErr)
	require.NotNil(t, user)

	type spanExpectation struct {
		query  string
		kind   string
		mode   string
		status codes.Code
	}
	expectedSpans := []spanExpectation{
		{query: "GetUser", kind: "read", mode: "read"},
		{query: "GetUser", kind: "read", mode: "primary"},
		{query: "GetUser", kind: "read", mode: "transaction"},
		{query: "CreateUser", kind: "write", mode: "primary", status: codes.Error},
	}
	spans := recorder.Ended()
	require.Len(t, spans, len(expectedSpans))
	for index, expected := range expectedSpans {
		assert.Equal(t, "pgmesh.query.Users."+expected.query, spans[index].Name())
		attributes := telemetryAttributeMap(spans[index].Attributes())
		assert.Equal(t, expected.query, attributes[pgmesh.AttributeQueryName].AsString())
		assert.Equal(t, expected.kind, attributes[pgmesh.AttributeQueryKind].AsString())
		assert.Equal(t, "main", attributes[pgmesh.AttributeReplicaSet].AsString())
		assert.Equal(t, expected.mode, attributes[pgmesh.AttributeRouteMode].AsString())
		assert.NotContains(t, attributes, attribute.Key("pgmesh.route.write_mirror_count"))
		assert.Equal(t, expected.status, spans[index].Status().Code)
	}

	var metrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &metrics))
	require.Len(t, metrics.ScopeMetrics, 1)
	require.Len(t, metrics.ScopeMetrics[0].Metrics, 1)
	histogram, ok := metrics.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	require.Len(t, histogram.DataPoints, len(expectedSpans))
	var measurementCount uint64
	for _, point := range histogram.DataPoints {
		measurementCount += point.Count
	}
	assert.Equal(t, uint64(len(expectedSpans)), measurementCount)

	logLines := strings.Split(strings.TrimSpace(logOutput.String()), "\n")
	require.Len(t, logLines, len(expectedSpans))
	for index, line := range logLines {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		assert.Equal(t, expectedSpans[index].query, record["query_name"])
		assert.Equal(t, expectedSpans[index].mode, record["route_mode"])
		assert.NotContains(t, record, "write_mirror_count")
		assert.Equal(t, expectedSpans[index].status == codes.Error, record["failed"])
	}
}

func telemetryAttributeMap(items []attribute.KeyValue) map[attribute.Key]attribute.Value {
	attributes := make(map[attribute.Key]attribute.Value, len(items))
	for _, item := range items {
		attributes[item.Key] = item.Value
	}
	return attributes
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

				_, err := store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2})
				require.NoError(t, err)
				_, err = store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2}, ReadFromPrimary())
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

				_, err := store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2}, nil)
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

				user, err := store.Users().CreateUser(t.Context(), &CreateUserParams{ID: 1, TenantID: 2, Name: "user"})
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

				_, err := store.Users().CreateUser(t.Context(), &CreateUserParams{ID: 1, TenantID: 2, Name: "user"})
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

				user, err := store.Users().CreateUser(t.Context(), &CreateUserParams{ID: 1, TenantID: 2, Name: "user"})
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

				user, err := store.Users().CreateUser(t.Context(), &CreateUserParams{ID: 1, TenantID: 2, Name: "user"})
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

				_, err := store.Users().GetUser(t.Context(), &GetUserParams{TenantID: 1, ID: 2}, WithTx(tx))
				require.NoError(t, err)
				_, err = store.Users().CreateUser(
					t.Context(),
					&CreateUserParams{ID: 1, TenantID: 2, Name: "user"},
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
