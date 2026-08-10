# Expand shards with synchronous dual writes

Synchronous write mirrors exist primarily to support staged physical-shard
expansion and replacement:

1. keep the old database as the active primary;
2. dual-write new changes to both the old and new databases;
3. backfill and verify historical data on the new database;
4. switch the virtual-shard mapping so the new database becomes active;
5. keep reverse dual writes briefly if rollback is required, then retire the
   old database.

Mirrors are a migration bridge. They are not permanent replicas, automatic
backfill, or an atomic cross-database commit protocol.

## Before enabling dual writes

- Apply a compatible schema to both old and new databases.
- Make write operations idempotent wherever possible.
- Identify every write path. Only generated mirrored methods participate.
- Plan the historical backfill, reconciliation, cutover, and rollback steps.
- Add metrics and alerts for mirror errors before relying on the new database.

Do not enable `ignore_mirror_error` during a shard migration unless another
system durably records and repairs every failed target write.

## 1. Register the old and new databases

Register the future shard as another replica set. During this phase, the old
database remains the main replica set:

```go
replicaSetOptions := []db.ShardedOption{
    db.WithReplicaSet("old-shard", oldShardPool),
    db.WithReplicaSet("new-shard", newShardPool),
}
```

## 2. Enable old-to-new dual writes

Attach the new database as a mirror of the old main replica set:

```go
mappingOption := db.WithVirtualShardMapping(
    "old-shard",
    pgmesh.VirtualShardRange(0, 128),
    "new-shard",
)
```

After this deployment, each generated non-transactional write runs on
`old-shard` first and `new-shard` second. Reads continue to use `old-shard`, so
the new database is not serving application traffic yet.

Mirrors attach to a main replica set, not an individual virtual shard. Every
mirrored generated write routed to `old-shard` is sent to every configured
mirror. When splitting one old shard into several new shards, a conservative
rollout may mirror the full source write stream to each new target and route
only its assigned virtual shards after cutover. Account for the temporary
storage and write load.

Mappings that reuse the same main replica-set name must use the same mirror
list in the same order.

## 3. Backfill and reconcile

Copy historical rows from the old database to the new database while dual
writes remain enabled. pgmesh does not perform this copy.

Backfill with idempotent inserts or upserts and account for races between the
copy and live writes. A one-time row count is not enough. Reconcile primary keys
and relevant values repeatedly until the old and new databases agree.

`pgx.ErrNoRows` (which matches `sql.ErrNoRows` through `errors.Is`) from a
mirror is ignored by default. This allows an update or delete to encounter a
row that has not been backfilled yet, but it also means a successful request is
not proof that the target already contains that row.

## 4. Understand failure behavior

For each mirrored generated write:

1. the old primary runs first;
2. primary failure stops the operation before the new database runs;
3. mirrors run sequentially in configured order;
4. the old primary result is retained;
5. the first mirror error other than `ErrNoRows` is returned;
6. primary success is not rolled back when a mirror fails.

Therefore, dual writes are synchronous but not atomic. A mirror error means the
old database may have committed while the new database did not. Record the
failure and repair it before cutover; make retries safe for an already-applied
primary write.

## 5. Verify before cutover

Before routing reads or authoritative writes to the new database, verify:

- the schema and constraints match the intended post-cutover application;
- the backfill is complete for every virtual shard being moved;
- reconciliation finds no unexplained row or value differences;
- all live write paths are covered or replicated through another mechanism;
- mirror error metrics have remained clean for the required observation window;
- the new database and its read replicas have enough capacity.

## 6. Switch the new database to active

Change the mapping so the new replica set becomes main:

```go
mappingOption := db.WithVirtualShardMapping(
    "new-shard",
    pgmesh.VirtualShardRange(0, 128),
)
```

Deploying this topology switches both reads and writes to the new database.
Changing the mapping does not move or recheck data, so cut over only after the
previous verification completes.

For a rollback window, make `old-shard` a mirror of `new-shard` during cutover:

```go
mappingOption := db.WithVirtualShardMapping(
    "new-shard",
    pgmesh.VirtualShardRange(0, 128),
    "old-shard",
)
```

This reverses the dual-write direction and keeps the old database current while
the new database is authoritative. Reconcile again before any rollback. Remove
the old mirror and retire the old database only after the rollback window ends.

## Writes that need another migration mechanism

The following paths are not covered by synchronous mirrors:

- generated calls using `WithTx`, because a `pgx.Tx` belongs to one database;
- generated `:batch*` result objects, which are passed through without mirror
  fan-out;
- SQL executed directly through pgx or another library;
- writes from older application versions that do not use the mirrored wrapper.

`:copyfrom` generated writes are mirrored normally.

Use an outbox, change-data-capture stream, migration-specific replay, or an
explicit dual-write transaction strategy for uncovered paths. Do not cut over
until those writes are present and verified on the new database.

See [`examples/04-mirrors-and-transactions`](../../examples/04-mirrors-and-transactions)
for the mechanics of mirrors and the deliberate transaction exception.
