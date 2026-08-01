# Use transactions

A transaction is owned by one PostgreSQL connection. Select the primary pool
with the same resolver, hasher, and mapping used by `Sharded`, then pass
the transaction to generated `Store` methods.

## Retain primary pools

Retain primary pools by replica-set name when constructing the store. pgmesh
does not own or close configured pools.

```go
primaryPools := map[string]*pgxpool.Pool{
    "shard-0": shard0PrimaryPool,
    "shard-1": shard1PrimaryPool,
}
```

## Resolve the target shard

Use the same resolver, hasher, and mapping as the store configuration:

```go
resolver := tenantResolver{}
key := resolver.TenantKey(db.TenantKey{TenantID: tenantID})
vshard := hasher.Hash(key)
replicaSetName := configuredReplicaSetFor(vshard)
```

Keep this placement helper beside topology construction so transaction setup
and the `Sharded` options cannot silently diverge.

## Begin on the selected primary

```go
pool := primaryPools[replicaSetName]
tx, err := pool.Begin(ctx)
if err != nil {
    return err
}
defer func() {
    _ = tx.Rollback(ctx)
}()
```

## Pass the generated route option

```go
updated, err := queries.Accounts().UpdateAccountName(
    ctx,
    &db.UpdateAccountNameParams{
        TenantID:    tenantID,
        ID:          accountID,
        DisplayName: "updated",
    },
    db.WithTx(tx),
)
if err != nil {
    return err
}

if err := tx.Commit(ctx); err != nil {
    return err
}
```

For reads, `WithTx(tx)` also pins execution to the transaction and therefore
the primary. It takes precedence over normal replica selection.

## Important constraints

- pgmesh does not verify that the supplied transaction belongs to the shard
  selected from the query arguments. Begin it from the matching primary pool.
- All queries in one transaction must resolve to the same physical shard.
- pgmesh does not coordinate cross-shard transactions.
- `shard: all()` always rejects `WithTx` with
  `pgmesh.ErrCrossShardTransaction`.
- Routed `:copyfrom` accepts `WithTx` only when every input row groups to one
  physical shard; it rejects the call before copying otherwise.
- Generated `<CopyQuery>Async` methods do not accept query options and cannot
  run in a transaction; use the synchronous copy method when `WithTx` is
  required.
- Transaction-bound generated calls do not fan writes out to mirrors.
- Always commit or roll back using normal pgx transaction handling.

The mirror exception is critical during physical-shard expansion: transactional
writes will not reach the future database through pgmesh. Capture and replay
them with an outbox, CDC, or another migration mechanism, and reconcile them
before cutover. See
[Expand shards with synchronous dual writes](add-write-mirrors.md).

The full runnable pattern is in
[`examples/04-mirrors-and-transactions`](../../examples/04-mirrors-and-transactions).
