# Add sharding

This guide changes the configuration behind the generated `Store`. Every query
in a sharded generated store must have a `shard` annotation; unsharded models
belong in a separate generated package. See [Add a query](add-a-query.md).

If virtual shards, replica sets, or mappings are new terms, start with the
[topology concepts and request-routing flow](../topology.md).

## 1. Choose a stable logical key

Implement the generated resolver interface. For an annotation such as
`shard: tenant(tenant_id)`, the generated interface includes `Tenant`:

```go
type tenantResolver struct{}

func (tenantResolver) Tenant(tenantID int64) uint64 {
    if tenantID < 0 {
        panic("tenant ID must not be negative")
    }
    return uint64(tenantID)
}
```

Keep the resolver behavior stable after data is written. If a route combines
multiple operands, normalize and combine them deterministically.

## 2. Choose the virtual-shard count and hash

The hash must return an index in `[0, NumVShards)`. Integer keys can use the
built-in modular hasher:

```go
const numVShards = 128

hasher := pgmesh.ModularShardHashFor[uint64](numVShards)
```

Use a custom `pgmesh.ShardHasher` when the key is not integer-like or when an
existing system already defines the hash. Changing the hash changes placement
and therefore requires a data migration.

## 3. Open physical database pools

Each replica set represents one physical shard. Start with one primary per
set; replicas and mirrors can be added separately.

```go
replicaSetOptions := []db.ShardedOption{
    db.WithReplicaSet("shard-0", shard0Pool),
    db.WithReplicaSet("shard-1", shard1Pool),
}
```

Names must be unique and non-empty. The application owns pool construction and
lifecycle.

## 4. Map every virtual shard exactly once

```go
mappingOptions := []db.ShardedOption{
    db.WithVShardMapping("shard-0", pgmesh.VShardRange(0, 64)),
    db.WithVShardMapping("shard-1", pgmesh.VShardRange(64, 128)),
}
```

`VShardRange(from, to)` is half-open. Every index from zero through
`NumVShards - 1` must occur once. Missing, duplicate, and out-of-range entries
are rejected when the mesh is created.

Changing this mapping tells pgmesh where requests should go; it does not move
existing rows. Move or copy data before switching production traffic.

For an online physical-shard expansion, use
[synchronous dual writes](add-write-mirrors.md) to keep the old database active
while the new database is backfilled and verified, then change the mapping.

## 5. Construct the same Store interface

```go
topologyOptions := append(replicaSetOptions, mappingOptions...)
queries, err := db.NewStore(
    ctx,
    db.Sharded(
        numVShards,
        hasher,
        tenantResolver{},
        topologyOptions...,
    ),
)
if err != nil {
    return err
}
```

`queries` has type `db.Store`, exactly as it does for a single database or
read/write-separated configuration. Normal reads use a replica when one is
configured. Writes use the selected physical shard's primary:

```go
account, err := queries.Accounts().UpsertAccount(ctx, &db.UpsertAccountParams{
    ID:          accountID,
    TenantID:    tenantID,
    DisplayName: "Ada",
})
```

Force a routed read to the primary when current data is required:

```go
account, err := queries.Accounts().GetAccount(
    ctx,
    &db.GetAccountParams{TenantID: tenantID, ID: accountID},
    db.ReadFromPrimary(),
)
```

Queries annotated with `shard: all()` run once per physical replica set rather
than once per virtual shard. Ordinary reads select one replica from each set;
`ReadFromPrimary()` selects every primary. Scatter writes also use each
physical primary and its configured mirrors.

Routed `:copyfrom` inputs are resolved and grouped by physical replica set
before any copy begins. Multiple virtual shards placed on the same replica set
share one copy operation.

Routed `:many` queries with one list parameter similarly resolve each lookup
item and issue one query per populated physical replica set. Their returned
rows are matched by a result field with the list parameter's exact name or its
simple English singular form and restored to input-key order.

## Operational checklist

- Apply compatible schema to every physical database before routing traffic.
- Confirm the resolver and hasher match the placement used by existing data.
- Treat scatter writes and grouped copies as potentially partially committed
  when one physical shard fails.
- Move data before changing a virtual-shard mapping.
- Keep old and new application versions compatible during a topology rollout.
- Close every configured pool during shutdown.

The complete runnable version is
[`examples/03-sharded-read-write`](../../examples/03-sharded-read-write).
