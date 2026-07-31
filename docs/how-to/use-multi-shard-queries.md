# Use multi-shard queries

pgmesh supports two explicit ways to read from more than one physical shard:

| Pattern | Use when | Physical dispatch |
| --- | --- | --- |
| Grouped list lookup | Every requested value has routing data | Only populated shards |
| `shard: all()` scatter | The query genuinely needs every shard | Every physical shard |

Prefer grouped lookup when the caller knows the requested keys. It avoids
unrelated shards and gives pgmesh enough information to restore request order.
Use scatter only for bounded administrative or global operations whose
cross-shard cost is intentional.

## Group a list lookup by shard

Define a routed `:many` query with exactly one one-dimensional list parameter:

```sql
-- name: ListAccountsByIDs :many
-- kind: read
-- shard: tenantKey(tenant_id)
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
WHERE id = ANY(@ids::bigint[])
ORDER BY id;
```

Here `tenant_id` is routing-only while `ids` is the database parameter. pgmesh
generates one input item combining the route key with the singular lookup
value:

```go
accounts, err := store.Accounts().ListAccountsByIDs(
    ctx,
    []*db.ListAccountsByIDsT{
        {
            TenantKey: db.TenantKey{TenantID: firstTenantID},
            ID:        firstAccountID,
        },
        {
            TenantKey: db.TenantKey{TenantID: secondTenantID},
            ID:        secondAccountID,
        },
    },
)
```

The generated method:

1. Resolves every input before dispatching database work.
2. Deduplicates lookup values within each physical shard.
3. Runs one query concurrently on every populated shard.
4. Validates that returned rows contain requested lookup keys.
5. Restores results to first-occurrence input order.

Missing keys add no result. Repeated rows for one key retain their SQL order.
The query must return a lookup field matching the list element type; pgmesh
matches the exact list name first and then a simple singular form such as
`ids` → `id`.

Ordinary reads use each shard's replica route. Add `ReadFromPrimary()` for
read-your-write behavior. `WithTx(tx)` is valid only when every item maps to
the transaction's single physical shard; otherwise pgmesh returns
`ErrCrossShardTransaction` before dispatch.

## Scatter over every physical shard

Use the reserved `all()` route for an operation that must run on all shards:

```sql
-- name: ListAllAccounts :many
-- kind: read
-- shard: all()
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
ORDER BY id;
```

The public call has no routing key:

```go
accounts, err := store.Accounts().ListAllAccounts(ctx)
```

Each physical shard executes concurrently. A `:many` result concatenates shard
results in deterministic topology order, but pgmesh does not provide a global
SQL order, limit, aggregation, or pagination layer. Apply those operations in
the application or use a dedicated global index/analytics system.

Scatter routing is supported for `:many`, `:exec`, and `:execrows`:

- `:many` concatenates rows in topology order.
- `:execrows` sums affected-row counts.
- `:exec` returns only the joined outcome.

`ReadFromPrimary()` pins scatter reads to every primary. `WithTx` is always
rejected because one PostgreSQL transaction cannot span physical shards.

## Handle failures deliberately

Routing failures in a grouped lookup dispatch nothing. Once grouped or scatter
work begins, all selected shards are attempted. If any physical execution
fails, pgmesh returns a joined error labeled with the shard name and returns no
rows or affected count.

This prevents callers from treating a partial aggregate as complete, but it
does not roll back successful writes on other shards. Multi-shard `:exec` and
`:execrows` operations can partially commit and should be restricted to
idempotent or operationally recoverable workflows.

## Observe fan-out

One generated call records one `pgmesh.query.logical.duration` point with
`pgmesh.route.scope="fanout"`. Every physical shard execution records its own
`pgmesh.query.physical.duration` point with `pgmesh.shard.name` and
`pgmesh.node.name`. The physical-to-logical throughput ratio therefore exposes
fan-out amplification directly.

See the runnable
[`07-multi-shard-queries` example](../../examples/07-multi-shard-queries) and
the [common PromQL queries](enable-opentelemetry.md#common-promql-queries).
