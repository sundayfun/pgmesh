# Add a query

## 1. Choose the sqlc command

Write the query using a sqlc command supported by pgmesh:

- `:one`, `:many`, `:exec`, `:execrows`, and `:execresult`;
- `:copyfrom`;
- `:batchexec`, `:batchone`, and `:batchmany`.

## 2. Classify it

Put `kind: read` or `kind: write` immediately after the sqlc name annotation:

```sql
-- name: ListAccounts :many
-- kind: read
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
ORDER BY id;
```

Use `read` for non-mutating operations. They can still be sent to the primary
with `ReadFromPrimary()` or `WithTx()`. Use `write` for inserts, updates,
deletes, DDL, and other operations that mutate database state.

The classification controls internal endpoint selection. Both read and write
methods appear on their required public store group.

## 3. Add a shard route when needed

For a query that can be routed from its arguments, add `shard` immediately
after `kind`:

```sql
-- name: GetAccount :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Accounts
-- GetAccount returns one account within a tenant.
SELECT id, tenant_id, display_name
FROM accounts
WHERE tenant_id = $1 AND id = $2;
```

`tenant` becomes a method on the generated `ShardResolver`:

```go
type ShardResolver[SK any] interface {
    Tenant(tenantID int64) SK
}
```

Route operands normally name SQL parameters. They must resolve to compatible Go
types anywhere the same route is used.

Some shard-local queries do not need the shard value in their SQL. In that
case, give the route a routing-only operand:

```sql
-- name: ListTenantAccounts :many
-- kind: read
-- shard: tenant(tenant_id)
-- store: Accounts
SELECT id, display_name
FROM accounts
ORDER BY id
LIMIT @limit OFFSET @offset;
```

Although `tenant_id` is not a SQL parameter here, it names a column on the
generated `Account` table model. The generator uses that model's `TenantID`
field name and Go type, then combines it with the original sqlc parameter
fields. Only the original fields are passed to SQL:

```go
type ListTenantAccountsShardParams struct {
    Limit    int32
    Offset   int32
    TenantID int64
}

accounts, err := queries.Accounts().ListTenantAccounts(
    ctx,
    &db.ListTenantAccountsShardParams{
        Limit:    100,
        TenantID: tenantID,
    },
)
```

Generation fails if a routing-only operand does not name a field on the query's
generated table model. For joins and ambiguous sqlc metadata, insert and result
models plus tables named by the SQL's source relations take precedence over
models inferred only from SQL parameters; matching models within the same tier
must produce compatible field names and Go types.

### Route list lookups by item

A routed `:many` query with exactly one one-dimensional list parameter is
automatically grouped by physical shard:

```sql
-- name: ListAccountsByID :many
-- kind: read
-- shard: tenant(tenant_id)
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
WHERE id = ANY(@id::bigint[]);
```

The list parameter must have a singular name such as `id`, and the query must
return exactly one field with the same SQL name and Go type. The route may use a
routing-only field. In that case pgmesh generates one input item containing
both the scalar lookup value and the routing data:

```go
type ListAccountsByIDShardParams struct {
    ID       int64
    TenantID int64
}

accounts, err := queries.Accounts().ListAccountsByID(
    ctx,
    []*db.ListAccountsByIDShardParams{
        {ID: firstID, TenantID: firstTenantID},
        {ID: secondID, TenantID: secondTenantID},
    },
)
```

pgmesh resolves every item before dispatch, deduplicates repeated lookup keys
within the same physical shard, and issues one concurrent query per populated
replica set. Returned rows are matched through the required lookup field and
restored to first-occurrence input order; missing keys add no row, and multiple
rows for one key retain their SQL order. Lookup values must be comparable Go
values.

Normal reads use each shard's replica route, while `ReadFromPrimary()` uses
primaries. `WithTx()` is accepted only when every item maps to one physical
shard. A routing failure dispatches nothing. Once queries have started, every
group is attempted; any shard or result-validation failure returns a joined,
replica-set-labeled error and no rows.

This inference changes the generated Store signature of an existing routed
`:many` query that has exactly one list parameter. The underlying sqlc method
is unchanged. Queries with scalar parameters, multiple SQL parameters,
multidimensional arrays, `:batchmany`, or `shard: all()` retain their existing
routing behavior.

### Run a query on every physical shard

Use the reserved route `shard: all()` when a query must run once per physical
replica set and does not have a shard key:

```sql
-- name: ListAllAccounts :many
-- kind: read
-- shard: all()
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
ORDER BY id;
```

Scatter routing is supported for `:many`, `:exec`, and `:execrows`. A `:many`
result concatenates physical-shard results in topology order, while `:execrows`
sums affected rows. Calls run concurrently and attempt every physical shard. If
any target fails, the returned error joins errors labeled with their
replica-set names and the returned rows or count is zero. Use
`ReadFromPrimary()` when a scatter read must bypass replicas.

`WithTx` cannot be used with `shard: all()` because a PostgreSQL transaction
belongs to one database. The method returns `pgmesh.ErrCrossShardTransaction`
without dispatching the query.

## 4. Group every query

Every query must declare its generated sub-interface. Add `store` after the
optional `shard` annotation:

```sql
-- name: GetAccount :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
WHERE tenant_id = $1 AND id = $2;
```

The value must be an exported Go identifier. pgmesh generates `Accounts`,
`AccountsReader`, and `AccountsWriter` interfaces, and exposes the group from
the root:

```go
type Store interface {
    Accounts() Accounts
}
```

Call the query through `queries.Accounts().GetAccount(...)`. Generation fails
when any query is missing its `store` annotation.

The annotation order is strict:

1. `-- name: ...`
2. `-- kind: read|write`
3. optional `-- shard: route(operand, ...)`
4. required `-- store: ExportedGroup`
5. optional ordinary documentation comments

## 5. Regenerate and compile

Use your project's generation command. In this repository:

```bash
just --justfile examples/justfile generate
go test ./...
```

For a downstream project that invokes sqlc directly:

```bash
sqlc generate
go test ./...
```

Generation fails when metadata is missing, out of order, malformed, or contains
a routing-only operand that does not align with a generated table field. Treat
that failure as part of the query review rather than moving routing into
handwritten code.

## 6. Call the generated Store

Business code always calls the same generated interface:

```go
account, err := queries.Accounts().GetAccount(ctx, &db.GetAccountParams{
    TenantID: tenantID,
    ID:       accountID,
})
```

For a read-your-write operation, add the generated option:

```go
account, err := queries.Accounts().GetAccount(ctx, arg, db.ReadFromPrimary())
```

## Batch and copy queries

A `:copyfrom` query may use an ordinary shard route:

```sql
-- name: CopyAccounts :copyfrom
-- kind: write
-- shard: tenant(tenant_id)
-- store: Accounts
INSERT INTO accounts (id, tenant_id, display_name)
VALUES ($1, $2, $3);
```

The generated method resolves every input row before writing, groups rows that
map to the same physical replica set, and runs one `COPY FROM` per nonempty
group. It therefore performs O(n) local routing work and O(p) database round
trips, where p is the number of targeted physical shards. Input order is
preserved within each group, and configured write mirrors receive their
physical shard's group.

Routing-only operands produce a flattened `[]*CopyAccountsShardParams`, using
the same wrapper convention as ordinary routed queries. A routing error causes
no copies. Database groups run concurrently and are all attempted; any error
causes the method to return a zero count and a joined, replica-set-labeled
error.

`WithTx` is accepted only when every row resolves to one physical shard. It
returns `pgmesh.ErrCrossShardTransaction` before writing when multiple shards
are present, and suppresses write mirrors when it succeeds.

Sharded `:batch*` methods remain unsupported because sqlc exposes their
database errors later through batch-result callbacks. Keep those methods in an
unsharded generated store or partition them explicitly in application code.
