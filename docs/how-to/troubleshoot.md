# Troubleshoot generation and routing

## Generation errors

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Missing required kind annotation | The query has no `kind` comment | Add `-- kind: read` or `-- kind: write` immediately after `-- name` |
| First comment must be kind | Ordinary documentation appears before `kind` | Move documentation after `kind`, optional `shard`, and required `store` |
| Invalid or misplaced shard annotation | `shard` is malformed or appears later | Use `-- shard: route(operand, ...)` directly after `kind` |
| Missing required store annotation | The query has no `store` comment | Add `-- store: ExportedGroup` immediately after optional `shard` metadata |
| Invalid or misplaced store annotation | `store` is unexported, malformed, or appears after documentation | Use `-- store: ExportedGroup` directly after optional `shard` metadata |
| Unknown shard operand | A routing-only operand matches neither a SQL parameter nor a field on the query's generated table model | Use the table column name, respecting the configured sqlc schema and rename options |
| Conflicting route types | The same resolver method is inferred with incompatible operand types | Align the SQL parameter types or use different route names |
| Grouped `:many` result key is missing or ambiguous | A routed one-list query cannot associate returned rows with lookup items | Return exactly one field whose SQL name and Go type match the singular list parameter |
| A sharded store contains an unsharded query | One generated store mixes routing models | Add shard metadata or move the model and queries to another generated package |
| Generated code does not compile | sqlc and plugin options differ | Align pointer, rename, override, package, and parameter-limit options |

Regenerate with the pinned repository toolchain:

```bash
just --justfile examples/justfile generate
go test ./...
```

## Topology construction errors

`NewStore` validates the opaque topology before returning it:

- every replica-set name must be unique and non-empty;
- every configured primary and replica database must be present;
- every mapping must reference known replica sets;
- every virtual shard must be mapped exactly once;
- mirror lists for one main replica set must be consistent;
- the shard resolver and hasher must be present for sharded configurations.

Use `errors.Is` with the exported errors in
[`errors.go`](../../errors.go) when startup diagnostics need classification.

## A read cannot find a recent write

Default routed reads use configured replicas. PostgreSQL replication may not
have applied the write yet. Retry according to application policy or use:

```go
value, err := queries.Accounts().GetAccount(ctx, arg, db.ReadFromPrimary())
```

pgmesh does not monitor replication lag.

## A write returns an error but the primary changed

A synchronous mirror may have failed after the primary succeeded. Generated
methods preserve the primary result but return the first non-ignored mirror
error. Treat retries as potentially duplicating the primary operation and make
mirrored writes idempotent where possible. Do not switch a virtual-shard mapping
to the new database until the failure has been repaired and the databases have
been reconciled; follow the
[shard-expansion cutover guide](add-write-mirrors.md).

## A transaction reaches the wrong database

The transaction was probably opened from a pool that does not match the query's
resolved physical shard. Use the same resolver, hasher, and mapping held in the
store configuration to choose the retained primary pool. pgmesh cannot
validate the origin of a `pgx.Tx`.

## A batch method cannot be generated with sharding

This is intentional. pgmesh partitions routed `:copyfrom` inputs by physical
shard, but sqlc `:batch*` methods expose execution errors later through callback
result objects. Put batch operations in a separate unsharded generated store or
partition them explicitly in application code.

## A special operation reports a cross-shard transaction

`shard: all()` cannot run through one transaction. Routed list lookups and
`:copyfrom` calls can use `WithTx` only when every input resolves to the same
physical shard. In both cases pgmesh returns
`pgmesh.ErrCrossShardTransaction` before dispatching any database operation.

## A scatter or grouped copy partially committed

Physical shards do not share a transaction. Scatter and grouped-copy calls
attempt every target and return joined errors labeled by replica-set name, but
successful shards may already have committed. Make these writes idempotent and
reconcile failed shards before retrying.

## Local integration failures

Run the topology lifecycle in separate steps to inspect it:

```bash
just integration-up
just integration-test
just examples-smoke
just integration-down
```

The default ports and their `PGMESH_*` overrides are documented in
[Development and verification](../development.md#local-postgresql-integration).
Ensure Docker is available and no other process owns those ports.
