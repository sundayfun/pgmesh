# pgmesh

[![Test](https://github.com/sundayfun/pgmesh/actions/workflows/test.yml/badge.svg)](https://github.com/sundayfun/pgmesh/actions/workflows/test.yml)
[![Lint](https://github.com/sundayfun/pgmesh/actions/workflows/lint.yml/badge.svg)](https://github.com/sundayfun/pgmesh/actions/workflows/lint.yml)
[![Integration](https://github.com/sundayfun/pgmesh/actions/workflows/integration.yml/badge.svg)](https://github.com/sundayfun/pgmesh/actions/workflows/integration.yml)

**Type-safe PostgreSQL query routing for sqlc and pgx/v5.**

## Why

sqlc generates type-safe queries, but it does not express where each query may
run. As an application adds read replicas or shards, routing logic can spread
through business code and make it easy to send a write to the wrong database.

pgmesh keeps that policy in generated Go types and one validated runtime
topology.

## What

pgmesh is a sqlc process plugin plus a small Go runtime. Together they provide:

- separate read, write, and primary-capable query APIs;
- mandatory query groups for keeping large generated stores navigable;
- replica reads with explicit primary reads when consistency requires them;
- logical-key routing through virtual shards to physical databases;
- explicit scatter queries, grouped list lookups, and physical-shard grouping
  for `COPY FROM`;
- shard-pinned transactions;
- synchronous write mirrors for staged shard expansion; and
- OpenTelemetry instrumentation and structured debug logging.

It is not a database proxy, connection pool, replication system, data migration
tool, or distributed transaction coordinator. Your application keeps control
of those concerns.

## How

Install the runtime and generator:

```bash
go get github.com/sundayfun/pgmesh
go install github.com/sundayfun/pgmesh/cmd/sqlc-gen-store@latest
```

Classify and group each sqlc query. Add a shard route only when the query
should be routed automatically:

```sql
-- name: GetAccount :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Accounts
SELECT * FROM accounts WHERE tenant_id = $1 AND id = $2;

-- name: UpsertAccount :one
-- kind: write
-- shard: tenant(tenant_id)
-- store: Accounts
INSERT INTO accounts (id, tenant_id, display_name) VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE SET display_name = EXCLUDED.display_name
RETURNING *;
```

Register the process plugin in `sqlc.yaml`, then generate both sqlc's queries
and pgmesh's wrappers:

```bash
sqlc generate
```

Construct the generated `Store` with a singleton topology:

```go
queries, err := db.NewStore(ctx, db.Singleton(pool))
account, err := queries.Accounts().GetAccount(ctx, &db.GetAccountParams{
    TenantID: tenantID,
    ID:       accountID,
})
```

When the deployment grows, replace `Singleton(...)` with a `Sharded(...)`
topology.
`queries` remains the same `db.Store` interface, so business code does not
depend on whether pgmesh uses one database, replicas, mirrors, or shards.

Follow the [quickstart](docs/quickstart.md) for a complete working setup, or
explore the [progressive examples](examples) from one database through replicas,
sharding, write mirrors, and transactions.

## Documentation

- [Documentation index](docs/README.md)
- [Topology concepts and request-routing flow](docs/topology.md)
- [Purpose, design, and non-goals](docs/purpose-and-rationale.md)
- [How-to guides](docs/how-to/README.md)
- [Development and verification](docs/development.md)

## License

[MIT](LICENSE)
