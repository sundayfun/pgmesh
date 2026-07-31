# Quickstart

This guide starts with one PostgreSQL database. It generates grouped,
read/write-aware wrappers; sharding is an optional next step.

## Prerequisites

You need Go, PostgreSQL, sqlc, and pgx/v5. This repository pins its development
sqlc version in [`just/toolings.just`](../just/toolings.just).

Add pgmesh and pgx to your application:

```bash
go get github.com/sundayfun/pgmesh
go get github.com/jackc/pgx/v5
```

Install the process plugin somewhere on `PATH`:

```bash
go install github.com/sundayfun/pgmesh/cmd/sqlc-gen-store@latest
```

When working from a pgmesh checkout instead, build the local plugin with:

```bash
go build -o bin/sqlc-gen-store ./cmd/sqlc-gen-store
```

## 1. Add a schema

Create `db/schema.sql`:

```sql
CREATE TABLE accounts (
    id BIGINT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    display_name TEXT NOT NULL
);
```

Apply the schema to the database using your normal migration tool.

## 2. Add annotated queries

Create `db/queries.sql`:

```sql
-- name: GetAccount :one
-- kind: read
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
WHERE id = $1;

-- name: UpsertAccount :one
-- kind: write
-- store: Accounts
INSERT INTO accounts (id, tenant_id, display_name)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
    display_name = EXCLUDED.display_name
RETURNING id, tenant_id, display_name;
```

Every query in a pgmesh-managed store needs `kind: read` or `kind: write`
immediately after the sqlc `name` annotation. Every query also needs a
`-- store: ExportedGroup` annotation after `kind` (or after `shard` when
present). Queries are called through their generated group, such as
`queries.Accounts().GetAccount(...)`.

## 3. Configure sqlc and pgmesh

Create `sqlc.yaml`. The plugin options that affect generated Go types should
match the corresponding sqlc Go generator options.

```yaml
version: "2"
plugins:
  - name: "pgmesh"
    process:
      cmd: "sqlc-gen-store"

sql:
  - engine: "postgresql"
    schema: "db/schema.sql"
    queries: "db/queries.sql"
    gen:
      go:
        package: "db"
        out: "internal/db"
        sql_package: "pgx/v5"
        emit_interface: true
        query_parameter_limit: 1
        emit_params_struct_pointers: true
        emit_result_struct_pointers: true
        emit_pointers_for_null_types: true
    codegen:
      - plugin: "pgmesh"
        out: "internal/db"
        options:
          package: "db"
          output_file_name: "zz_generated_store.go"
          sql_package: "pgx/v5"
          query_parameter_limit: 1
          emit_params_struct_pointers: true
          emit_result_struct_pointers: true
          emit_pointers_for_null_types: true
```

If `sqlc-gen-store` is not on `PATH`, set `process.cmd` to its absolute or
project-relative path.

## 4. Generate the package

```bash
sqlc generate
```

Alongside sqlc's output, pgmesh generates the public root `Store`, its query
group interfaces, `NewStore`, topology option APIs, and private routing
executors.

Commit generated files when your project checks them in. Regenerate them after
every schema, query, annotation, or relevant sqlc option change.

## 5. Use the generated store

Replace `example.com/app/internal/db` with your generated package path:

```go
package main

import (
    "context"
    "log"

    "github.com/jackc/pgx/v5/pgxpool"

    "example.com/app/internal/db"
)

func main() {
    ctx := context.Background()
    pool, err := pgxpool.New(ctx, "postgres://user:pass@localhost/app?sslmode=disable")
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close()

    queries, err := db.NewStore(ctx, db.Singleton(pool))
    if err != nil {
        log.Fatal(err)
    }
    accounts := queries.Accounts()
    account, err := accounts.UpsertAccount(ctx, &db.UpsertAccountParams{
        ID:          1,
        TenantID:    42,
        DisplayName: "Ada",
    })
    if err != nil {
        log.Fatal(err)
    }

    loaded, err := accounts.GetAccount(ctx, account.ID)
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("loaded %s", loaded.DisplayName)
}
```

At this stage both methods use the same pool. Adding replicas or changing to a
sharded configuration does not change the `db.Store` query API.

## Next steps

- [Choose a feature and runnable example](features.md)
- [Add a query](how-to/add-a-query.md)
- [Use asynchronous COPY batching](how-to/use-async-copy-batching.md)
- [Add read replicas](how-to/add-read-replicas.md)
- [Add sharding](how-to/add-sharding.md)
- [Expand shards with synchronous dual writes](how-to/add-write-mirrors.md)
- Explore the [runnable examples](../examples)
