# Progressive usage examples

These programs use the sqlc-generated `internal/sharded` account package and
the `internal/one` unsharded settings package, building from the simplest
deployment to the full runtime topology.

Account commands are accessed through the generated `Accounts()` group,
tenant reports through `Reports()`, and settings through `Settings()`. Helper
functions use narrower generated reader and writer interfaces where
appropriate.

| Example | Topology | Features |
| --- | --- | --- |
| [`01-single-database`](01-single-database) | One PostgreSQL database | Generated `Store` with a `Singleton` topology |
| [`02-read-write-split`](02-read-write-split) | One primary and one or more replicas | The same `Store`, replica reads, round-robin selection, strong reads |
| [`03-sharded-read-write`](03-sharded-read-write) | Two physical shards, each with a replica | Virtual shards, read/write splitting, structured logging, separate settings store |
| [`04-mirrors-and-transactions`](04-mirrors-and-transactions) | Two sharded primary/replica sets plus future-shard mirrors | Staged shard-expansion dual writes, primary-pinned transactions, mirror suppression in transactions |
| [`05-async-copy-batching`](05-async-copy-batching) | Two sharded primaries | Per-shard COPY coalescing, futures, explicit flush barrier |
| [`06-cache-aside`](06-cache-aside) | One PostgreSQL database | Generated query-group factory, cache miss/hit, write-through update |
| [`07-multi-shard-queries`](07-multi-shard-queries) | Two sharded primaries | Grouped list lookup and explicit all-shard scatter read |

The source schema and annotated queries are in [`sqlc`](sqlc). The examples
have their own [`justfile`](justfile), which uses the repository's pinned sqlc
version. From the module root, regenerate the shared package with:

```bash
cd examples
just generate
```

Every program reads DSNs from environment variables. Apply `sqlc/schema.sql`
to each database before running one. For example:

```bash
SINGLE_DATABASE_DSN='postgres://user:pass@localhost/accounts?sslmode=disable' \
  go run ./examples/01-single-database
```

Replica examples assume PostgreSQL replication is configured outside this
library. A write followed immediately by a default replica read can therefore
observe normal replication lag; use the generated `ReadFromPrimary()` option
when read-your-write consistency is required.

`go test ./examples/...` compile-checks every program. From the module root,
`just verify` starts the local PostgreSQL topology, runs the generated-package
integration suite, and executes all seven programs as smoke tests. Run an
individual example with the environment variables listed in its README for a
visible end-to-end flow.
