# Asynchronous COPY batching

This example configures the generated `CopyAccounts :copyfrom` query for
per-shard micro-batching. Two submissions target `shard-0` and coalesce into
one physical COPY; a third targets `shard-1` and executes independently.

The one-hour batch timeout is intentional: the program demonstrates the full
`EnqueueCopyAccounts` → `FlushCopyAccounts` → `Future.Await` lifecycle without
depending on a timer. `FlushCopyAccounts` forces every partial batch accepted
before its barrier to execute across both physical shards.

Apply [`examples/sqlc/schema.sql`](../sqlc/schema.sql) to both databases, then
set `ASYNC_COPY_SHARD0_DSN` and `ASYNC_COPY_SHARD1_DSN` before running:

```bash
go run ./examples/05-async-copy-batching
```

See [Use asynchronous COPY batching](../../docs/how-to/use-async-copy-batching.md)
for shutdown ordering, cancellation, partial-commit, and telemetry semantics.
