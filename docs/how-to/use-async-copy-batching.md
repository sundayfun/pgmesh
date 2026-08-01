# Use asynchronous COPY batching

Every sqlc `:copyfrom` query generates synchronous, asynchronous, and explicit
flush APIs. For a query named `CopyAccounts`, pgmesh generates:

| API | Purpose |
| --- | --- |
| `CopyAccounts` | Route and execute the rows synchronously |
| `CopyAccountsAsync` | Accept rows and return a `*pgmesh.Future[int64]` |
| `FlushCopyAccounts` | Force partial batches to execute and wait at a barrier |
| `WithCopyAccountsBatching` | Configure per-shard batch size and timeout |

The suffix always follows the sqlc query name, so `CopyEvents` generates
`CopyEventsAsync`, `FlushCopyEvents`, and `WithCopyEventsBatching`.

## Define the COPY query

Annotate a normal sqlc `:copyfrom` statement with its route and store group:

```sql
-- name: CopyAccounts :copyfrom
-- kind: write
-- shard: tenantKey(tenant_id)
-- store: Accounts
INSERT INTO accounts (id, tenant_id, display_name)
VALUES ($1, $2, $3);
```

Regenerate the store, then enable coalescing when constructing it:

```go
store, err := db.NewStore(
    ctx,
    topology,
    db.WithCopyAccountsBatching(pgmesh.CopyBatchConfig{
        BatchSize:    500,
        FlushTimeout: 5 * time.Millisecond,
    }),
)
if err != nil {
    return err
}
```

`BatchSize` is the maximum number of rows in one physical COPY on one shard.
Zero leaves timed batches unbounded. A zero `FlushTimeout` uses
`pgmesh.DefaultCopyBatchFlushTimeout`; negative values make store construction
fail.

## Submit, flush, and await

Keep every returned future. `CopyAccountsAsync` preflights routing and then
allows rows from concurrent callers to share physical COPY batches on the same
shard:

```go
first := store.Accounts().CopyAccountsAsync(ctx, []*db.CopyAccountsT{{
    TenantKey:   db.TenantKey{TenantID: 20},
    ID:          1001,
    DisplayName: "Ada",
}})
second := store.Accounts().CopyAccountsAsync(ctx, []*db.CopyAccountsT{{
    TenantKey:   db.TenantKey{TenantID: 100},
    ID:          1002,
    DisplayName: "Linus",
}})

// Force every partial CopyAccounts batch to execute now. This waits for all
// CopyAccounts submissions accepted before the barrier, across every shard.
flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
if err := store.Accounts().FlushCopyAccounts(flushCtx); err != nil {
    return fmt.Errorf("flush account copies: %w", err)
}

for index, future := range []*pgmesh.Future[int64]{first, second} {
    count, err := future.Await(ctx)
    if err != nil {
        return fmt.Errorf("await account copy %d: %w", index, err)
    }
    log.Printf("copy %d wrote %d rows", index, count)
}
```

`FlushCopyAccounts` does not return row counts, so it does not replace
`Future.Await`. Always await every future: routing failures happen before work
is accepted by a batcher and therefore may be visible only through that
future.

## Understand the flush barrier

`FlushCopyAccounts` is per generated COPY query, not a global store flush. It:

1. Forces the current partial batch on every physical shard to become ready.
2. Waits for submissions that were outstanding when the flush began.
3. Returns joined, shard-labeled execution errors from that barrier.

Submissions accepted after the barrier are not included. Flush does not close
the batcher and producers may submit more work afterward. Stop producers
before using it as a graceful-shutdown drain:

```go
producers.Wait()

shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := store.Accounts().FlushCopyAccounts(shutdownCtx); err != nil {
    return fmt.Errorf("drain account copies: %w", err)
}
```

Call the generated flush method for every `:copyfrom` query that may still have
outstanding work. It is also useful in tests to avoid waiting for a batch
timeout.

If the flush context expires, only that wait stops. Accepted database work
continues, and a later `Await` or flush can observe its result. Similarly,
canceling the async call context after acceptance or canceling an `Await` does not
cancel the write.

## Operational boundaries

- Do not mutate submitted rows or referenced data until the future resolves.
- Different physical shards batch and execute independently.
- One submission may split across shards or `BatchSize` boundaries; its future
  resolves only after every fragment completes.
- Without `WithCopyAccountsBatching`, `CopyAccountsAsync` remains asynchronous but each
  call uses an immediate physical COPY per targeted shard. Flush still acts as
  a barrier for outstanding work.
- Async COPY cannot use a transaction or `QueryOption`. Use synchronous
  `CopyAccounts(..., db.WithTx(tx))` when all rows target one shard and must be
  transactional.
- Batches and shards can commit independently. A later failure can make a
  future return a zero count even though earlier fragments committed.
- Batching is in memory and does not limit the outstanding backlog. Await
  futures or enforce application-level admission control.

## Observe explicit flushes

An explicit flush produces normal `pgmesh.query.physical.duration` points for
the database executions. COPY metrics use
`pgmesh.copy.batch.flush_reason="explicit"`; the
`pgmesh.copy.batch.flushes` counter and COPY duration/row histograms can be
grouped by shard and node. `FlushCopyAccounts` itself is a control barrier and
does not add a separate logical-query duration point.

See the runnable
[`05-async-copy-batching` example](../../examples/05-async-copy-batching)
for a complete `CopyAccountsAsync` → `FlushCopyAccounts` → `Await` flow.
