# Cache-aside query-group wrapper

This example wraps the generated `Settings` query group with
`WithSettingsFactory`. The wrapper embeds the generated interface, overrides
one read and one write, and leaves every other generated method delegated.

The first `GetSetting` is a database-backed cache miss and the second is a
cache hit. `UpsertSetting` updates the cached value after the primary write
succeeds. Reads carrying a generated query option bypass the cache so primary
and transaction consistency semantics remain intact.

The in-memory map is intentionally small and process-local. A production cache
still needs an expiration, invalidation, stampede, and consistency policy.

Apply [`examples/sqlc/schema.sql`](../sqlc/schema.sql), set
`CACHE_DATABASE_DSN`, and run:

```bash
go run ./examples/06-cache-aside
```

See [Add cache-aside behavior](../../docs/how-to/add-cache-aside.md) for the
factory lifecycle and telemetry behavior.
