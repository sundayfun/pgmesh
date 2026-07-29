# Enable structured debug logging

Generated stores emit structured slog records when routed queries and
factory-wrapped store calls complete. Logging is disabled by default and is
independent of OpenTelemetry tracing and metrics.

Create a logger whose handler enables Debug records, then pass it when building
the topology:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug,
}))

store, err := db.NewStore(
    ctx,
    db.Singleton(pool),
    db.WithLogger(logger),
)
```

`WithLogger` applies to both singleton and sharded topologies. Omitting it or
passing nil disables logging.
pgmesh does not modify the logger or its handler; if the handler's minimum
level is higher than Debug, the records are filtered normally.

An internal query record has Debug level, message `pgmesh query completed`, and
these attributes:

| Attribute | Value |
| --- | --- |
| `store_name` | Generated query-group name |
| `query_name` | Generated query method name |
| `query_kind` | `read` or `write` |
| `failed` | Boolean error outcome |
| `duration` | End-to-end duration as a `slog.Duration` value |
| `vshard` | Virtual shard index, encoded as a string |
| `replica_set` | Physical replica-set name |
| `route_mode` | `read`, `primary`, or `transaction` |
| `error` | Error value, present only when the operation failed |

A configured factory also produces a Debug record with message
`pgmesh store completed` and these attributes:

| Attribute | Value |
| --- | --- |
| `store_name` | Generated query-group name |
| `query_name` | Generated query method name |
| `query_kind` | `read` or `write` |
| `internal_store_executed` | Whether the generated internal method was entered |
| `failed` | Boolean error outcome |
| `duration` | End-to-end wrapper duration |
| `error` | Error value, present only when the wrapper failed |

A cache hit produces only the store record with
`internal_store_executed=false`. A miss logs the internal query first, including
its route, then the outer store completion with `internal_store_executed=true`.
A routing failure has no query route attributes because no shard was selected.
Contexts are passed to `LogAttrs`, allowing a context-aware slog handler or
OpenTelemetry logging bridge to add trace correlation fields.

sqlc batch methods that return a batch-results object do not log completion
when queued because execution finishes later through result callbacks.
