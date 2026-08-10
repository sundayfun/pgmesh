# Enable structured debug logging

Generated stores emit structured slog records for logical query calls,
physical database executions, and configured application wrappers. Logging is
disabled by default and is independent of OpenTelemetry tracing and metrics.

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

A physical database record has Debug level, message
`pgmesh physical query completed`, and these attributes:

| Attribute | Value |
| --- | --- |
| `store_name` | Generated query-group name |
| `query_name` | Generated query method name |
| `query_kind` | `read` or `write` |
| `failed` | Boolean error outcome |
| `duration` | Physical execution duration as a `slog.Duration` value |
| `virtual_shard` | Exact virtual shard, encoded as a string and present only when known |
| `replica_set` | Physical replica-set name |
| `node` | `primary`, `replica-N`, or `transaction` |
| `node_role` | `primary`, `read_replica`, or `transaction` |
| `route_mode` | `read`, `primary`, or `transaction` |
| `error` | Error value, present only when the execution failed |

A logical call produces `pgmesh logical query completed`. It has the common
query attributes plus `route_mode`, `route_scope`, and `replica_set_count`; its
duration includes routing, physical executions, and fan-out aggregation.

A configured factory also produces a Debug record with message
`pgmesh store query completed` and these attributes:

| Attribute | Value |
| --- | --- |
| `store_name` | Generated query-group name |
| `query_name` | Generated query method name |
| `query_kind` | `read` or `write` |
| `store_delegated` | Whether the wrapper called the generated logical query |
| `failed` | Boolean error outcome |
| `duration` | End-to-end wrapper duration |
| `error` | Error value, present only when the wrapper failed |

A cache hit produces only the store record with `store_delegated=false`.
A miss logs each physical execution, then the logical completion, and finally
the store completion with `store_delegated=true`. A routing failure has no
physical record because no shard was selected. Contexts are passed to
`LogAttrs`, allowing a context-aware slog handler or OpenTelemetry logging
bridge to add trace correlation fields.

sqlc batch methods that return a batch-results object do not log completion
when queued because execution finishes later through result callbacks.
Generated asynchronous copy methods log completion when their returned future
resolves, so their duration includes queueing and physical COPY time. Explicit
`Flush<CopyQuery>` control calls do not produce query-completion records.
