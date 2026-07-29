# Enable OpenTelemetry tracing and metrics

Generated internal store methods emit routed-query telemetry. A query group
configured with `With<Group>Factory` also emits separate store telemetry around
the application wrapper. When the wrapper delegates, the routed-query span is
a child of the store span.

Configure trace and metric SDKs and exporters in the application, then pass
their providers when building the topology:

```go
tracerProvider := sdktrace.NewTracerProvider(
    sdktrace.WithBatcher(traceExporter),
    sdktrace.WithResource(resource),
)
defer tracerProvider.Shutdown(context.Background())

meterProvider := sdkmetric.NewMeterProvider(
    sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
    sdkmetric.WithResource(resource),
)
defer meterProvider.Shutdown(context.Background())

store, err := db.NewStore(
    ctx,
    db.Singleton(pool),
    db.WithTracerProvider(tracerProvider),
    db.WithMeterProvider(meterProvider),
)
```

The store options apply to both singleton and sharded topologies. If providers
are not supplied, pgmesh uses OpenTelemetry's global providers, so applications that call
`otel.SetTracerProvider` and
`otel.SetMeterProvider` need no pgmesh-specific options. When no SDK is
configured, OpenTelemetry's default providers are no-ops. The application owns
provider lifecycle: build providers before the store, keep them alive while
queries run, then flush or shut them down after query traffic has stopped.
pgmesh never shuts down a provider.

pgmesh emits two metric instruments:

| Metric | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `pgmesh.store.duration` | Histogram | `s` | End-to-end duration of a configured factory wrapper |
| `pgmesh.query.duration` | Histogram | `s` | Generated internal routing and query duration |

Each histogram's count reports throughput for its layer. For a cache-aside
wrapper, filter `pgmesh.store.duration` by
`pgmesh.store.internal_executed=false` for hits and `true` for delegations.
`pgmesh.query.duration` counts internal executions. Both histograms use default
explicit bucket boundaries of `0.001`, `0.005`, `0.01`, `0.05`, `0.1`, `0.5`,
`1`, `5`, and `10` seconds; applications can override their aggregations with
SDK views.

Factory wrappers use span name `pgmesh.store.<Group>.<QueryName>`. Generated
internal methods use `pgmesh.query.<Group>.<QueryName>`. For example, a
cache-miss call to `store: Users` query `GetUser` produces:

```text
pgmesh.store.Users.GetUser
└── pgmesh.query.Users.GetUser
    └── optional instrumented pgx/database span
```

A cache hit produces only the store span. A group without a factory produces
only the query span.

sqlc batch methods that return a batch-results object do not emit completion
telemetry when queued because their database work finishes later through result
callbacks. Instrument batch consumption separately when those operations need
latency or outcome telemetry.

Store spans and `pgmesh.store.duration` points record:

| Attribute | When recorded | Value |
| --- | --- | --- |
| `pgmesh.store.name` | Always | Generated query-group name |
| `pgmesh.query.name` | Always | Generated query method name |
| `pgmesh.query.kind` | Always | `read` or `write` |
| `pgmesh.store.internal_executed` | Always | Whether the generated internal method was entered |
| `error.type` | On failure | Predictable Go error type |

Query spans and `pgmesh.query.duration` points record:

| Attribute | When recorded | Value |
| --- | --- | --- |
| `pgmesh.store.name` | Always | Generated query-group name |
| `pgmesh.query.name` | Always | Generated query method name |
| `pgmesh.query.kind` | Always | `read` or `write` |
| `pgmesh.route.replica_set` | After successful routing | Physical replica-set name |
| `pgmesh.route.mode` | After successful routing | `read`, `primary`, or `transaction` |
| `error.type` | On failure | Predictable Go error type |

Virtual-shard indexes are deliberately excluded from OpenTelemetry attributes
because they create one dimension value per virtual shard. Debug logs retain
the selected `vshard` for individual-query diagnosis. Scatter and grouped-copy
operations retain one logical span for the generated method and omit a
misleading single virtual-shard or replica-set value.

Store telemetry never includes physical route attributes; those belong to its
query child. `internal_executed=true` means the generated internal method was
entered, but does not guarantee a database request occurred because routing can
fail first. For a wrapper whose only short-circuit is a cache lookup,
`internal_executed=false` indicates a cache hit.

Routing, database, and mirror errors are recorded on the span and set its status
to error. Successful operations omit `error.type`.

The store-span context is passed to the application wrapper, and the query-span
context is passed into the selected generated query method. Wrappers must
delegate with the received context to preserve the parent/child relationship
and internal-execution marker. If the pgx pool is instrumented separately, its
database spans appear as children of the pgmesh query span.
