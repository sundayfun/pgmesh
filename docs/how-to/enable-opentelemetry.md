# Enable OpenTelemetry tracing and metrics

Generated stores separate telemetry into three layers:

1. A `store` measurement covers an optional application factory wrapper.
2. An `operation` measurement covers one logical generated-method call.
3. A `query` measurement covers one physical database execution on one shard
   and node.

This distinction matters for fan-out. One scatter query contributes one
operation data point and one query data point per physical shard. It therefore
reports both caller-visible latency and the throughput and latency handled by
each database node.

## Configure providers

Configure trace and metric SDKs and exporters in the application, then pass
their providers when building the store:

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

The options apply to singleton and sharded topologies. Without explicit
providers, pgmesh uses OpenTelemetry's global providers. Default providers are
no-ops when no SDK is configured. The application owns provider lifecycle;
pgmesh never shuts down a provider.

## Metrics

| Metric | Type | Unit | Meaning |
| --- | --- | --- | --- |
| `pgmesh.store.duration` | Histogram | `s` | End-to-end duration of a configured factory wrapper |
| `pgmesh.operation.duration` | Histogram | `s` | End-to-end duration of one logical generated-method call, including routing and fan-out aggregation |
| `pgmesh.query.duration` | Histogram | `s` | Duration of one physical database execution on one selected shard and node |
| `pgmesh.copy.batch.rows` | Histogram | `{row}` | Attempted rows in each physical COPY batch |
| `pgmesh.copy.batch.submissions` | Histogram | `{submission}` | Logical submission fragments represented in each physical COPY batch |
| `pgmesh.copy.batch.flushes` | Counter | `{batch}` | Physical COPY batches, grouped by flush reason |
| `pgmesh.copy.batch.duration` | Histogram | `s` | Physical COPY execution duration |
| `pgmesh.copy.queue.duration` | Histogram | `s` | Time from oldest-row admission until physical COPY execution begins |

Each histogram's count reports throughput for its layer:

- Use `pgmesh.operation.duration` for application-call QPS and latency.
- Use `pgmesh.query.duration` for database QPS and latency, grouped by
  `pgmesh.shard.name`, `pgmesh.node.name`, and optionally query name.
- Use `pgmesh.store.duration` with
  `pgmesh.store.internal_executed=false` for factory short-circuits such as
  cache hits, and `true` for delegations.

Duration histograms use explicit bucket boundaries of `0.001`, `0.005`,
`0.01`, `0.05`, `0.1`, `0.5`, `1`, `5`, and `10` seconds. Applications can
override aggregations with SDK views.

### Common PromQL queries

These examples target a current OpenTelemetry Collector and Prometheus setup
that [preserves UTF-8 OpenTelemetry names][prometheus-otel-utf8], using
`NoTranslation` or an equivalent configuration. PromQL quotes metric and label
names containing dots. Classic histogram components keep the structural
`_count`, `_sum`, and `_bucket` suffixes; for example,
`pgmesh.query.duration` produces `pgmesh.query.duration_count`. All latency
results below are in seconds.

Logical application-call QPS, by generated method:

```promql
sum by ("pgmesh.store.name", "pgmesh.query.name") (
  rate({"pgmesh.operation.duration_count"}[5m])
)
```

Logical application-call p95 latency:

```promql
histogram_quantile(
  0.95,
  sum by (le, "pgmesh.store.name", "pgmesh.query.name") (
    rate({"pgmesh.operation.duration_bucket"}[5m])
  )
)
```

Physical database QPS for every shard and selected node:

```promql
sum by ("pgmesh.shard.name", "pgmesh.node.name", "pgmesh.node.role") (
  rate({"pgmesh.query.duration_count"}[5m])
)
```

Physical database p95 latency for every shard and selected node:

```promql
histogram_quantile(
  0.95,
  sum by (le, "pgmesh.shard.name", "pgmesh.node.name", "pgmesh.node.role") (
    rate({"pgmesh.query.duration_bucket"}[5m])
  )
)
```

Add `"pgmesh.store.name"` and `"pgmesh.query.name"` to both `sum by` clauses when
the result needs a per-generated-method breakdown. Prometheus metrics aggregate
executions over time; use the physical query spans to see the exact shards used
by one individual operation.

Read-replica QPS can use the same throughput query with
`"pgmesh.node.role"="read_replica"`:

```promql
sum by ("pgmesh.shard.name", "pgmesh.node.name") (
  rate({
    "pgmesh.query.duration_count",
    "pgmesh.node.role"="read_replica"
  }[5m])
)
```

Read-replica p95 latency:

```promql
histogram_quantile(
  0.95,
  sum by (le, "pgmesh.shard.name", "pgmesh.node.name") (
    rate({
      "pgmesh.query.duration_bucket",
      "pgmesh.node.role"="read_replica"
    }[5m])
  )
)
```

Physical database error percentage for every generated method and target:

```promql
100 *
sum by (
  "pgmesh.store.name",
  "pgmesh.query.name",
  "pgmesh.shard.name",
  "pgmesh.node.name"
) (
  rate({
    "pgmesh.query.duration_count",
    "error.type"=~".+"
  }[5m])
)
/
sum by (
  "pgmesh.store.name",
  "pgmesh.query.name",
  "pgmesh.shard.name",
  "pgmesh.node.name"
) (
  rate({"pgmesh.query.duration_count"}[5m])
)
```

Physical executions per logical operation, grouped by generated method:

```promql
sum by ("pgmesh.store.name", "pgmesh.query.name") (
  rate({"pgmesh.query.duration_count"}[5m])
)
/
sum by ("pgmesh.store.name", "pgmesh.query.name") (
  rate({"pgmesh.operation.duration_count"}[5m])
)
```

A result near `1` means one physical query per logical operation. Values above
`1` expose fan-out amplification, including `SetMultiRoute` operations.

Factory-wrapper short-circuit percentage, such as cache hits:

```promql
100 *
sum by ("pgmesh.store.name", "pgmesh.query.name") (
  rate({
    "pgmesh.store.duration_count",
    "pgmesh.store.internal_executed"="false"
  }[5m])
)
/
sum by ("pgmesh.store.name", "pgmesh.query.name") (
  rate({"pgmesh.store.duration_count"}[5m])
)
```

Physical COPY p95 latency for every shard and selected node:

```promql
histogram_quantile(
  0.95,
  sum by (le, "pgmesh.shard.name", "pgmesh.node.name") (
    rate({"pgmesh.copy.batch.duration_bucket"}[5m])
  )
)
```

See Prometheus's [`histogram_quantile` documentation][histogram-quantile] for
other quantiles and aggregation patterns. Keep `le` in the `sum by` clause
when querying these classic histograms.

### Operation attributes

`pgmesh.operation.duration` records a stable, bounded set of dimensions:

| Attribute | Value |
| --- | --- |
| `pgmesh.store.name` | Generated query-group name |
| `pgmesh.query.name` | Generated query method name |
| `pgmesh.query.kind` | `read` or `write` |
| `pgmesh.route.mode` | `read`, `primary`, `transaction`, or `unresolved` |
| `pgmesh.route.scope` | `single`, `fanout`, or `unresolved` |
| `error.type` | Error type on failure; omitted on success |

Operation spans and logs also include `pgmesh.route.shard_count`. The count is
not a metric dimension because arbitrary fan-out sizes would create unnecessary
time series.

### Physical-query attributes

Every `pgmesh.query.duration` data point identifies the exact selected target:

| Attribute | Value |
| --- | --- |
| `pgmesh.store.name` | Generated query-group name |
| `pgmesh.query.name` | Generated query method name |
| `pgmesh.query.kind` | `read` or `write` |
| `pgmesh.shard.name` | Physical shard (replica-set) topology name |
| `pgmesh.node.name` | `primary`, `replica-N`, or `transaction` |
| `pgmesh.node.role` | `primary`, `read_replica`, or `transaction` |
| `pgmesh.route.mode` | `read`, `primary`, or `transaction` |
| `error.type` | Error type on failure; omitted on success |

Replica ordinals follow configuration order. For example,
`WithReadReplicas(replica0, replica1)` produces `replica-0` and `replica-1`.
When no read replicas are configured, ordinary reads use node `primary` with
role `primary` while retaining route mode `read`.

For an externally supplied transaction, pgmesh cannot inspect the transaction's
underlying pool or connection. It therefore uses node name and role
`transaction` instead of falsely attributing that execution to the configured
primary.

Physical-query spans and debug logs additionally include
`pgmesh.shard.virtual` when one exact virtual shard was selected. Fan-out,
grouped, and physical-shard scan queries omit it because one physical execution
can represent several virtual shards. The attribute is always excluded from
metrics: virtual shard counts can be large, whereas physical shard and node
identities stay bounded by the deployed topology.

### COPY attributes

All physical COPY metrics use the physical-query attributes above and add
`pgmesh.copy.batch.flush_reason`, whose value is `size`, `timeout`, `explicit`,
or `immediate`. A failed batch still contributes its attempted row count and
adds `error.type`.

`pgmesh.copy.batch.rows` sum divided by count gives average physical batch
size. The equivalent calculation for `pgmesh.copy.batch.submissions` gives the
average number of logical submissions coalesced into each batch.

## Traces

Factory wrappers use `pgmesh.store.<Group>.<QueryName>`, logical operations use
`pgmesh.operation.<Group>.<QueryName>`, and physical queries use
`pgmesh.query.<Group>.<QueryName>`. A cache miss produces:

```text
pgmesh.store.Users.GetUser
└── pgmesh.operation.Users.GetUser
    └── pgmesh.query.Users.GetUser
        └── optional instrumented pgx/database span
```

A cache hit produces only the store span. A group without a factory begins at
the operation span. A fan-out operation has one query child per physical shard
execution.

Routing and database errors mark both the affected physical query and its
logical operation as errors. A mirror error is reflected on the primary write
query and operation because synchronous mirrors are part of that selected
write target.

## Asynchronous and batch behavior

sqlc batch methods returning a batch-results object do not emit completion
telemetry when queued because their database work finishes later through
result callbacks. Instrument batch consumption separately when those methods
need latency or outcome telemetry.

Generated `Enqueue<CopyQuery>` operation spans and metrics end when their future
resolves, so they include queueing and COPY time. Each physical COPY execution
also emits its own query span and metric point. `Flush<CopyQuery>` is a control
operation and emits no operation point of its own; a batch forced by it emits
query and COPY metrics, with COPY flush reason `explicit`.

Wrappers must delegate with the received context to preserve the store →
operation → query hierarchy and the internal-execution marker. Pass the
physical query context to the database call so separately instrumented pgx
spans appear beneath the correct pgmesh query span.

[histogram-quantile]: https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile
[prometheus-otel-utf8]: https://prometheus.io/docs/guides/opentelemetry/#utf-8
