# Feature guide

Use this matrix to find the focused guide and runnable example for each
application-facing pgmesh feature.

| Feature | What it demonstrates | Guide | Runnable example |
| --- | --- | --- | --- |
| Generated query groups and capability interfaces | `Store`, group accessors, `Reader`/`Writer` interfaces | [Quickstart](quickstart.md) | [`01-single-database`](../examples/01-single-database) |
| Singleton topology | One primary with topology-independent query APIs | [Quickstart](quickstart.md) | [`01-single-database`](../examples/01-single-database) |
| Read replicas and strong reads | Replica round-robin and `ReadFromPrimary()` | [Add read replicas](how-to/add-read-replicas.md) | [`02-read-write-split`](../examples/02-read-write-split) |
| Virtual sharding | Stable shard keys, virtual-shard mappings, separate routed/unrouted stores | [Add sharding](how-to/add-sharding.md) | [`03-sharded-read-write`](../examples/03-sharded-read-write) |
| Write mirrors | Synchronous old-to-new dual writes for shard migration | [Expand shards](how-to/add-write-mirrors.md) | [`04-mirrors-and-transactions`](../examples/04-mirrors-and-transactions) |
| Shard-pinned transactions | Open on the selected primary and pass `WithTx()` | [Use transactions](how-to/use-transactions.md) | [`04-mirrors-and-transactions`](../examples/04-mirrors-and-transactions) |
| Asynchronous COPY batching | Per-shard coalescing, futures, explicit flush, shutdown barriers | [Use asynchronous COPY batching](how-to/use-async-copy-batching.md) | [`05-async-copy-batching`](../examples/05-async-copy-batching) |
| Query-group wrappers and cache-aside | `WithXXXFactory`, cache hits, delegated misses, write-through updates | [Add cache-aside behavior](how-to/add-cache-aside.md) | [`06-cache-aside`](../examples/06-cache-aside) |
| Grouped and scatter queries | List lookup grouping and explicit `shard: all()` fan-out | [Use multi-shard queries](how-to/use-multi-shard-queries.md) | [`07-multi-shard-queries`](../examples/07-multi-shard-queries) |
| Structured debug logging | Logical and physical query records with route attributes | [Enable structured logging](how-to/enable-logging.md) | [`03-sharded-read-write`](../examples/03-sharded-read-write) |
| OpenTelemetry metrics and traces | Wrapper/logical/physical latency, per-node throughput and concurrency, COPY telemetry | [Enable OpenTelemetry](how-to/enable-opentelemetry.md) | Application Collector/exporter configuration |
| Generator layouts and sqlc options | Same-package and separate-package output, renames, overrides | [Configure generation](how-to/configure-generation.md) | [`tests/same_package`](../tests/same_package) and [`tests/separate_package`](../tests/separate_package) fixtures |

OpenTelemetry exporters and Collectors remain application infrastructure, so
the telemetry guide uses provider injection and production PromQL instead of a
fake in-process exporter example. Generation edge cases use compile-checked
fixtures because they describe output layouts rather than runtime workflows.

For supported sqlc commands and route annotations, see
[Add a query](how-to/add-a-query.md). For deliberate non-goals and operational
boundaries, see [Purpose and rationale](purpose-and-rationale.md).

Integration scenarios follow the same feature boundaries. See the
[`tests` organization guide](../tests/README.md) for the directory-to-feature
map and the deliberately small mixed-feature policy.
