# Test organization

Integration scenarios live directly under the feature they verify. Each
directory should remain readable without understanding unrelated runtime
features.

| Directory | Focus |
| --- | --- |
| [`read_replicas`](read_replicas) | Replica rotation, primary reads, and primary fallback |
| [`sharding`](sharding) | Virtual-shard write routing |
| [`copy`](copy) | Synchronous `COPY FROM` grouping |
| [`async_copy`](async_copy) | Asynchronous batching, flush, and futures |
| [`grouped_queries`](grouped_queries) | Per-key routing and result-order restoration |
| [`scatter`](scatter) | Explicit all-shard reads, writes, and transaction rejection |
| [`write_mirrors`](write_mirrors) | Singleton and sharded mirrors, errors, and COPY mirroring |
| [`query_group_factory`](query_group_factory) | Generated group wrappers and cache behavior |
| [`telemetry`](telemetry) | Wrapper, logical, and physical span hierarchy |
| [`type_mapping`](type_mapping) | Real PostgreSQL nullable, network, enum, and range scans |
| [`transactions`](transactions) | Shard-pinned transactions and one mixed composition scenario |

`transactions/mixed_features_integration_test.go` combines transaction
pinning, sharding, synchronous COPY, and mirror suppression. Keep mixed
coverage small; new behavior belongs in its focused feature directory first.

The generator-layout fixtures are also top-level features:

- [`same_package`](same_package) owns the shared generated API and schema used
  by the runtime feature tests.
- [`separate_package`](separate_package) verifies exported sqlc types across
  package boundaries.
- [`command_shapes`](command_shapes) verifies focused CRUD, batch, and COPY
  command shapes.

Reusable topology code belongs under [`internal`](internal). It must contain
setup and assertions only, never feature scenarios.

Run one feature against an already-running local topology with:

```bash
just integration-test-feature async_copy
```

Run the complete Docker-backed suite with `just integration`.
