# sqlc process-plugin example

This directory is a minimal PostgreSQL/sqlc project showing the annotation
grammar and process-plugin configuration. Use the dedicated examples
`justfile` to build the local plugin and run the pinned sqlc version. From the
module root:

```bash
cd examples
just generate
```

The checked-in generated packages expose only their topology-independent
`Store` interfaces and config-driven constructors. `internal/sharded` contains
the account store with shard annotations; `internal/one` contains the settings
store with its own model and no shard routes. Both are called through the same
generated API shape even though their internal routing differs.

The sharded query set covers single-key routing, grouped list lookup,
all-shard scatter, and routed `COPY FROM`. The focused programs in the parent
directory exercise those generated shapes against singleton, replica, and
sharded topologies.

The checked-in `tests/same_package`, `tests/separate_package`, and
`tests/command_shapes` fixtures compile those generator layouts. Focused
runtime directories under `tests` are exercised against five local PostgreSQL
databases by the module root's `just verify` recipe.
