# Configure and regenerate code

This page is the authoritative reference for the pgmesh sqlc process plugin.
The generator intentionally supports PostgreSQL through `pgx/v5` only. Unknown
options are errors, so removed fields and misspellings fail generation instead
of silently changing the generated API.

## Keep sqlc and pgmesh aligned

sqlc generates models and query methods. pgmesh generates wrappers that refer
to those types. Copy signature-affecting settings from `gen.go` into the pgmesh
`codegen.options` block:

```yaml
version: "2"
plugins:
  - name: "pgmesh"
    process:
      cmd: "./bin/sqlc-gen-store"

sql:
  - engine: "postgresql"
    schema: "schema.sql"
    queries: "queries.sql"
    gen:
      go:
        package: "db"
        out: "internal/db"
        sql_package: "pgx/v5"
        emit_interface: true
        query_parameter_limit: 1
        emit_params_struct_pointers: true
    codegen:
      - plugin: "pgmesh"
        out: "internal/db"
        options:
          package: "db"
          output_file_name: "zz_generated_store.go"
          sql_package: "pgx/v5"
          query_parameter_limit: 1
          emit_params_struct_pointers: true
```

`sql_package` must be `pgx/v5`. Transaction support is always generated.

## Supported options

| Option | Type | Default | Purpose |
| --- | --- | --- | --- |
| `output_file_name` | string | `store_querier.go` | Base `.go` filename and stem for the generated file set. It must be a visible base filename and cannot end in `_test.go`. |
| `package` | string | sqlc output directory basename, otherwise `db` | Go package clause for wrappers. |
| `internal_import_path` | string | empty | Imports sqlc output when wrappers use a separate package. |
| `internal_import_alias` | string | `internal` | Optional valid Go alias for `internal_import_path`. |
| `export_sqlc_types` | boolean | `false` | Re-exports sqlc parameter, row, model, and dependent package-local types used by the store API. Requires `internal_import_path`. |
| `constructor` | exported identifier | `NewStore` | Public constructor that accepts an opaque `Topology`. |
| `store_interface` | exported identifier | `Store` | Public topology-independent query interface. |
| `resolver_interface` | exported identifier | `ShardResolver` | Public generic shard resolver interface. |
| `sharded_constructor` | exported identifier | `Sharded` | Public sharded `Topology` constructor. |
| `runtime_import_path` | string | `github.com/sundayfun/pgmesh` | Runtime import override, primarily for forks. |
| `ignore_mirror_error` | boolean | `false` | Discards mirror errors. By default only mirror `ErrNoRows` is ignored. |
| `sql_package` | string | `pgx/v5` | sqlc driver selection; no other value is supported. |
| `query_parameter_limit` | nonnegative integer | `1` | Matches sqlc's parameter-struct threshold. |
| `emit_params_struct_pointers` | boolean | `false` | Matches sqlc parameter-struct pointer emission. |
| `emit_result_struct_pointers` | boolean | `false` | Matches sqlc result-struct pointer emission. |
| `emit_pointers_for_null_types` | boolean | `false` | Matches sqlc pgx/v5 nullable pointer emission. |
| `emit_pointers_for_null_enum_types` | boolean | inherits nullable pointer setting | Overrides nullable enum pointer emission. |
| `emit_exact_table_names` | boolean | `false` | Matches sqlc's table-derived Go naming. |
| `rename` | map of string to Go identifier | empty | Matches sqlc identifier renames. |
| `overrides` | override list | empty | Matches sqlc database-type and column overrides. |

The four customizable public names must be exported, valid Go identifiers,
distinct from one another, and free of generated declaration conflicts.
`internal_import_alias` must not conflict with imports used by generated code.

### Override format

Each override specifies exactly one of `db_type` or `column` and a nonempty
`go_type`:

```yaml
overrides:
  - db_type: "pg_catalog.uuid"
    nullable: false
    go_type: "github.com/google/uuid.UUID"
  - column: "users.external_id"
    go_type:
      import: "example.com/app/domain"
      package: "domain"
      type: "UserID"
      pointer: true
      slice: false
```

Supported fields are `db_type`, `column`, `nullable`, `unsigned`, and
`go_type`. Column patterns accept `*` and `?`; escape literal `*`, `?`, or `\`
with `\`. `go_type` supports sqlc's string form and map form shown above.

## Generated API and file layout

`zz_generated_store.go` produces:

- `zz_generated_store_interfaces.go` for optional sqlc type aliases plus the root `Store`, private executor, and shard resolver contracts;
- `zz_generated_store_read.go` for the private read executor;
- `zz_generated_store_write.go` for private primary and mirror execution;
- `zz_generated_store.go` for query and store options, `Topology`, `Singleton`, `NewStore`, and shared routing machinery;
- `zz_generated_store_<group>.go` for each `store:` annotation's sub-store interfaces, accessor, and routed query methods, with the group name converted to snake case;
- `zz_generated_store_sharded.go` for sharded functional options and topology construction.

The sharded file contains only its generated header and package clause when no
query has shard metadata. This ensures regeneration removes obsolete routed
code.

sqlc does not remove process-plugin files that disappear from a generation
response. After renaming or removing a `store:` group, remove its obsolete
`zz_generated_store_<group>.go` file.

The stable default public surface is:

| Symbol | Purpose |
| --- | --- |
| `Store` | Topology-independent root exposing every required query group |
| `<Group>`, `<Group>Reader`, `<Group>Writer` | Combined and read/write-separated interfaces derived from `store` annotations |
| `With<Group>Factory(func(<Group>) <Group>)` | Optional per-group wrapper factory |
| `NewStore(ctx, topology, ...StoreOption)` | Store constructor, group factories, and common telemetry options |
| `Singleton(primary, ...SingletonOption)` | Single-primary topology with optional replicas and mirrors |
| `Sharded(numVShards, hasher, resolver, ...ShardedOption)` | Sharded topology, emitted for routed stores |
| `ReadFromPrimary`, `WithTx` | Per-query routing options |

Repeated topology options append in call order. Common scalar store options,
such as `WithLogger` and `With<Group>Factory`, use the last supplied value.
Group factories run once after successful topology construction, and a nil
factory leaves that group unwrapped. A configured group is retained behind a
generated telemetry facade that records `pgmesh.store.duration`, emits a
`pgmesh.store.*` span, and reports `pgmesh.store.internal_executed`.
Option constructors clone slice inputs, and `NewStore` reports nil topology,
singleton, sharded, or store options as configuration errors.

Internal sqlc integration is fixed to `Querier`, `Queries`, and `New`.
Generated implementation names such as `queryStore`, `readQueries`,
`writeQueries`, and `meshStore` are private and not configurable.

## Separate-package generation

When wrapper output differs from sqlc Go output, configure its import:

```yaml
gen:
  go:
    package: "internal"
    out: "internal/db"
codegen:
  - plugin: "pgmesh"
    out: "internal/store"
    options:
      package: "store"
      internal_import_path: "example.com/app/internal/db"
      internal_import_alias: "db"
      export_sqlc_types: true
      output_file_name: "zz_generated_store.go"
```

With `export_sqlc_types`, the wrapper package emits Go type aliases for the
sqlc-owned types used by its generated query signatures. Callers can construct
`store.GetUserParams` and receive `store.User` without importing the sqlc
package:

```go
user, err := queries.Users().GetUser(ctx, &store.GetUserParams{
    TenantID: tenantID,
    ID:       userID,
})
```

Aliases preserve the original type identity; they do not copy or convert
values. The generator also aliases package-local enum or override types used by
the exported params, rows, and models. Generation fails with an actionable
error if an alias would collide with a generated pgmesh declaration.

The checked-in
[`tests/generate/separate_package/sqlc.yaml`](../../tests/generate/separate_package/sqlc.yaml)
builds the separate-package configuration and exercises these aliases.

## Migration from legacy options

This release removes legacy compatibility fields without a deprecation period.

| Removed field | Migration |
| --- | --- |
| `source_package` | Remove it; use `package` and, for separate output, `internal_import_path`. |
| `interface` | Remove it; the sqlc interface is fixed to `Querier`. |
| `type` | Remove it; the private combined wrapper is fixed to `queryStore`. |
| `target_type` | Remove it; the sqlc query type is fixed to `Queries`. |
| `target_constructor` | Remove it; the sqlc constructor is fixed to `New`. |
| `receiver` | Remove it; private receivers are fixed to `q`. |
| `skip_with_tx` | Remove it; pgx/v5 transaction support is always generated. |
| `read_interface` | Remove it; fixed to private `readQuerier`. |
| `write_interface` | Remove it; fixed to private `writeQuerier`. |
| `read_type` | Remove it; fixed to private `readQueries`. |
| `write_type` | Remove it; fixed to private `writeQueries`. |
| `read_constructor` | Remove it; fixed to private `newReadQueries`. |
| `write_constructor` | Remove it; fixed to private `newWriteQueries`. |
| `node_constructor` | Remove it; fixed to private `newStoreNode`. |
| `sharded_type` | Remove it; fixed to private `meshStore`. |
| override `postgres_type` | Replace with `db_type`. |
| override `null` | Replace with `nullable`. |

Because decoding is strict, leaving any removed field in `sqlc.yaml` produces
an actionable unknown-field error.

## Regenerate deterministically

This repository pins sqlc 1.31.1:

```bash
just generate-tests
just --justfile examples/justfile generate
git diff --exit-code
```

Downstream:

```bash
sqlc generate
go test ./...
git diff --exit-code
```

Pin sqlc and the pgmesh plugin version in CI. Regenerate after changing SQL,
schema, annotations, output layout, names, pointer settings, renames, or
overrides.
