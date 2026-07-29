# How-to guides

These guides assume sqlc and the pgmesh process plugin are already generating a
working package. If not, begin with the [quickstart](../quickstart.md).

## Change generated queries

- [Add a query](add-a-query.md)
- [Configure and regenerate code](configure-generation.md)

## Change routing topology

- [Add sharding](add-sharding.md)
- [Add read replicas](add-read-replicas.md)
- [Expand shards with synchronous dual writes](add-write-mirrors.md)
- [Use transactions](use-transactions.md)

## Add application behavior

- [Add cache-aside behavior](add-cache-aside.md)

## Observe routed queries

- [Enable OpenTelemetry tracing and metrics](enable-opentelemetry.md)
- [Enable structured debug logging](enable-logging.md)

## Diagnose problems

- [Troubleshoot generation and routing](troubleshoot.md)

For the reasons behind these boundaries, read
[Purpose and rationale](../purpose-and-rationale.md).
