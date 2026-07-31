# Grouped and scatter queries across shards

This example contrasts two generated multi-shard read APIs:

- `ListAccountsByIDs` accepts route metadata per lookup value, groups values by
  physical shard, runs one query per populated shard, and restores requested
  result order.
- `ListAllAccounts` uses the explicit `shard: all()` route and runs once on
  every physical shard, concatenating results in topology order.

Both reads use `ReadFromPrimary()` because the program reads immediately after
seeding data. In a replicated deployment, omit that option when normal replica
consistency is acceptable.

Apply [`examples/sqlc/schema.sql`](../sqlc/schema.sql) to both databases, set
`MULTI_SHARD0_DSN` and `MULTI_SHARD1_DSN`, and run:

```bash
go run ./examples/07-multi-shard-queries
```

See [Use multi-shard queries](../../docs/how-to/use-multi-shard-queries.md) for
dispatch, ordering, transaction, and partial-failure behavior.
