-- name: GetAccount :one
-- kind: read
-- shard: tenantKey(tenant_id)
-- store: Accounts
SELECT id, tenant_id, display_name
FROM accounts
WHERE tenant_id = $1 AND id = $2;

-- name: UpdateAccountName :one
-- kind: write
-- shard: tenantKey(tenant_id)
-- store: Accounts
UPDATE accounts
SET display_name = $3
WHERE tenant_id = $1 AND id = $2
RETURNING id, tenant_id, display_name;

-- name: UpsertAccount :one
-- kind: write
-- shard: tenantKey(tenant_id)
-- store: Accounts
INSERT INTO accounts (id, tenant_id, display_name)
VALUES ($1, $2, $3)
ON CONFLICT (id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
    display_name = EXCLUDED.display_name
RETURNING id, tenant_id, display_name;

-- name: CountAccounts :one
-- kind: read
-- shard: tenantKey(tenant_id)
-- store: Reports
SELECT COUNT(*)
FROM accounts
WHERE tenant_id = $1;
