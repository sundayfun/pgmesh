-- name: GetUser :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Users
SELECT id, tenant_id, name
FROM users
WHERE tenant_id = $1 AND id = $2;

-- name: CreateUser :one
-- kind: write
-- shard: tenant(tenant_id)
-- store: Users
INSERT INTO users (id, tenant_id, name)
VALUES ($1, $2, $3)
RETURNING id, tenant_id, name;

-- name: UpdateUserName :one
-- kind: write
-- shard: tenant(tenant_id)
-- store: Users
UPDATE users
SET name = $3
WHERE tenant_id = $1 AND id = $2
RETURNING id, tenant_id, name;

-- name: ListAllUsers :many
-- kind: read
-- shard: all()
-- store: Users
SELECT id, tenant_id, name
FROM users
ORDER BY id;

-- name: ListUsersByIDs :many
-- kind: read
-- shard: tenant(tenant_id)
-- store: Users
SELECT id, tenant_id, name
FROM users
WHERE id = ANY(@ids::bigint[]);

-- name: DeleteAllUsers :exec
-- kind: write
-- shard: all()
-- store: Users
DELETE FROM users;

-- name: DeleteAllUsersByName :execrows
-- kind: write
-- shard: all()
-- store: Users
DELETE FROM users
WHERE name = $1;

-- name: CopyUsers :copyfrom
-- kind: write
-- shard: tenant(tenant_id)
-- store: Users
INSERT INTO users (id, tenant_id, name)
VALUES ($1, $2, $3);

-- name: GetAnalysis :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Analyses
SELECT id, tenant_id, summary, state, source, active_window
FROM analyses
WHERE tenant_id = $1 AND id = $2;

-- name: GetTenantUserAnalysis :one
-- kind: read
-- shard: tenant(tenant_id)
-- store: Analyses
SELECT users.id AS user_id, analyses.id AS analysis_id
FROM users
JOIN analyses ON analyses.tenant_id = users.tenant_id
WHERE users.id = @user_id
  AND analyses.id = @analysis_id;

-- name: ListP2PMessagesByChat :many
-- kind: read
-- shard: messageKey(user_id, to_user_or_group_id, in_group)
-- store: QueryMessage
SELECT * FROM "message"
WHERE in_group = FALSE
  AND LEAST(user_id, to_user_or_group_id) = LEAST(@user_id::bigint, @peer_id::bigint)
  AND GREATEST(user_id, to_user_or_group_id) = GREATEST(@user_id::bigint, @peer_id::bigint)
  AND created_at >= @created_since::timestamptz
  AND deleted_at IS NULL
  AND (@last_id::public.xid IS NULL OR id < @last_id::public.xid)
ORDER BY id DESC
LIMIT $1;

-- name: ListP2PMessageIDsByChat :many
-- kind: read
-- shard: messageKey(user_id, to_user_or_group_id, in_group)
-- store: QueryMessage
SELECT id FROM "message"
WHERE in_group = FALSE
  AND LEAST(user_id, to_user_or_group_id) = LEAST(@user_id::bigint, @peer_id::bigint)
  AND GREATEST(user_id, to_user_or_group_id) = GREATEST(@user_id::bigint, @peer_id::bigint)
  AND created_at >= @created_since::timestamptz
  AND deleted_at IS NULL
  AND (@last_id::public.xid IS NULL OR id < @last_id::public.xid)
ORDER BY id DESC
LIMIT $1;
