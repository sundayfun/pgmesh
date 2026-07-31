-- name: GetCommandUser :one
-- kind: read
-- store: Commands
SELECT id, tenant_id, name
FROM users
WHERE id = $1;

-- name: ListCommandUsers :many
-- kind: read
-- store: Commands
SELECT id, tenant_id, name
FROM users
ORDER BY id;

-- name: DeleteCommandUser :exec
-- kind: write
-- store: Commands
DELETE FROM users
WHERE id = $1;

-- name: DeleteCommandUsersByTenant :execrows
-- kind: write
-- store: Commands
DELETE FROM users
WHERE tenant_id = $1;

-- name: TouchCommandUser :execresult
-- kind: write
-- store: Commands
UPDATE users
SET name = name
WHERE id = $1;

-- name: CopyCommandUsers :copyfrom
-- kind: write
-- store: Commands
INSERT INTO users (id, tenant_id, name)
VALUES ($1, $2, $3);

-- name: BatchInsertCommandUsers :batchexec
-- kind: write
-- store: Commands
INSERT INTO users (id, tenant_id, name)
VALUES ($1, $2, $3);

-- name: BatchGetCommandUser :batchone
-- kind: read
-- store: Commands
SELECT id, tenant_id, name
FROM users
WHERE id = $1;

-- name: BatchListCommandUsersByTenant :batchmany
-- kind: read
-- store: Commands
SELECT id, tenant_id, name
FROM users
WHERE tenant_id = $1
ORDER BY id;
