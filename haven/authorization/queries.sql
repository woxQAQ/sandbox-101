-- name: TenantForToken :one
SELECT id FROM tenants WHERE token_hash=?;

-- name: GetTenantLimits :one
SELECT body FROM tenants WHERE id=?;

-- name: PutTenant :execrows
INSERT INTO tenants (id,token_hash,body) VALUES(?,?,?)
ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,body=excluded.body;
