-- name: TenantForToken :one
SELECT id FROM tenants WHERE token_hash=?;

-- name: GetTenantLimits :one
SELECT max_sandboxes,max_storage_mib FROM tenants WHERE id=?;

-- name: PutTenant :execrows
INSERT INTO tenants (id,token_hash,max_sandboxes,max_storage_mib) VALUES(?,?,?,?)
ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,max_sandboxes=excluded.max_sandboxes,max_storage_mib=excluded.max_storage_mib;
