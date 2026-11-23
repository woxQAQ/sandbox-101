-- name: GetIntent :one
SELECT tenant_id,candidate_operation_id FROM template_owners WHERE template_id=?;

-- name: CountOwnedIntent :one
SELECT COUNT(*) FROM template_owners WHERE template_id=? AND tenant_id=?;

-- name: ReservePublication :execrows
INSERT INTO template_owners(template_id,tenant_id,candidate_operation_id) VALUES(?,?,?) ON CONFLICT(template_id) DO UPDATE SET candidate_operation_id=excluded.candidate_operation_id WHERE tenant_id=excluded.tenant_id;

-- name: CountSnapshotUsers :one
SELECT COUNT(*) FROM templates t JOIN template_versions v ON v.operation_id=t.published_operation_id WHERE v.snapshot_id=?;

-- name: GetTemplate :one
SELECT published_operation_id FROM templates WHERE id=?;

-- name: GetTemplateVersion :one
SELECT v.*,o.tenant_id FROM template_versions v JOIN template_owners o ON o.template_id=v.template_id WHERE v.operation_id=?;

-- name: GetTemplateLabels :many
SELECT name,value FROM template_version_labels WHERE version_id=?;

-- name: InsertTemplateVersion :execrows
INSERT INTO template_versions(operation_id,template_id,image,vcpus,memory_mib,disk_mib,warmup,snapshot_id,target_warm) VALUES(?,?,?,?,?,?,?,?,?);

-- name: InsertTemplateLabel :execrows
INSERT INTO template_version_labels(version_id,name,value) VALUES(?,?,?);

-- name: GetPublisher :one
SELECT i.tenant_id,o.scope_kind,o.scope_id,o.status
FROM template_owners i JOIN operations o ON o.id=sqlc.arg('published_operation_id') WHERE i.template_id=sqlc.arg('template_id');

-- name: ListTemplateIDs :many
SELECT t.id,CAST(coalesce(o.tenant_id,'') AS TEXT) AS tenant FROM templates t LEFT JOIN template_owners o ON o.template_id=t.id ORDER BY t.id;

-- name: InsertTemplate :execrows
INSERT INTO templates(id,published_operation_id) VALUES(?,?);

-- name: ReplaceTemplate :execrows
UPDATE templates SET published_operation_id=sqlc.arg(published_operation_id) WHERE id=sqlc.arg(id) AND published_operation_id=sqlc.arg(previous_operation_id);

-- name: GetReservation :one
SELECT * FROM pool_slots WHERE deleted_at IS NULL AND operation_id=?;

-- name: ListReady :many
SELECT operation_id FROM pool_slots WHERE deleted_at IS NULL AND status='ready' ORDER BY operation_id;

-- name: CountTemplateReservations :one
SELECT COUNT(*) FROM pool_slots WHERE deleted_at IS NULL AND template_id=?;

-- name: CountReservation :one
SELECT COUNT(*) FROM pool_slots WHERE deleted_at IS NULL AND operation_id=?;

-- name: GetReservationForVM :one
SELECT operation_id FROM pool_slots WHERE deleted_at IS NULL AND vm_id=?;

-- name: FirstReady :one
SELECT p.operation_id FROM pool_slots p WHERE p.deleted_at IS NULL AND p.template_id=? AND p.template_version=? AND p.status='ready'
ORDER BY p.created_at_unix_ns,p.vm_id LIMIT 1;

-- name: InsertReservation :execrows
INSERT INTO pool_slots(operation_id,template_id,template_version,vm_id,sandbox_id,source_operation_id,source_input_hash,created_at_unix_ns,status) VALUES(?,?,?,?,?,?,?,?,'pending');

-- name: ListWarmVMs :many
SELECT vm_id FROM pool_slots WHERE deleted_at IS NULL AND template_id=? AND status='ready' ORDER BY vm_id;

-- name: CountWarmVM :one
SELECT COUNT(*) FROM pool_slots WHERE deleted_at IS NULL AND vm_id=? AND status='ready';

-- name: CountWarmSnapshot :one
SELECT COUNT(*) FROM pool_slots p JOIN template_versions v ON v.operation_id=p.template_version WHERE p.deleted_at IS NULL AND p.status='ready' AND v.snapshot_id=?;

-- name: CountWarmIdentity :one
SELECT COUNT(*) FROM pool_slots p JOIN template_versions v ON v.operation_id=p.template_version WHERE p.deleted_at IS NULL AND p.status='ready' AND p.vm_id=? AND p.template_id=? AND v.snapshot_id=?;

-- name: MarkReady :execrows
UPDATE pool_slots SET status='ready',instance=? WHERE deleted_at IS NULL AND operation_id=? AND status='pending' AND instance='' AND source_input_hash=?;

-- name: SelectReservation :execrows
UPDATE pool_slots SET status='selected' WHERE deleted_at IS NULL AND operation_id=? AND status='ready' AND vm_id=? AND instance=? AND source_input_hash=? AND template_version=?;

-- name: MarkPendingCleanup :execrows
UPDATE pool_slots SET status='cleanup' WHERE deleted_at IS NULL AND operation_id=? AND status='pending';

-- name: MarkReadyCleanup :execrows
UPDATE pool_slots SET status='cleanup' WHERE deleted_at IS NULL AND operation_id=? AND status='ready' AND instance=? AND source_input_hash=?;

-- name: DeleteReservation :execrows
UPDATE pool_slots SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND operation_id=? AND status=? AND instance=? AND source_input_hash=?;
