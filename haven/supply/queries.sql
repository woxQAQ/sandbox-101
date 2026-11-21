-- name: GetIntent :one
SELECT tenant_id,operation_id FROM template_intents WHERE template_id=?;

-- name: GetPermanentOwner :one
SELECT tenant_id FROM template_intents WHERE template_id=?;

-- name: CountOwnedIntent :one
SELECT COUNT(*) FROM template_intents WHERE template_id=? AND tenant_id=?;

-- name: ReservePublication :execrows
INSERT INTO template_intents(template_id,tenant_id,operation_id) VALUES(?,?,?) ON CONFLICT(template_id) DO UPDATE SET operation_id=excluded.operation_id WHERE tenant_id=excluded.tenant_id;

-- name: CountSnapshotUsers :one
-- The cast fixes sqlc's BLOB inference; concatenation keeps the comparison affinity-free.
SELECT COUNT(*) FROM templates WHERE json_extract(body,'$.snapshot_id')=(CAST(? AS TEXT)||'');

-- name: GetTemplate :one
SELECT body FROM templates WHERE id=?;

-- name: GetOwnedTemplate :one
SELECT body FROM templates WHERE id=? AND tenant=?;

-- name: GetPublisher :one
SELECT i.tenant_id,o.scope_kind,o.scope_id,o.status,i.operation_id
FROM template_intents i JOIN operations o ON o.id=i.operation_id WHERE i.template_id=?;

-- name: ListTemplateIDs :many
SELECT id,tenant FROM templates ORDER BY id;

-- name: InsertTemplate :execrows
INSERT INTO templates(id,tenant,body) VALUES(?,?,?);

-- name: ReplaceTemplate :execrows
UPDATE templates SET body=sqlc.arg('new_body') WHERE id=sqlc.arg('id') AND tenant=sqlc.arg('tenant') AND body=sqlc.arg('previous_body');

-- name: GetReservation :one
SELECT template_id,template_version,vm_id,status,body FROM pool_slots WHERE operation_id=?;

-- name: ListReady :many
SELECT operation_id FROM pool_slots WHERE status='ready' ORDER BY operation_id;

-- name: CountTemplateReservations :one
SELECT COUNT(*) FROM pool_slots WHERE template_id=?;

-- name: CountReservation :one
SELECT COUNT(*) FROM pool_slots WHERE operation_id=?;

-- name: GetReservationForVM :one
SELECT operation_id FROM pool_slots WHERE vm_id=?;

-- name: FirstReady :one
SELECT p.operation_id FROM warm w JOIN pool_slots p ON p.vm_id=w.vm_id
WHERE w.template_id=? AND w.snapshot_id=? AND p.template_version=? AND p.status='ready'
ORDER BY w.created_at,w.vm_id LIMIT 1;

-- name: InsertReservation :execrows
INSERT INTO pool_slots(operation_id,template_id,template_version,vm_id,status,body) VALUES(?,?,?,?,'pending',?);

-- name: ListWarmVMs :many
SELECT vm_id FROM warm WHERE template_id=? ORDER BY vm_id;

-- name: CountWarmVM :one
SELECT COUNT(*) FROM warm WHERE vm_id=?;

-- name: CountWarmSnapshot :one
SELECT COUNT(*) FROM warm WHERE snapshot_id=?;

-- name: CountWarmIdentity :one
SELECT COUNT(*) FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?;

-- name: RemoveWarm :execrows
DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?;

-- name: ConsumeWarm :execrows
DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=? AND created_at=?;

-- name: InsertWarm :execrows
INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?);

-- name: MarkReady :execrows
UPDATE pool_slots SET status='ready',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND status='pending' AND body=sqlc.arg('previous_body');

-- name: SelectReservation :execrows
UPDATE pool_slots SET status='selected' WHERE operation_id=? AND status='ready' AND body=?;

-- name: MarkPendingCleanup :execrows
UPDATE pool_slots SET status='cleanup' WHERE operation_id=? AND status='pending';

-- name: MarkReadyCleanup :execrows
UPDATE pool_slots SET status='cleanup' WHERE operation_id=? AND status='ready' AND body=?;

-- name: DeleteReservation :execrows
DELETE FROM pool_slots WHERE operation_id=? AND status=? AND body=?;
