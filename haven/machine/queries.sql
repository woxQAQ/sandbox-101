-- name: GetNetworkPolicyOwner :one
SELECT instance,operation_id FROM network_policies WHERE vm_id=?;

-- name: GetNetworkPolicy :one
SELECT operation_id,instance,body FROM network_policies WHERE vm_id=?;

-- name: UpsertNetworkPolicy :execrows
INSERT INTO network_policies(vm_id,instance,operation_id,body) VALUES(?,?,?,?) ON CONFLICT(vm_id) DO UPDATE SET instance=excluded.instance,operation_id=excluded.operation_id,body=excluded.body;

-- name: DeleteNetworkPolicy :execrows
DELETE FROM network_policies WHERE vm_id=? AND instance=? AND operation_id=?;

-- name: DeleteMatchingNetworkPolicy :execrows
DELETE FROM network_policies WHERE vm_id=? AND instance=? AND operation_id=? AND body=?;

-- name: CreateCandidate :execrows
INSERT INTO vms(id,slot,current_instance,state,operation_id,body) VALUES(?,?,'','creating',?,?);

-- name: ListSlots :many
SELECT slot FROM vms ORDER BY slot;

-- name: CountCurrent :one
SELECT COUNT(*) FROM vms WHERE id=?;

-- name: ListCurrentBodies :many
SELECT id,body FROM vms ORDER BY id;

-- name: RecordCreatedInstance :execrows
UPDATE vms SET state='created',current_instance=? WHERE id=? AND operation_id=?;

-- name: RecordStopped :execrows
UPDATE vms SET state='stopped',body=? WHERE id=? AND current_instance=? AND operation_id=?;

-- name: CompleteStop :execrows
UPDATE vms SET state='stopped',operation_id=NULL,body=? WHERE id=? AND operation_id=? AND current_instance=?;

-- name: CompleteStart :execrows
UPDATE vms SET state='running',operation_id=NULL,body=? WHERE id=? AND current_instance=? AND operation_id=? AND state='created';

-- name: ClaimCurrentBody :execrows
UPDATE vms SET operation_id=sqlc.arg('owner'),body=sqlc.arg('body')
WHERE id=sqlc.arg('id') AND current_instance=sqlc.arg('instance') AND operation_id IS NULL;

-- name: ClaimStoppedBody :execrows
UPDATE vms SET operation_id=sqlc.arg('owner'),body=sqlc.arg('body')
WHERE id=sqlc.arg('id') AND operation_id IS NULL AND current_instance=sqlc.arg('instance') AND state='stopped';

-- name: TransferOwnership :execrows
UPDATE vms SET operation_id=sqlc.arg('next_owner'),body=sqlc.arg('body')
WHERE id=sqlc.arg('id') AND operation_id=sqlc.arg('previous_owner') AND current_instance=sqlc.arg('instance');

-- name: DeleteStopped :execrows
DELETE FROM vms WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped';

-- name: InsertInstance :execrows
INSERT INTO instances(id,vm_id,operation_id,body) VALUES(?,?,?,?);

-- name: InsertVersion :execrows
INSERT INTO resource_versions(vm_id,operation_id,step_key,body) VALUES(?,?,?,?);

-- name: ReplaceInstanceBody :execrows
UPDATE instances SET body=? WHERE id=? AND vm_id=? AND operation_id=?;

-- name: ListVersionIDs :many
SELECT operation_id FROM resource_versions ORDER BY operation_id;

-- name: CountInstances :one
SELECT COUNT(*) FROM instances WHERE vm_id=?;

-- name: CountVersions :one
SELECT COUNT(*) FROM resource_versions WHERE vm_id=?;

-- name: DeleteVersion :execrows
DELETE FROM resource_versions WHERE vm_id=? AND operation_id=? AND step_key=?;

-- name: DeleteInstance :execrows
DELETE FROM instances WHERE id=? AND vm_id=? AND operation_id=?;

-- name: ReplaceVersionBody :execrows
UPDATE resource_versions SET body=sqlc.arg('new_body')
WHERE vm_id=sqlc.arg('vm_id') AND operation_id=sqlc.arg('operation_id') AND body=sqlc.arg('previous_body');

-- name: ClaimStopped :execrows
UPDATE vms SET operation_id=sqlc.arg('owner')
WHERE id=sqlc.arg('id') AND operation_id IS NULL AND state='stopped' AND current_instance=sqlc.arg('instance');

-- name: ReleaseStoppedOwnership :execrows
UPDATE vms SET operation_id=NULL WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped';

-- name: ClaimRunning :execrows
UPDATE vms SET operation_id=sqlc.arg('owner')
WHERE id=sqlc.arg('id') AND current_instance=sqlc.arg('instance') AND operation_id IS NULL AND state='running';

-- name: ReleaseOwnership :execrows
UPDATE vms SET operation_id=NULL WHERE id=? AND current_instance=? AND operation_id=?;

-- name: GetCurrent :one
SELECT current_instance, state, operation_id, body FROM vms WHERE id = ?;

-- name: GetInstance :one
SELECT body FROM instances WHERE id = ? AND vm_id = ? AND operation_id = ?;

-- name: GetVersion :one
SELECT body FROM resource_versions WHERE vm_id = ? AND operation_id = ? AND step_key = ?;

-- name: GetRetainedVersion :one
SELECT step_key, body FROM resource_versions WHERE vm_id = ? AND operation_id = ?;

-- name: GetRetainedInstance :one
SELECT operation_id, body FROM instances WHERE id = ? AND vm_id = ?;

-- name: InsertSourceLease :execrows
INSERT INTO snapshot_source_leases(snapshot_id,operation_id,step_key) VALUES(?,?,?);

-- name: DeleteSourceLease :execrows
DELETE FROM snapshot_source_leases WHERE snapshot_id=? AND operation_id=? AND step_key=?;

-- name: CountStepLease :one
SELECT COUNT(*) FROM snapshot_source_leases WHERE operation_id=? AND step_key=?;

-- name: CountSourceLease :one
SELECT COUNT(*) FROM snapshot_source_leases WHERE snapshot_id=? AND operation_id=? AND step_key=?;

-- name: ListSourceLeases :many
SELECT snapshot_id,operation_id,step_key FROM snapshot_source_leases ORDER BY operation_id;

-- name: InsertBacking :execrows
INSERT INTO vm_memory_backings(path,vm_id,file_digest) VALUES(?,?,?);

-- name: DeleteBacking :execrows
DELETE FROM vm_memory_backings WHERE path=? AND vm_id=? AND file_digest=?;

-- name: CountBacking :one
SELECT COUNT(*) FROM vm_memory_backings WHERE path=?;

-- name: CountVMBackings :one
SELECT COUNT(*) FROM vm_memory_backings WHERE vm_id=?;

-- name: CountMatchingBacking :one
SELECT COUNT(*) FROM vm_memory_backings WHERE path=? AND vm_id=? AND file_digest=?;

-- name: ListBackingPaths :many
SELECT path FROM vm_memory_backings ORDER BY path;

-- name: GetSnapshotPublication :one
SELECT body FROM snapshot_publications WHERE snapshot_id=? AND tenant_id=?;

-- name: CountSnapshotPublication :one
SELECT COUNT(*) FROM snapshot_publications WHERE snapshot_id=?;

-- name: InsertSnapshot :execrows
INSERT INTO snapshots(id,source_vm_id,published,deleting,body) VALUES(?,?,0,0,?);

-- name: InsertCaptureOwner :execrows
INSERT INTO snapshot_owners(snapshot_id,tenant_id,source_operation_id,source_kind) VALUES(?,?,?,'capture');

-- name: PublishSnapshot :execrows
UPDATE snapshots SET published=1,body=sqlc.arg('new_body')
WHERE id=sqlc.arg('id') AND source_vm_id=sqlc.arg('source_vm') AND published=0 AND deleting=0 AND body=sqlc.arg('previous_body');

-- name: MarkFailedDeleting :execrows
UPDATE snapshots SET deleting=1 WHERE id=? AND published=0 AND deleting=0;

-- name: MarkPublishedDeleting :execrows
UPDATE snapshots SET deleting=1 WHERE id=? AND published=1 AND deleting=0 AND body=?;

-- name: DeleteSnapshot :execrows
DELETE FROM snapshots WHERE id=? AND published=? AND deleting=1 AND body=?;

-- name: InsertSnapshotPublication :execrows
INSERT INTO snapshot_publications(snapshot_id,tenant_id,body) VALUES(?,?,?);

-- name: DeleteSnapshotPublication :execrows
DELETE FROM snapshot_publications WHERE snapshot_id=? AND tenant_id=? AND body=?;

-- name: GetOwnedBody :one
SELECT s.body FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?;

-- name: GetOwnedPublication :one
SELECT s.body,s.published,s.deleting,o.source_operation_id,o.source_kind
FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?;

-- name: GetCapture :one
SELECT published,deleting,body FROM snapshots WHERE id=? AND source_vm_id=?;

-- name: ListSnapshots :many
SELECT id,body,published,deleting FROM snapshots ORDER BY id;

-- name: GetPublishedBody :one
SELECT body FROM snapshots WHERE id=? AND published=1 AND deleting=? AND source_vm_id=?;

-- name: CountSnapshot :one
SELECT COUNT(*) FROM snapshots WHERE id=?;

-- name: CountCaptureOwner :one
SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture';

-- name: CountSnapshotLeases :one
SELECT COUNT(*) FROM snapshot_source_leases WHERE snapshot_id=?;
