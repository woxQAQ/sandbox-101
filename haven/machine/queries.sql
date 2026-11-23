-- name: GetNetworkPolicyOwner :one
SELECT instance,operation_id FROM network_policies WHERE vm_id=?;

-- name: GetNetworkPolicy :one
SELECT operation_id,instance,policy_hash FROM network_policies WHERE vm_id=?;

-- name: UpsertNetworkPolicy :execrows
INSERT INTO network_policies(vm_id,instance,operation_id,policy_hash,retirement_step) VALUES(sqlc.arg(vm_id),sqlc.arg(instance),sqlc.arg(operation_id),sqlc.narg(policy_hash),CASE WHEN sqlc.narg(policy_hash) IS NOT NULL THEN 'direct_retirement' END)
ON CONFLICT(vm_id) DO UPDATE SET instance=excluded.instance,operation_id=excluded.operation_id,policy_hash=excluded.policy_hash,retirement_step=excluded.retirement_step;

-- name: DeleteNetworkPolicy :execrows
DELETE FROM network_policies WHERE vm_id=? AND instance=? AND operation_id=?;

-- name: DeleteMatchingNetworkPolicy :execrows
DELETE FROM network_policies WHERE vm_id=sqlc.arg(vm_id) AND instance=sqlc.arg(instance) AND operation_id=sqlc.arg(operation_id) AND policy_hash IS sqlc.narg(policy_hash);

-- name: CreateCandidate :execrows
INSERT INTO vms(id,slot,current_instance,state,operation_id,generation_operation_id,directory,resource_hash) VALUES(?,?,'','creating',?,?,?,?);

-- name: ListSlots :many
SELECT slot FROM vms ORDER BY slot;

-- name: CountCurrent :one
SELECT COUNT(*) FROM vms WHERE id=?;

-- name: ListCurrentBodies :many
SELECT id,directory FROM vms ORDER BY id;

-- name: RecordCreatedInstance :execrows
UPDATE vms SET state='created',current_instance=? WHERE id=? AND operation_id=?;

-- name: RecordStopped :execrows
UPDATE vms SET state='stopped',verified=1,agent_ready=0,observation_hash=NULL WHERE id=? AND current_instance=? AND operation_id=?;

-- name: CompleteStop :execrows
UPDATE vms SET state='stopped',operation_id=NULL,verified=1,agent_ready=0,observation_hash=NULL WHERE id=? AND operation_id=? AND current_instance=?;

-- name: CompleteStart :execrows
UPDATE vms SET state='running',operation_id=NULL,verified=1,agent_ready=1,observation_hash=? WHERE id=? AND current_instance=? AND operation_id=? AND state='created';

-- name: ClaimForDeletion :execrows
UPDATE vms SET operation_id=sqlc.arg('owner'),deleting=1
WHERE id=sqlc.arg('id') AND current_instance=sqlc.arg('instance') AND operation_id IS NULL;

-- name: ClaimRestoreGeneration :execrows
UPDATE vms SET operation_id=sqlc.arg(owner),generation_operation_id=sqlc.arg(generation_operation_id),directory=sqlc.arg(directory),resource_hash=sqlc.arg(resource_hash),verified=0,agent_ready=0,deleting=0,observation_hash=NULL
WHERE id=sqlc.arg('id') AND operation_id IS NULL AND current_instance=sqlc.arg('instance') AND state='stopped';

-- name: TransferOwnership :execrows
UPDATE vms SET operation_id=sqlc.arg('next_owner'),deleting=1
WHERE id=sqlc.arg('id') AND operation_id=sqlc.arg('previous_owner') AND current_instance=sqlc.arg('instance');

-- name: DeleteStopped :execrows
DELETE FROM vms WHERE id=? AND operation_id=? AND current_instance=? AND state='stopped';

-- name: InsertInstance :execrows
INSERT INTO instances(id,vm_id,operation_id,resource_hash) VALUES(?,?,?,?);

-- name: InsertVersion :execrows
INSERT INTO resource_versions(vm_id,operation_id,step_key,instance,resource_hash) VALUES(?,?,'resource',?,?);

-- name: PublishInstanceObservation :execrows
UPDATE instances SET observation_hash=? WHERE id=? AND vm_id=? AND operation_id=? AND resource_hash=? AND observation_hash IS NULL;

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

-- name: RecordVersionDisk :execrows
UPDATE resource_versions SET disk_device=?,disk_inode=?,disk_mode=?
WHERE vm_id=? AND operation_id=? AND step_key='resource' AND instance=? AND resource_hash=? AND disk_device IS NULL;

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
SELECT sqlc.embed(v),s.input,s.input_hash FROM vms v LEFT JOIN operation_steps s ON s.operation_id=v.generation_operation_id AND s.step_key='resource' WHERE v.id=?;

-- name: GetInstance :one
SELECT resource_hash,observation_hash FROM instances WHERE id = ? AND vm_id = ? AND operation_id = ?;

-- name: GetVersion :one
SELECT instance,resource_hash,disk_device,disk_inode,disk_mode FROM resource_versions WHERE vm_id = ? AND operation_id = ? AND step_key = ?;

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
SELECT record_hash FROM snapshot_publications WHERE snapshot_id=? AND tenant_id=? AND capture_operation_id=?;

-- name: CountSnapshotPublication :one
SELECT COUNT(*) FROM snapshot_publications WHERE snapshot_id=?;

-- name: InsertSnapshot :execrows
INSERT INTO snapshots(id,source_vm_id,tenant_id,capture_operation_id,source_instance,directory,published,deleting) VALUES(?,?,?,?,?,?,0,0);

-- name: InsertCaptureOwner :execrows
INSERT INTO snapshot_owners(snapshot_id,tenant_id,source_operation_id,source_kind) VALUES(?,?,?,'capture');

-- name: PublishSnapshot :execrows
UPDATE snapshots SET published=1,manifest_hash=? WHERE id=? AND source_vm_id=? AND capture_operation_id=? AND tenant_id=? AND source_instance=? AND directory=? AND published=0 AND deleting=0;

-- name: MarkFailedDeleting :execrows
UPDATE snapshots SET deleting=1 WHERE id=? AND published=0 AND deleting=0;

-- name: MarkPublishedDeleting :execrows
UPDATE snapshots SET deleting=1 WHERE id=? AND published=1 AND deleting=0 AND capture_operation_id=? AND tenant_id=? AND source_vm_id=? AND source_instance=? AND directory=? AND manifest_hash=?;

-- name: DeleteSnapshot :execrows
DELETE FROM snapshots WHERE id=sqlc.arg(id) AND published=sqlc.arg(published) AND deleting=1 AND capture_operation_id=sqlc.arg(capture_operation_id) AND tenant_id=sqlc.arg(tenant_id) AND source_vm_id=sqlc.arg(source_vm_id) AND source_instance=sqlc.arg(source_instance) AND directory=sqlc.arg(directory) AND manifest_hash IS sqlc.narg(manifest_hash);

-- name: InsertSnapshotPublication :execrows
INSERT INTO snapshot_publications(snapshot_id,tenant_id,capture_operation_id,record_hash) VALUES(?,?,?,?);

-- name: DeleteSnapshotPublication :execrows
DELETE FROM snapshot_publications WHERE snapshot_id=? AND tenant_id=? AND capture_operation_id=? AND record_hash=?;

-- name: GetOwnedSnapshot :one
SELECT s.* FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?;

-- name: GetOwnedPublication :one
SELECT sqlc.embed(s),o.source_operation_id,o.source_kind
FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?;

-- name: GetCapture :one
SELECT * FROM snapshots WHERE id=? AND source_vm_id=?;

-- name: ListSnapshots :many
SELECT * FROM snapshots ORDER BY id;

-- name: GetPublishedBody :one
SELECT * FROM snapshots WHERE id=? AND published=1 AND deleting=? AND source_vm_id=?;

-- name: CountSnapshot :one
SELECT COUNT(*) FROM snapshots WHERE id=?;

-- name: CountCaptureOwner :one
SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture';

-- name: CountSnapshotLeases :one
SELECT COUNT(*) FROM snapshot_source_leases WHERE snapshot_id=?;
