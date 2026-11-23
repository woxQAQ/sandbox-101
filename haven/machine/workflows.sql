-- name: GetResourceLink :one
SELECT s.status,s.input_hash,s.candidate_resource_id,l.child_operation_id,l.child_kind
FROM operation_steps s
LEFT JOIN resource_links l ON l.parent_operation_id=s.operation_id AND l.step_key=s.step_key
WHERE s.operation_id=sqlc.arg(parent_operation_id) AND s.step_key=sqlc.arg(step_key) AND s.action='resource_link';

-- name: InsertResourceLink :execrows
INSERT INTO resource_links(parent_operation_id,step_key,child_operation_id,child_kind) VALUES(?,?,?,?);

-- name: InsertResourceStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,phase) VALUES(?,'resource',?,?,?,?,?,'pending','admitted');

-- name: InsertResourceProgress :execrows
INSERT INTO resource_progress(operation_id,restore_snapshot_id,backing_holder) VALUES(?,?,?);

-- name: ReadRestoreMaterials :one
SELECT * FROM restore_materials WHERE deleted_at IS NULL AND operation_id=?;

-- name: ClearRestoreFiles :exec
UPDATE restore_files SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND operation_id=?;

-- name: ClearRestoreMaterials :execrows
UPDATE restore_materials SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND operation_id=?;

-- name: InsertRestoreMaterials :execrows
INSERT INTO restore_materials(operation_id,complete,files_present,version_device,version_inode,version_mode,restore_device,restore_inode,restore_mode) VALUES(?,?,?,?,?,?,?,?,?);

-- name: ReadRestoreFiles :many
SELECT * FROM restore_files WHERE deleted_at IS NULL AND operation_id=?;

-- name: InsertRestoreFile :execrows
INSERT INTO restore_files(operation_id,name,source,candidate,size,device,inode,mode) VALUES(?,?,?,?,?,?,?,?);

-- name: GetResourceProgress :one
SELECT * FROM resource_progress WHERE operation_id=?;

-- name: SetResourceProgress :execrows
UPDATE resource_progress SET backing_ready=?,restore_blocked=?,disk_device=?,disk_inode=?,disk_mode=?,materials_hash=?,observation_present=?,alive=?,paused=?,booted=?,configured=?,pid=?,start_time=?,network=?,result_instance=?,error=? WHERE operation_id=?;

-- name: TransitionResource :execrows
UPDATE operation_steps SET phase=sqlc.arg(next_phase),status=sqlc.arg(next_status) WHERE operation_id=sqlc.arg(operation_id) AND step_key='resource' AND phase=sqlc.arg(previous_phase) AND status=sqlc.arg(previous_status) AND input_hash=sqlc.arg(input_hash) AND status IN ('pending','running') AND (sqlc.arg(next_status)<>'succeeded' OR status='running');

-- name: InsertCaptureProgress :execrows
INSERT INTO capture_progress(operation_id) VALUES(?);

-- name: GetCaptureBoundary :one
SELECT * FROM capture_boundaries WHERE operation_id=?;

-- name: InsertCaptureBoundary :execrows
INSERT INTO capture_boundaries(operation_id,path,parent_device,parent_inode,parent_mode,publication_device,publication_inode,publication_mode) VALUES(?,?,?,?,?,?,?,?);

-- name: ReadCaptureProgress :one
SELECT resume_required,captured,materialized,artifacts_synced FROM capture_progress WHERE operation_id=?;

-- name: UpdateCaptureProgress :execrows
UPDATE capture_progress SET resume_required=?,captured=?,materialized=?,artifacts_synced=? WHERE operation_id=?;

-- name: ListCaptureFiles :many
SELECT path,digest FROM capture_files WHERE operation_id=? ORDER BY path;

-- name: InsertCaptureFile :execrows
INSERT INTO capture_files(operation_id,path,digest) VALUES(?,?,?);

-- name: UpdateCaptureError :execrows
UPDATE operation_steps SET step_error=? WHERE operation_id=? AND step_key='capture';

-- name: TransitionDirectRetirement :execrows
UPDATE operation_steps SET status=sqlc.arg(next_status),phase=sqlc.arg(next_phase),retired_by=sqlc.narg(retired_by) WHERE operation_id=sqlc.arg(operation_id) AND step_key='direct_retirement' AND input_hash=sqlc.arg(input_hash) AND status=sqlc.arg(previous_status) AND phase=sqlc.arg(previous_phase) AND retired_by IS NULL AND status IN ('pending','running') AND (sqlc.arg(next_phase)<>'pending' OR status='pending') AND (sqlc.arg(next_phase)<>'replaced' OR status='pending') AND (sqlc.arg(next_phase)<>'complete' OR status='running');

-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,phase,step_error,retired_by
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: GetPendingCleanupBody :one
SELECT witness_hash FROM cleanup_intents WHERE id=? AND operation_id=? AND status='pending';


-- name: CountUnfinishedCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status!='done';


-- name: TransferPendingCleanup :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='pending';


-- name: TransferBlockedCleanupResource :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='blocked';


-- name: InsertArmedCleanup :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'armed',?,?);


-- name: TransitionDirectCleanup :execrows
UPDATE cleanup_intents SET status=sqlc.arg('next_status') WHERE id=sqlc.arg('id') AND operation_id=sqlc.arg('operation_id') AND resource_kind='direct_policy' AND status=sqlc.arg('previous_status');


-- name: CompleteLiveDirectCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status IN ('armed','pending');


-- name: ListLiveDirectRetirements :many
SELECT operation_id FROM operation_steps WHERE step_key='direct_retirement' AND candidate_resource_id=? AND status IN ('pending','running');


-- name: InsertBlockedCleanup :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'blocked',?,?);


-- name: TransferPendingCleanupResource :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='pending';


-- name: PendingCleanupResourceOwner :one
SELECT operation_id FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: CleanupResourceCount :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status=?;


-- name: CleanupBody :one
SELECT witness_hash FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status=?;


-- name: MatchingCleanupCount :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status=? AND witness_hash=?;


-- name: CountPendingCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';








-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: CountCompletedCleanup :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND status='done' AND witness_hash=?;





-- name: CompleteRetainedCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='retained';


-- name: TransferBlockedCleanup :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='blocked';


-- name: CompleteTransferredCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred';


-- name: CompletePendingCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending';


-- name: RetireDeletionReceipt :execrows
UPDATE deletion_intents SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND path=? AND operation_id=? AND step_key=?;


-- name: RetainCleanup :execrows
UPDATE cleanup_intents SET status='retained' WHERE id=? AND operation_id=? AND status='pending';


-- name: InsertLinkStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,candidate_resource_id,expected_instance,status) VALUES(?,?,'resource_link',?,?,?,'pending');


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'pending',?,?);


-- name: GetRetainedCleanup :one
SELECT operation_id,witness_hash FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='retained';


-- name: RestoreBackingOperation :one
SELECT r.operation_id FROM resource_progress r JOIN operation_steps s ON s.operation_id=r.operation_id AND s.step_key=r.step_key
WHERE s.action IN ('clone','restore') AND r.backing_holder=? AND r.restore_snapshot_id=?;

