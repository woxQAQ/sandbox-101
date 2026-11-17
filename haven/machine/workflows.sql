-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: GetPendingCleanupBody :one
SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND status='pending';


-- name: CountUnfinishedCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status!='done';


-- name: TransferPendingCleanup :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='pending';


-- name: TransferBlockedCleanupResource :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='blocked';


-- name: CompleteRunningStepByInput :execrows
UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status='running' AND input_hash=?;


-- name: CompletePendingStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('next_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status='pending';


-- name: InsertArmedCleanup :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'armed',?);


-- name: TransitionDirectCleanup :execrows
UPDATE cleanup_intents SET status=sqlc.arg('next_status') WHERE id=sqlc.arg('id') AND operation_id=sqlc.arg('operation_id') AND resource_kind='direct_policy' AND status=sqlc.arg('previous_status');


-- name: CompleteLiveDirectCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status IN ('armed','pending');


-- name: ListLiveDirectRetirements :many
SELECT operation_id FROM operation_steps WHERE step_key='direct_retirement' AND candidate_resource_id=? AND status IN ('pending','running');


-- name: InsertBlockedCleanup :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'blocked',?);


-- name: FailUnchangedLiveStep :execrows
UPDATE operation_steps SET status='failed',body=sqlc.arg('next_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status IN ('pending','running');


-- name: TransferPendingCleanupResource :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='pending';


-- name: PendingCleanupResourceOwner :one
SELECT operation_id FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: CleanupResourceCount :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status=?;


-- name: BeginPendingStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('next_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND status='pending' AND body=sqlc.arg('previous_body');


-- name: ReplaceRunningStepBody :execrows
UPDATE operation_steps SET body=sqlc.arg('next_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND status='running' AND body=sqlc.arg('previous_body');


-- name: CleanupBody :one
SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status=?;


-- name: MatchingCleanupCount :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status=? AND body=?;


-- name: FailLiveStep :execrows
UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: CountPendingCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: InsertDeletionReceipt :execrows
INSERT INTO deletion_intents(path,operation_id,step_key,body) VALUES(?,?,?,?);


-- name: DeleteMatchingDeletionReceipt :execrows
DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=? AND body=?;


-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: AdvanceRunningStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status='running';


-- name: AdvanceLiveStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND status IN ('pending','running') AND body=sqlc.arg('previous_body');


-- name: CountCompletedCleanup :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND status='done' AND body=?;


-- name: GetDeletionReceipt :one
SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?;


-- name: CompleteRetainedCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='retained';


-- name: TransferBlockedCleanup :execrows
UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND status='blocked';


-- name: CompleteTransferredCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred';


-- name: CompletePendingCleanup :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending';


-- name: RetireDeletionReceipt :execrows
DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?;


-- name: CompleteRunningStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status='running';


-- name: RetainCleanup :execrows
UPDATE cleanup_intents SET status='retained' WHERE id=? AND operation_id=? AND status='pending';


-- name: CompleteUnchangedStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status IN ('pending','running');


-- name: InsertStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?);


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'pending',?);


-- name: GetRetainedCleanup :one
SELECT operation_id,body FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='retained';


-- name: RestoreBackingOperation :one
SELECT operation_id FROM operation_steps WHERE step_key='resource' AND action IN ('clone','restore')
AND json_extract(input,'$.backing_holder')=(CAST(sqlc.arg('holder_id') AS TEXT)||'') AND json_extract(input,'$.restore.snapshot.id')=(CAST(sqlc.arg('snapshot_id') AS TEXT)||'');

