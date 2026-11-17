-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: RecordLiveStepProgress :execrows
UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: CountPendingOwnedCleanup :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: InsertStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?);


-- name: CompleteLiveStep :execrows
UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status IN ('pending','running') AND input_hash=?;


-- name: ListGCStepInputs :many
SELECT operation_id,input_hash,input,status,action FROM operation_steps ORDER BY rowid;


-- name: ListFailedSupplySteps :many
SELECT operation_id,status FROM operation_steps WHERE step_key='supply' AND status='failed' ORDER BY operation_id;


-- name: ListCaptureLinks :many
SELECT operation_id,input_hash,input,status FROM operation_steps WHERE action='resource_link' AND step_key='link:capture' AND json_extract(input,'$.child_id')=(CAST(sqlc.arg(child_id) AS TEXT)||'');


-- name: ListPendingCleanupOperations :many
SELECT operation_id FROM cleanup_intents WHERE resource_kind=? AND status='pending' ORDER BY operation_id;


-- name: ListSuccessfulStops :many
SELECT operation_id FROM operation_steps WHERE step_key='stop' AND status='succeeded' AND candidate_resource_id=? AND expected_instance=? ORDER BY rowid DESC;


-- name: GetCleanupKindStatus :one
SELECT status FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=?;

