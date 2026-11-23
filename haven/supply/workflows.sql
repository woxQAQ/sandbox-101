-- name: InsertSupply :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,phase,completed_at,step_error) VALUES(?,'supply',?,?,?,?,'',?,?,?,?);

-- name: TransitionSupply :execrows
UPDATE operation_steps SET status=sqlc.arg(next_status),phase=sqlc.arg(next_phase),completed_at=sqlc.arg(completed_at),result_snapshot_id=sqlc.arg(result_snapshot_id),step_error=sqlc.arg(step_error)
WHERE operation_id=sqlc.arg(operation_id) AND step_key='supply' AND input_hash=sqlc.arg(input_hash) AND status=sqlc.arg(previous_status) AND status IN ('pending','running') AND phase=sqlc.arg(previous_phase) AND completed_at=sqlc.arg(previous_completion) AND result_snapshot_id=sqlc.arg(previous_snapshot) AND step_error=sqlc.arg(previous_error);

-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,phase,completed_at,result_snapshot_id,step_error
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: CountLiveResourceLinks :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND action='resource_link' AND status IN ('pending','running');


-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'pending',?,?);


-- name: ListRefillAttempts :many
SELECT s.input,s.completed_at,s.status FROM operation_steps s WHERE s.step_key='supply' AND s.action='refill' AND s.candidate_resource_id=? AND s.status IN ('failed','succeeded') ORDER BY s.rowid DESC LIMIT 10;

