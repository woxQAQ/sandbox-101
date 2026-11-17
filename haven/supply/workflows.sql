-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: CountLiveResourceLinks :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND action='resource_link' AND status IN ('pending','running');


-- name: FailLiveStep :execrows
UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: AdvanceLiveStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND status IN ('pending','running') AND body=sqlc.arg('previous_body');


-- name: CompleteUnchangedStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status IN ('pending','running');


-- name: InsertStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?);


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'pending',?);


-- name: ListRefillAttempts :many
SELECT s.input,s.body,s.status FROM operation_steps s WHERE s.step_key='supply' AND s.action='refill' AND s.candidate_resource_id=? AND s.status IN ('failed','succeeded') ORDER BY s.rowid DESC LIMIT 10;

