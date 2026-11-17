-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: ListSuccessfulPauses :many
SELECT operation_id FROM operation_steps WHERE step_key='lifecycle' AND action='pause' AND candidate_resource_id=? AND status='succeeded' ORDER BY rowid DESC;


-- name: CountFailedLifecycleChildren :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND status='failed' AND (action='resource_link' OR action='activate');


-- name: BeginPendingStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('next_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND status='pending' AND body=sqlc.arg('previous_body');


-- name: FailLiveStep :execrows
UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: CountPendingCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: CompletePendingCleanupIfPresent :exec
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: AdvanceLiveStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND status IN ('pending','running') AND body=sqlc.arg('previous_body');


-- name: CompleteRunningStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status='running';


-- name: CompleteUnchangedStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status IN ('pending','running');


-- name: InsertStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?);


-- name: CompleteLiveStep :execrows
UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status IN ('pending','running') AND input_hash=?;


-- name: CountForkBranchLinks :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND step_key LIKE 'link:branch:%';


-- name: FinishLiveStep :execrows
UPDATE operation_steps SET status=?,body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'pending',?);


-- name: GetLatestActivationStep :one
SELECT step_key,input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND action='activate' ORDER BY rowid DESC LIMIT 1;

