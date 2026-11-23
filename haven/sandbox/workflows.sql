-- name: InsertActivation :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,phase) VALUES(?,?,'activate',?,?,?,?,'pending','admitted');

-- name: TransitionActivation :execrows
UPDATE operation_steps SET phase=sqlc.arg(next_phase),status=sqlc.arg(next_status) WHERE operation_id=sqlc.arg(operation_id) AND step_key=sqlc.arg(step_key) AND action='activate'
AND input_hash=sqlc.arg(input_hash) AND phase=sqlc.arg(previous_phase) AND status=sqlc.arg(previous_status) AND status IN ('pending','running');

-- name: InsertWarmClaimStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,candidate_resource_id,expected_instance,status,phase) VALUES(?,'warm_claim','claim',?,?,?,'pending','selected');

-- name: InsertWarmClaim :execrows
INSERT INTO warm_claims(operation_id,refill_operation_id,source_operation_id) VALUES(?,?,?);

-- name: GetWarmClaimInputs :one
SELECT w.refill_operation_id,w.source_operation_id,s.phase,a.input AS activation,a.input_hash AS activation_hash,o.scope_id
FROM warm_claims w JOIN operation_steps s ON s.operation_id=w.operation_id AND s.step_key=w.step_key
JOIN operation_steps a ON a.operation_id=w.operation_id AND a.step_key='activation:warm' AND a.action='activate'
JOIN operations o ON o.id=w.operation_id AND o.scope_kind='tenant' WHERE w.operation_id=?;

-- name: TransitionWarmClaim :execrows
UPDATE operation_steps SET phase=sqlc.arg(next_phase),status=sqlc.arg(next_status)
WHERE operation_id=sqlc.arg(operation_id) AND step_key='warm_claim' AND input_hash=sqlc.arg(input_hash) AND phase=sqlc.arg(previous_phase) AND status IN ('pending','running');

-- name: InsertForkProgress :execrows
INSERT INTO fork_progress(operation_id) VALUES(?);

-- name: ReadForkProgress :one
SELECT capture_operation_id,publication_hash,child_count FROM fork_progress WHERE operation_id=?;

-- name: SaveForkProgress :execrows
UPDATE fork_progress SET capture_operation_id=?,publication_hash=?,child_count=? WHERE operation_id=?;

-- name: SetForkError :execrows
UPDATE operation_steps SET step_error=? WHERE operation_id=? AND step_key='fork_group';

-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,phase,result_snapshot_id,step_error
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: ListSuccessfulPauses :many
SELECT operation_id FROM operation_steps WHERE step_key='lifecycle' AND action='pause' AND candidate_resource_id=? AND status='succeeded' ORDER BY rowid DESC;


-- name: CountFailedLifecycleChildren :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND status='failed' AND (action='resource_link' OR action='activate');


-- name: CountPendingCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: CompletePendingCleanupIfPresent :exec
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: CountForkBranchLinks :one
SELECT COUNT(*) FROM operation_steps WHERE operation_id=? AND step_key LIKE 'link:branch:%';


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'pending',?,?);


-- name: GetLatestActivationStep :one
SELECT step_key,input_hash,input,status,phase,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND action='activate' ORDER BY rowid DESC LIMIT 1;

