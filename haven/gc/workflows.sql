-- name: InsertGCProgress :execrows
INSERT INTO gc_progress(operation_id) VALUES(?);

-- name: GetGCProgress :one
SELECT next_candidate FROM gc_progress WHERE operation_id=?;

-- name: AdvanceGCProgress :execrows
UPDATE gc_progress SET next_candidate=sqlc.arg(next_candidate) WHERE operation_id=sqlc.arg(operation_id) AND next_candidate=sqlc.arg(previous_candidate);

-- name: SetGCStatus :execrows
UPDATE operation_steps SET status=sqlc.arg(next_status),phase=sqlc.arg(next_phase),completed_at=sqlc.arg(completed_at) WHERE operation_id=sqlc.arg(operation_id) AND step_key='gc' AND input_hash=sqlc.arg(input_hash) AND status=sqlc.arg(previous_status) AND status IN ('pending','running');

-- name: InsertGCOutcome :execrows
INSERT INTO gc_outcomes(operation_id,disposition,ordinal,kind,resource_id,tenant_id,cache_builder,cache_diff_id,proof,child_operation_id,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?);

-- name: ListGCOutcomes :many
SELECT disposition,ordinal,kind,resource_id,tenant_id,cache_builder,cache_diff_id,proof,child_operation_id,reason FROM gc_outcomes WHERE operation_id=? ORDER BY disposition,ordinal;

-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,completed_at
FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: CountPendingOwnedCleanup :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: ListGCStepInputs :many
SELECT operation_id,step_key,input_hash,input,status,action FROM operation_steps ORDER BY rowid;


-- name: ListFailedSupplySteps :many
SELECT operation_id,status FROM operation_steps WHERE step_key='supply' AND status='failed' ORDER BY operation_id;


-- name: ListCaptureLinks :many
SELECT parent_operation_id FROM resource_links WHERE child_operation_id=? AND step_key='link:capture';


-- name: ListPendingCleanupOperations :many
SELECT operation_id FROM cleanup_intents WHERE resource_kind=? AND status='pending' ORDER BY operation_id;


-- name: ListSuccessfulStops :many
SELECT operation_id FROM operation_steps WHERE step_key='stop' AND status='succeeded' AND candidate_resource_id=? AND expected_instance=? ORDER BY rowid DESC;


-- name: GetCleanupKindStatus :one
SELECT status FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=?;

