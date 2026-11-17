-- name: GetRequestState :one
SELECT request_hash,request,scope_kind,scope_id,status,body FROM operations WHERE id=?;

-- name: RunLiveOperationResult :execrows
UPDATE operations SET status='running',body=? WHERE id=? AND status IN ('pending','running');

-- name: GetRequestIdentity :one
SELECT status,scope_kind,scope_id,request_hash,request FROM operations WHERE id=?;

-- name: GetStepTarget :one
SELECT candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key=? AND input_hash=?;

-- name: GetLiveOperationBody :one
SELECT body FROM operations WHERE id=? AND status IN ('pending','running');

-- name: ReplaceLiveOperationBody :execrows
UPDATE operations SET body=? WHERE id=? AND status IN ('pending','running');

-- name: GetResultState :one
SELECT status,body FROM operations WHERE id=?;

-- name: GetResourceLinkStep :one
SELECT input,input_hash,status,candidate_resource_id FROM operation_steps WHERE operation_id=? AND step_key=? AND action='resource_link';

-- name: GetRequestHash :one
SELECT request_hash FROM operations WHERE id=?;

-- name: GetStepStatus :one
SELECT status FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: CompletePendingLink :execrows
UPDATE operation_steps SET status=?,body=? WHERE operation_id=? AND step_key=? AND status='pending';

-- name: RunLiveOperation :execrows
UPDATE operations SET status='running' WHERE id=? AND status IN ('pending','running');

-- name: FailResult :execrows
UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running');

-- name: CompleteResult :execrows
UPDATE operations SET status='succeeded',body=? WHERE id=? AND status IN ('pending','running');

-- name: InsertOperation :execrows
INSERT INTO operations(id,scope_kind,scope_id,idem_key,request_hash,request,status,body) VALUES(?,?,?,?,?,?,?,?);

-- name: InsertCredential :execrows
INSERT INTO operation_credentials(operation_id,request_mac) VALUES(?,?);

-- name: FinishLiveOperation :execrows
UPDATE operations SET status=?,body=? WHERE id=? AND status IN ('pending','running');

-- name: CompletePendingOperation :execrows
UPDATE operations SET status='succeeded' WHERE id=? AND status='pending';

-- name: CompleteLiveOperation :execrows
UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running');

-- name: GetScopedRequest :one
SELECT id,idem_key,status,request_hash,request,body FROM operations WHERE scope_kind=? AND scope_id=? AND idem_key=?;

-- name: GetRequestID :one
SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?;

-- name: GetTenantScope :one
SELECT scope_id FROM operations WHERE id=? AND scope_kind='tenant';

-- name: GetTenantCredentialMAC :one
SELECT c.request_mac FROM operations o JOIN operation_credentials c ON c.operation_id=o.id WHERE o.scope_kind='tenant' AND o.scope_id=? AND o.idem_key=?;

-- name: GetCredentialMAC :one
SELECT request_mac FROM operation_credentials WHERE operation_id=?;

-- name: GetScopedOperation :one
SELECT id, idem_key, status, body, request_hash, request
FROM operations WHERE id=? AND scope_kind=? AND scope_id=?;

-- name: ListUnfinishedOperations :many
SELECT DISTINCT operation_id FROM operation_steps
WHERE status IN ('pending','running') ORDER BY operation_id;

-- name: GetOperationTimes :one
SELECT created_at, finished_at FROM operation_metadata WHERE operation_id=?;

-- name: GetOperationProjection :one
SELECT id, idem_key, scope_kind, scope_id, request_hash, request, status, body
FROM operations WHERE id=?;

-- name: GetPublicStatus :one
SELECT o.status FROM operations o
JOIN operation_credentials c ON c.operation_id=o.id
WHERE o.id=? AND o.scope_kind='tenant';

-- name: GetPublication :one
SELECT body FROM operation_publications WHERE operation_id=?;

-- name: ListUnpublishedTerminals :many
SELECT o.id FROM operations o
LEFT JOIN operation_publications p ON p.operation_id=o.id
WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL
AND (EXISTS(SELECT 1 FROM operation_credentials c WHERE c.operation_id=o.id)
  OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate'))
ORDER BY o.rowid;

-- name: InsertPublication :execrows
INSERT INTO operation_publications (operation_id, body) VALUES (?,?);

-- name: ListStepsForRecovery :many
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps ORDER BY rowid;

-- name: ListPublicationProofs :many
SELECT p.body AS publication,o.id,o.idem_key,o.scope_kind,o.scope_id,o.request,o.status,o.body AS result,
       m.created_at,m.finished_at
FROM operation_publications p
JOIN operations o ON o.id=p.operation_id
JOIN operation_metadata m ON m.operation_id=o.id ORDER BY o.rowid;

-- name: CountMissingPublications :one
SELECT COUNT(*) FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id
WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL
AND (EXISTS(SELECT 1 FROM operation_credentials c WHERE c.operation_id=o.id)
  OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate'));
