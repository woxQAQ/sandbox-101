-- name: InsertPhaseStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,phase,result_snapshot_id) VALUES(?,?,?,?,?,?,?,?,?,?);

-- name: AdvancePhase :execrows
UPDATE operation_steps SET status=sqlc.arg(next_status),phase=sqlc.arg(next_phase),result_snapshot_id=sqlc.arg(next_snapshot) WHERE operation_id=sqlc.arg(operation_id) AND step_key=sqlc.arg(step_key) AND input_hash=sqlc.arg(input_hash) AND status=sqlc.arg(previous_status) AND phase=sqlc.arg(previous_phase) AND result_snapshot_id=sqlc.arg(previous_snapshot) AND status IN ('pending','running');

-- name: ListResultSandboxes :many
SELECT sandbox_id FROM operation_result_sandboxes WHERE deleted_at IS NULL AND operation_id=? ORDER BY position;

-- name: UpdateLiveResult :execrows
UPDATE operations SET status=COALESCE(sqlc.narg(next_status),status),finished_at=sqlc.arg(finished_at),
 result_sandbox_id=sqlc.arg(result_sandbox_id),result_vm_id=sqlc.arg(result_vm_id),result_image_digest=sqlc.arg(result_image_digest),
 result_template_id=sqlc.arg(result_template_id),result_checkpoint_id=sqlc.arg(result_checkpoint_id),result_snapshot_id=sqlc.arg(result_snapshot_id),
 resource_ready=sqlc.arg(resource_ready),result_error=sqlc.arg(result_error) WHERE id=sqlc.arg(id) AND status IN ('pending','running');

-- name: ClearResultSandboxes :exec
UPDATE operation_result_sandboxes SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND operation_id=?;

-- name: InsertResultSandbox :execrows
INSERT INTO operation_result_sandboxes(operation_id,position,sandbox_id) VALUES(?,?,?);

-- name: InsertRequestResource :execrows
INSERT INTO operation_resource_requests(operation_id,template_id,sandbox_id,snapshot_id,expected_instance,stop_operation_id,source_operation_id,cache_builder,cache_diff_id) VALUES(?,?,?,?,?,?,?,?,?);

-- name: GetRequestResource :one
SELECT * FROM operation_resource_requests WHERE operation_id=?;

-- name: InsertRequestImport :execrows
INSERT INTO operation_import_requests(operation_id,layout,digest,materialization,max_bytes,max_files) VALUES(?,?,?,?,?,?);

-- name: GetRequestImport :one
SELECT * FROM operation_import_requests WHERE operation_id=?;

-- name: InsertRequestGC :execrows
INSERT INTO operation_gc_requests(operation_id,dry_run,batch,budget_ns) VALUES(?,?,?,?);

-- name: GetRequestGC :one
SELECT * FROM operation_gc_requests WHERE operation_id=?;

-- name: InsertCommand :execrows
INSERT INTO operation_commands(operation_id,claim_action,observed_generation,sandbox_id,checkpoint_id,template_id,ttl_seconds,on_expire,auto_renew,lease_deadline,policy_mode,max_connections,branch_count,has_template,policy_rule_count,environment_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

-- name: GetCommand :one
SELECT * FROM operation_commands WHERE operation_id=?;

-- name: InsertCommandTemplate :execrows
INSERT INTO command_templates(operation_id,id,tenant_id,image,vcpus,memory_mib,disk_mib,snapshot_id,state,target_warm,warmup) VALUES(?,?,?,?,?,?,?,?,?,?,?);

-- name: GetCommandTemplate :one
SELECT * FROM command_templates WHERE operation_id=?;

-- name: InsertCommandRule :execrows
INSERT INTO command_policy_rules(operation_id,kind,position,value) VALUES(?,?,?,?);

-- name: GetCommandRules :many
SELECT * FROM command_policy_rules WHERE operation_id=? ORDER BY kind,position;

-- name: InsertCommandEnv :execrows
INSERT INTO command_environment(operation_id,name,value,guest,header,placeholder,host_count) VALUES(?,?,?,?,?,?,?);

-- name: GetCommandEnv :many
SELECT * FROM command_environment WHERE operation_id=? ORDER BY name;

-- name: InsertCommandHost :execrows
INSERT INTO command_injection_hosts(operation_id,name,position,host) VALUES(?,?,?,?);

-- name: GetCommandHosts :many
SELECT * FROM command_injection_hosts WHERE operation_id=? ORDER BY name,position;

-- name: GetStepTarget :one
SELECT candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key=? AND input_hash=?;

-- name: GetStepStatus :one
SELECT status FROM operation_steps WHERE operation_id=? AND step_key=?;

-- name: CompletePendingLink :execrows
UPDATE operation_steps SET status=? WHERE operation_id=? AND step_key=? AND action='resource_link' AND status='pending';

-- name: MarkLiveOperationRunning :execrows
UPDATE operations SET status='running' WHERE id=? AND status IN ('pending','running');

-- name: InsertOperation :execrows
INSERT INTO operations(id,scope_kind,scope_id,idem_key,action,request_kind,status,created_at,finished_at,result_sandbox_id,result_vm_id,result_image_digest,result_template_id,result_checkpoint_id,result_snapshot_id,resource_ready,result_error) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

-- name: InsertCommandMAC :execrows
INSERT INTO operation_request_macs(operation_id,request_mac) VALUES(?,?);

-- name: MarkLiveOperationSucceeded :execrows
UPDATE operations SET status='succeeded',finished_at=? WHERE id=? AND status IN ('pending','running');

-- name: GetScopedRequest :one
SELECT * FROM operations WHERE scope_kind=? AND scope_id=? AND idem_key=?;

-- name: GetOperationIDByTenantKey :one
SELECT id FROM operations WHERE scope_kind='tenant' AND scope_id=? AND idem_key=?;

-- name: GetTenantScope :one
SELECT scope_id FROM operations WHERE id=? AND scope_kind='tenant';

-- name: GetCommandMACByTenantKey :one
SELECT c.request_mac FROM operations o JOIN operation_request_macs c ON c.operation_id=o.id WHERE o.scope_kind='tenant' AND o.scope_id=? AND o.idem_key=?;

-- name: GetCommandMAC :one
SELECT request_mac FROM operation_request_macs WHERE operation_id=?;

-- name: GetScopedOperation :one
SELECT * FROM operations WHERE id=? AND scope_kind=? AND scope_id=?;

-- name: ListIDsWithUnfinishedSteps :many
SELECT DISTINCT operation_id FROM operation_steps
WHERE status IN ('pending','running') ORDER BY operation_id;

-- name: GetOperationTimes :one
SELECT created_at, finished_at FROM operations WHERE id=?;

-- name: GetOperationProjection :one
SELECT * FROM operations WHERE id=?;

-- name: GetPublicStatus :one
SELECT o.status FROM operations o
JOIN operation_request_macs c ON c.operation_id=o.id
WHERE o.id=? AND o.scope_kind='tenant';

-- name: GetPublication :one
SELECT * FROM operation_publications WHERE operation_id=?;

-- name: HasActivateStep :one
SELECT EXISTS(SELECT 1 FROM operation_steps WHERE operation_id=? AND action='activate') AS has_activate;

-- name: InsertPublication :execrows
INSERT INTO operation_publications(operation_id,response_hash,charge_tenant_id,template_version_id,template_state)
VALUES(sqlc.arg(operation_id),sqlc.arg(response_hash),sqlc.arg(charge_tenant_id),
 (SELECT published_operation_id FROM templates WHERE id=sqlc.narg(template_id)),sqlc.arg(template_state));

-- name: InsertPublishedSandbox :execrows
INSERT INTO published_sandboxes(operation_id,position,sandbox_id,tenant_id,state,vm_id,snapshot_id,instance,template_id,provision_source,vcpus,memory_mib,disk_mib,deadline,paused_until,ttl_seconds,on_expire,auto_renew,policy_mode,max_connections,requires_secrets,resume_attempt,epoch,sandbox_operation_id,last_error,created_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

-- name: ListPublishedSandboxes :many
SELECT * FROM published_sandboxes WHERE operation_id=? ORDER BY position;

-- name: InsertPublishedRule :execrows
INSERT INTO published_sandbox_rules(operation_id,sandbox_position,kind,position,value) VALUES(?,?,?,?,?);

-- name: ListPublishedRules :many
SELECT * FROM published_sandbox_rules WHERE operation_id=? ORDER BY sandbox_position,kind,position;

-- name: InsertPublishedCheckpoint :execrows
INSERT INTO published_checkpoints(operation_id,checkpoint_id,tenant_id,sandbox_id,snapshot_id,vcpus,memory_mib,disk_mib,created_at) VALUES(?,?,?,?,?,?,?,?,?);

-- name: GetPublishedCheckpoint :one
SELECT * FROM published_checkpoints WHERE operation_id=?;

-- name: GetPublishedTemplate :one
SELECT v.*,o.tenant_id FROM template_versions v JOIN template_owners o ON o.template_id=v.template_id WHERE v.operation_id=?;

-- name: ListStepsForRecovery :many
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance
FROM operation_steps ORDER BY rowid;

-- name: ListUnpublishedOperations :many
SELECT o.* FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id
WHERE p.operation_id IS NULL ORDER BY o.id;

-- name: ListPublicationIDs :many
SELECT operation_id FROM operation_publications ORDER BY operation_id;

-- name: CountMissingPublications :one
SELECT COUNT(*) FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id
WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL
AND (EXISTS(SELECT 1 FROM operation_request_macs c WHERE c.operation_id=o.id)
  OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate'));
