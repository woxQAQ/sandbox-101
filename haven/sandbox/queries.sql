-- name: DeleteUnchangedSettings :execrows
DELETE FROM sandbox_settings WHERE sandbox_id=? AND generation=?;

-- name: ListRunningSubjects :many
SELECT s.id,s.tenant FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state='running' ORDER BY s.id;

-- name: ListLeaseCandidates :many
SELECT s.id,s.tenant,s.state FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state IN ('running','paused') ORDER BY s.id;

-- name: InsertSandbox :execrows
INSERT INTO sandboxes(id,tenant,state,template_id,vcpus,memory_mib,disk_mib,creation_epoch,created_at,provision_source,last_error,operation_id,vm_id,instance,snapshot_id,source_operation_id,slot_operation_id,slot_input_hash)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

-- name: ReplaceSandbox :execrows
UPDATE sandboxes SET state=sqlc.arg(state),template_id=sqlc.arg(template_id),vcpus=sqlc.arg(vcpus),memory_mib=sqlc.arg(memory_mib),disk_mib=sqlc.arg(disk_mib),
 creation_epoch=sqlc.arg(creation_epoch),created_at=sqlc.arg(created_at),provision_source=sqlc.arg(provision_source),last_error=sqlc.arg(last_error),
 operation_id=sqlc.narg(operation_id),vm_id=sqlc.arg(vm_id),instance=sqlc.arg(instance),snapshot_id=sqlc.arg(snapshot_id),source_operation_id=sqlc.narg(source_operation_id),slot_operation_id=sqlc.narg(slot_operation_id),slot_input_hash=sqlc.narg(slot_input_hash)
WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state=sqlc.arg(previous_state) AND source_operation_id IS sqlc.narg(previous_source) AND instance=sqlc.arg(previous_instance) AND operation_id IS sqlc.narg(previous_operation);

-- name: AssignWarmSandbox :execrows
UPDATE sandboxes SET operation_id=sqlc.narg(claim_id),slot_operation_id=sqlc.narg(claim_id),slot_input_hash=sqlc.narg(slot_input_hash),creation_epoch=sqlc.arg(creation_epoch),created_at=sqlc.arg(created_at),provision_source='warm'
WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='provisioning' AND source_operation_id=sqlc.narg(source_operation_id) AND vm_id=sqlc.arg(vm_id) AND instance=sqlc.arg(instance) AND slot_operation_id IS NULL AND template_id=sqlc.arg(template_id) AND vcpus=sqlc.arg(vcpus) AND memory_mib=sqlc.arg(memory_mib) AND disk_mib=sqlc.arg(disk_mib);

-- name: PublishRunningSandbox :execrows
UPDATE sandboxes SET state='running', last_error='', operation_id=NULL WHERE id=? AND tenant=? AND state IN ('provisioning','running') AND source_operation_id=? AND instance=?;

-- name: SetSandboxError :execrows
UPDATE sandboxes SET last_error=? WHERE id=? AND tenant=? AND source_operation_id=? AND instance=?;

-- name: PublishSandboxInstance :execrows
UPDATE sandboxes SET instance=? WHERE id=? AND tenant=? AND state='provisioning' AND source_operation_id=? AND vm_id=?;

-- name: TerminateLiveSandbox :execrows
UPDATE sandboxes SET state='terminated',operation_id=sqlc.narg(operation_id) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state!='terminated' AND source_operation_id IS sqlc.narg(source_operation_id) AND instance=sqlc.arg(instance);

-- name: InsertSandboxSettings :execrows
INSERT INTO sandbox_settings(sandbox_id,generation,instance,active_epoch,ttl_seconds,on_expire,auto_renew,deadline,paused_until,policy_mode,max_connections,env_present) VALUES(?,?,?,?,?,?,?,?,?,?,?,?);

-- name: InsertFence :execrows
INSERT INTO sandbox_operation_fences(sandbox_id,operation_id) VALUES(?,?);

-- name: CountSandboxSettings :one
SELECT COUNT(*) FROM sandbox_settings WHERE sandbox_id=?;

-- name: ReplaceSettings :execrows
UPDATE sandbox_settings SET generation=?,instance=?,active_epoch=?,ttl_seconds=?,on_expire=?,auto_renew=?,deadline=?,paused_until=?,policy_mode=?,max_connections=?,env_present=? WHERE sandbox_id=?;

-- name: InsertEnvironment :execrows
INSERT INTO sandbox_environment(sandbox_id,name,value) VALUES(?,?,?);
-- name: InsertPolicyRule :execrows
INSERT INTO sandbox_policy_rules(sandbox_id,kind,position,value) VALUES(?,?,?,?);
-- name: InsertSecretDescriptor :execrows
INSERT INTO sandbox_secret_descriptors(sandbox_id,name,header,placeholder) VALUES(?,?,?,?);
-- name: InsertSecretHost :execrows
INSERT INTO sandbox_secret_hosts(sandbox_id,name,position,host) VALUES(?,?,?,?);
-- name: ClearEnvironment :exec
DELETE FROM sandbox_environment WHERE sandbox_id=?;
-- name: ClearPolicyRules :exec
DELETE FROM sandbox_policy_rules WHERE sandbox_id=?;
-- name: ClearSecretDescriptors :exec
DELETE FROM sandbox_secret_descriptors WHERE sandbox_id=?;

-- name: GetSandbox :one
SELECT * FROM sandboxes WHERE id=? AND tenant=?;

-- name: SandboxOwner :one
SELECT tenant FROM sandboxes WHERE id=?;

-- name: GetSettings :one
SELECT * FROM sandbox_settings WHERE sandbox_id=?;

-- name: ReadEnvironment :many
SELECT name,value FROM sandbox_environment WHERE sandbox_id=? ORDER BY name;

-- name: ReadPolicyRules :many
SELECT kind,value FROM sandbox_policy_rules WHERE sandbox_id=? ORDER BY kind,position;

-- name: ReadSecretDescriptors :many
SELECT d.name,d.header,d.placeholder,h.host FROM sandbox_secret_descriptors d
LEFT JOIN sandbox_secret_hosts h ON h.sandbox_id=d.sandbox_id AND h.name=d.name
WHERE d.sandbox_id=? ORDER BY d.name,h.position;

-- name: UpsertFence :execrows
INSERT INTO sandbox_operation_fences(sandbox_id,operation_id) VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id;

-- name: GetFence :one
SELECT operation_id FROM sandbox_operation_fences WHERE sandbox_id=?;

-- name: ListSandboxIDs :many
SELECT id,tenant FROM sandboxes ORDER BY id;

-- name: ListCapabilitySubjects :many
SELECT tenant,id FROM sandboxes WHERE state<>'terminated' OR CAST(sqlc.arg(include_retired) AS BOOLEAN);

-- name: GetCheckpoint :one
SELECT * FROM checkpoint_bindings WHERE id=?;

-- name: GetOwnedCheckpoint :one
SELECT * FROM checkpoint_bindings WHERE id=? AND tenant=?;

-- name: ListCheckpoints :many
SELECT * FROM checkpoint_bindings ORDER BY id;

-- name: InsertCheckpoint :execrows
INSERT INTO checkpoint_bindings(id,tenant,snapshot_id,operation_id,sandbox_id,template_id,vcpus,memory_mib,disk_mib,created_at) VALUES(?,?,?,?,?,?,?,?,?,?);

-- name: DeleteCheckpoint :execrows
DELETE FROM checkpoint_bindings WHERE id=? AND tenant=? AND snapshot_id=? AND sandbox_id=? AND template_id=? AND vcpus=? AND memory_mib=? AND disk_mib=? AND created_at=?;

-- name: DeleteFence :execrows
DELETE FROM sandbox_operation_fences WHERE sandbox_id=? AND operation_id=?;
