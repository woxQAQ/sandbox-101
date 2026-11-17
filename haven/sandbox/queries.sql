-- name: DeleteUnchangedSettings :execrows
DELETE FROM sandbox_settings WHERE sandbox_id=? AND body=?;

-- name: TerminateUnallocatedSandbox :execrows
UPDATE sandboxes SET state='terminated',body=sqlc.arg(next_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='provisioning' AND body=sqlc.arg(previous_body);

-- name: ListRunningSubjects :many
SELECT s.id,s.tenant FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state='running' ORDER BY s.id;

-- name: ListLeaseCandidates :many
SELECT s.id,s.tenant,s.state,c.body FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state IN ('running','paused') ORDER BY s.id;

-- name: RetireTerminatedBody :execrows
UPDATE sandboxes SET body=sqlc.arg(next_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='terminated' AND body=sqlc.arg(previous_body);

-- name: ReplacePausedBody :execrows
UPDATE sandboxes SET body=sqlc.arg(next_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='paused' AND body=sqlc.arg(previous_body);

-- name: TerminatePausedSandbox :execrows
UPDATE sandboxes SET state='terminated',body=sqlc.arg(next_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='paused' AND body=sqlc.arg(previous_body);

-- name: AssignWarmSandbox :execrows
UPDATE sandboxes SET body=sqlc.arg(next_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state='provisioning' AND body=sqlc.arg(previous_body);

-- name: InsertSandbox :execrows
INSERT INTO sandboxes(id,tenant,state,body) VALUES(?,?,?,?);

-- name: ReuseSandboxForProvision :execrows
UPDATE sandboxes SET state='provisioning',body=sqlc.arg(new_body) WHERE id=sqlc.arg(id) AND tenant=sqlc.arg(tenant) AND state=sqlc.arg(previous_state) AND body=sqlc.arg(previous_body);

-- name: InsertSandboxSettings :execrows
INSERT INTO sandbox_settings(sandbox_id,body) VALUES(?,?);

-- name: InsertConfigurationFence :execrows
INSERT INTO configuration_fences(sandbox_id,operation_id) VALUES(?,?);

-- name: UpdateProvisioningSandboxBody :execrows
UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='provisioning';

-- name: PublishRunningSandbox :execrows
UPDATE sandboxes SET state='running',body=? WHERE id=? AND tenant=? AND state IN ('provisioning','running');

-- name: UpdateSandboxBody :execrows
UPDATE sandboxes SET body=? WHERE id=? AND tenant=?;

-- name: CountSandboxSettings :one
SELECT COUNT(*) FROM sandbox_settings WHERE sandbox_id=?;

-- name: TerminateLiveSandbox :execrows
UPDATE sandboxes SET state='terminated',body=? WHERE id=? AND tenant=? AND state!='terminated';

-- name: RestorePausedSandbox :execrows
UPDATE sandboxes SET state='paused',body=sqlc.arg('next_body') WHERE id=sqlc.arg('id') AND tenant=sqlc.arg('tenant') AND state='terminated' AND body=sqlc.arg('previous_body');

-- name: ReplaceSettings :execrows
UPDATE sandbox_settings SET body=sqlc.arg('next_body') WHERE sandbox_id=sqlc.arg('sandbox_id') AND body=sqlc.arg('previous_body');

-- name: GetSandbox :one
SELECT state,body FROM sandboxes WHERE id=? AND tenant=?;

-- name: SandboxOwner :one
SELECT tenant FROM sandboxes WHERE id=?;

-- name: GetSettings :one
SELECT body FROM sandbox_settings WHERE sandbox_id=?;

-- name: UpsertFence :execrows
INSERT INTO configuration_fences(sandbox_id,operation_id) VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id;

-- name: GetFence :one
SELECT operation_id FROM configuration_fences WHERE sandbox_id=?;

-- name: ListSandboxIDs :many
SELECT id,tenant FROM sandboxes ORDER BY id;

-- name: ListActiveCapabilities :many
SELECT body FROM sandboxes WHERE state<>'terminated';

-- name: ListAllCapabilities :many
SELECT body FROM sandboxes;

-- name: GetCheckpoint :one
SELECT body FROM checkpoint_bindings WHERE id=?;

-- name: GetOwnedCheckpoint :one
SELECT body FROM checkpoint_bindings WHERE id=? AND tenant=?;

-- name: ListCheckpoints :many
SELECT body FROM checkpoint_bindings ORDER BY id;

-- name: InsertCheckpoint :execrows
INSERT INTO checkpoint_bindings(id,tenant,snapshot_id,operation_id,body) VALUES(?,?,?,?,?);

-- name: DeleteCheckpoint :execrows
DELETE FROM checkpoint_bindings WHERE id=? AND tenant=? AND snapshot_id=? AND body=?;

-- name: DeleteFence :execrows
DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?;
