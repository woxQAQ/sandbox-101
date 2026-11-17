-- name: GetManifestBody :one
SELECT body FROM images WHERE digest=?;

-- name: ListManifestBodies :many
SELECT body FROM images;

-- name: ListManifestDigests :many
SELECT digest FROM images ORDER BY digest;

-- name: CountManifest :one
SELECT COUNT(*) FROM images WHERE digest=?;

-- name: InsertManifest :execrows
INSERT INTO images(digest,body) VALUES(?,?);

-- name: GetImportIntent :one
SELECT operation_id,layout,body FROM image_intents WHERE digest=?;

-- name: CountImportIntent :one
SELECT COUNT(*) FROM image_intents WHERE digest=?;

-- name: InsertImportIntent :execrows
INSERT INTO image_intents(digest,operation_id,layout,body) VALUES(?,?,?,?);

-- name: DeleteImportIntent :execrows
DELETE FROM image_intents WHERE digest=? AND operation_id=? AND body=?;

-- name: GetCacheBody :one
SELECT body FROM image_caches WHERE builder=? AND diff_id=?;

-- name: InsertCache :execrows
INSERT INTO image_caches(builder,diff_id,body) VALUES(?,?,?);

-- name: DeleteCache :execrows
DELETE FROM image_caches WHERE builder=? AND diff_id=? AND body=?;

-- name: ListCacheKeys :many
SELECT builder,diff_id FROM image_caches ORDER BY builder,diff_id;

-- name: ListFundedCacheBodies :many
SELECT body FROM image_caches WHERE json_extract(body,'$.source_operation')=(CAST(? AS TEXT)||'');

-- name: RecordLiveStepProgress :execrows
UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: FailLiveStep :execrows
UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running');


-- name: ListStepOperations :many
SELECT operation_id FROM operation_steps WHERE step_key=?;


-- name: ListStepAdmissions :many
SELECT operation_id FROM operation_steps WHERE step_key=? ORDER BY rowid;


-- name: CountPendingOwnedCleanup :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: CountPendingCleanupResource :one
SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending';


-- name: CountPendingCleanupID :one
SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND resource_kind=? AND status='pending';


-- name: InsertDeletionReceipt :execrows
INSERT INTO deletion_intents(path,operation_id,step_key,body) VALUES(?,?,?,?);


-- name: DeleteMatchingDeletionReceipt :execrows
DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=? AND body=?;


-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: CompletePendingCleanupResource :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='pending';


-- name: AdvanceLiveStep :execrows
UPDATE operation_steps SET status='running',body=sqlc.arg('new_body')
WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key')
AND input_hash=sqlc.arg('input_hash') AND status IN ('pending','running') AND body=sqlc.arg('previous_body');


-- name: GetDeletionReceipt :one
SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?;


-- name: CompleteUnchangedStep :execrows
UPDATE operation_steps SET status='succeeded',body=sqlc.arg('new_body') WHERE operation_id=sqlc.arg('operation_id') AND step_key=sqlc.arg('step_key') AND input_hash=sqlc.arg('input_hash') AND body=sqlc.arg('previous_body') AND status IN ('pending','running');


-- name: InsertStep :execrows
INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?);


-- name: CompleteLiveStep :execrows
UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key=? AND status IN ('pending','running') AND input_hash=?;


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,body) VALUES(?,?,?,?,'pending',?);


-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body
FROM operation_steps WHERE operation_id=? AND step_key=?;

