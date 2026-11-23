-- name: PublishedManifestHash :one
SELECT manifest_hash FROM images WHERE digest=?;

-- name: GetManifest :one
SELECT * FROM image_manifests WHERE manifest_hash=?;

-- name: ListImageLayers :many
SELECT layer_digest,diff_id FROM image_layers WHERE manifest_hash=? ORDER BY position;

-- name: ListImageTools :many
SELECT name,version FROM image_tools WHERE manifest_hash=? ORDER BY name;

-- name: ListImageFiles :many
SELECT path,sha256 FROM image_files WHERE manifest_hash=? ORDER BY path;

-- name: ListImageArtifacts :many
SELECT v.* FROM image_artifacts a JOIN image_artifact_values v ON v.artifact_hash=a.artifact_hash WHERE a.manifest_hash=? ORDER BY a.position;

-- name: ListManifestDigests :many
SELECT digest FROM images ORDER BY digest;

-- name: CountManifest :one
SELECT COUNT(*) FROM images WHERE digest=?;

-- name: PublishImage :execrows
INSERT INTO images(digest,manifest_hash) VALUES(?,?);

-- name: InsertManifest :execrows
INSERT INTO image_manifests(digest,format,layout,architecture,builder,manifest_hash,layers_present,tools_present) VALUES(?,?,?,?,?,?,?,?);

-- name: InsertLayer :execrows
INSERT INTO image_layers(manifest_hash,position,layer_digest,diff_id) VALUES(?,?,?,?);

-- name: InsertTool :execrows
INSERT INTO image_tools(manifest_hash,name,version) VALUES(?,?,?);

-- name: InsertFile :execrows
INSERT INTO image_files(manifest_hash,path,sha256) VALUES(?,?,?);

-- name: InsertArtifact :execrows
INSERT INTO image_artifacts(manifest_hash,position,artifact_hash) VALUES(?,?,?);

-- name: GetArtifactValue :one
SELECT * FROM image_artifact_values WHERE artifact_hash=?;

-- name: InsertArtifactValue :execrows
INSERT INTO image_artifact_values(artifact_hash,role,path,sha256,diff_id,device_id,guest_path,serial,cache_sha256) VALUES(?,?,?,?,?,?,?,?,?);

-- name: GetImportIntent :one
SELECT operation_id,layout,input_hash FROM image_intents WHERE deleted_at IS NULL AND digest=?;

-- name: CountImportIntent :one
SELECT COUNT(*) FROM image_intents WHERE deleted_at IS NULL AND digest=?;

-- name: InsertImportIntent :execrows
INSERT INTO image_intents(digest,operation_id,layout,input_hash) VALUES(?,?,?,?);

-- name: DeleteImportIntent :execrows
UPDATE image_intents SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND digest=? AND operation_id=? AND input_hash=? AND layout=?;

-- name: GetCacheWitness :one
SELECT * FROM image_cache_witnesses WHERE witness_hash=?;

-- name: GetCache :one
SELECT c.source_operation_id,sqlc.embed(w) FROM image_caches c JOIN image_cache_witnesses w ON w.witness_hash=c.witness_hash WHERE c.deleted_at IS NULL AND c.builder=? AND c.diff_id=?;

-- name: InsertCache :execrows
INSERT INTO image_caches(builder,diff_id,source_operation_id,witness_hash) VALUES(?,?,?,?);

-- name: InsertCacheWitness :execrows
INSERT INTO image_cache_witnesses(builder,diff_id,witness_hash,physical_bytes,path,directory_device,directory_inode,directory_mode,data_device,data_inode,data_mode,receipt_device,receipt_inode,receipt_mode,receipt_sha256,format,profile,sha256,tar_bytes,entries)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

-- name: DeleteCache :execrows
UPDATE image_caches SET deleted_at=CURRENT_TIMESTAMP WHERE deleted_at IS NULL AND builder=? AND diff_id=? AND source_operation_id=?;

-- name: ListCacheKeys :many
SELECT builder,diff_id FROM image_caches WHERE deleted_at IS NULL ORDER BY builder,diff_id;

-- name: ListFundedCaches :many
SELECT c.source_operation_id,sqlc.embed(w) FROM image_caches c JOIN image_cache_witnesses w ON w.witness_hash=c.witness_hash WHERE c.deleted_at IS NULL AND c.source_operation_id=? ORDER BY c.builder,c.diff_id;

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








-- name: CompletePendingCleanupKind :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND status='pending';


-- name: CompletePendingCleanupResource :execrows
UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind=? AND resource_id=? AND status='pending';


-- name: InsertCleanupIntent :execrows
INSERT INTO cleanup_intents(id,operation_id,resource_kind,resource_id,status,step_key,witness_hash) VALUES(?,?,?,?,'pending',?,?);


-- name: CleanupHistoryPaths :many
SELECT path FROM deletion_history WHERE operation_id=? AND step_key='image_cleanup' ORDER BY ordinal;

-- name: BindCleanupReceipts :execrows
UPDATE operation_steps SET receipts_hash=? WHERE operation_id=? AND step_key='image_cleanup' AND receipts_hash IS NULL;

-- name: InsertImportProgress :execrows
INSERT INTO image_import_progress(operation_id) VALUES(?);

-- name: ReadImportProgress :one
SELECT failed_phase,output_manifest_hash,output_file_hash,output_synced FROM image_import_progress WHERE operation_id=?;

-- name: UpdateImportProgress :execrows
UPDATE image_import_progress SET failed_phase=?,output_manifest_hash=?,output_file_hash=?,output_synced=? WHERE operation_id=?;

-- name: GetImagePreparation :one
SELECT * FROM image_preparations WHERE operation_id=?;

-- name: InsertImagePreparation :execrows
INSERT INTO image_preparations(operation_id,path,work_device,work_inode,work_mode,output_device,output_inode,output_mode) VALUES(?,?,?,?,?,?,?,?);

-- name: SetImportError :execrows
UPDATE operation_steps SET step_error=? WHERE operation_id=? AND step_key='image_import';

-- name: ReadImportLayers :many
SELECT ordinal,tar_bytes,entries,cache_hash,artifact_hash FROM image_import_layers WHERE operation_id=? ORDER BY ordinal;

-- name: AppendImportLayer :execrows
INSERT INTO image_import_layers(operation_id,ordinal,tar_bytes,entries,cache_hash,artifact_hash) VALUES(?,?,?,?,?,?);

-- name: GetStep :one
SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,phase,step_error,receipts_hash
FROM operation_steps WHERE operation_id=? AND step_key=?;

