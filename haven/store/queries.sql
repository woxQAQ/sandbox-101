-- name: ClaimDeletion :execrows
INSERT INTO deletion_intents(path,operation_id,step_key,receipt_hash) VALUES(?,?,?,?);

-- name: LiveDeletionHash :one
SELECT receipt_hash FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=?;

-- name: InsertDeletionReceipt :execrows
INSERT INTO deletion_history(path,operation_id,step_key,format,directory,receipt_hash,ordinal)
VALUES(sqlc.arg(path),sqlc.arg(operation_id),sqlc.arg(step_key),sqlc.arg(format),sqlc.arg(directory),sqlc.arg(receipt_hash),
 (SELECT COUNT(*) FROM deletion_history WHERE operation_id=sqlc.arg(operation_id) AND step_key=sqlc.arg(step_key)));

-- name: InsertDeletionNode :execrows
INSERT INTO deletion_nodes(operation_id,step_key,receipt_path,relative_path,device,inode,mode) VALUES(?,?,?,?,?,?,?);

-- name: ReadDeletionReceipt :many
SELECT r.format,r.directory,r.receipt_hash,n.relative_path,n.device,n.inode,n.mode FROM deletion_history r
LEFT JOIN deletion_nodes n ON n.receipt_path=r.path AND n.operation_id=r.operation_id AND n.step_key=r.step_key WHERE r.path=? AND r.operation_id=? AND r.step_key=?;

-- name: DeleteMatchingReceipt :execrows
DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key=? AND receipt_hash=?;

-- name: InitializeMetadata :exec
INSERT INTO store_metadata(singleton,format,version,identity,node_key_sha256,epoch) VALUES(1,sqlc.arg(format),sqlc.arg(version),sqlc.arg(identity),sqlc.arg(node_key_sha256),0);

-- name: GetMetadata :one
SELECT format,version,identity,node_key_sha256,epoch FROM store_metadata WHERE singleton=1;

-- name: GetEpoch :one
SELECT epoch FROM store_metadata WHERE singleton=1;

-- name: GetJournalTail :one
SELECT hash FROM journal ORDER BY seq DESC LIMIT 1;

-- name: AppendJournal :execrows
INSERT INTO journal(signed_payload,hash) VALUES(?,?);

-- name: ListJournal :many
SELECT signed_payload,hash FROM journal ORDER BY seq;
