-- name: InitializeMetadata :exec
INSERT INTO store_metadata(singleton,format,version,identity,node_key_sha256,epoch) VALUES(1,sqlc.arg(format),sqlc.arg(version),sqlc.arg(identity),sqlc.arg(node_key_sha256),0);

-- name: GetMetadata :one
SELECT format,version,identity,node_key_sha256,epoch FROM store_metadata WHERE singleton=1;

-- name: GetEpoch :one
SELECT epoch FROM store_metadata WHERE singleton=1;

-- name: GetJournalTail :one
SELECT hash FROM journal ORDER BY seq DESC LIMIT 1;

-- name: AppendJournal :execrows
INSERT INTO journal(body,hash) VALUES(?,?);

-- name: ListJournal :many
SELECT body,hash FROM journal ORDER BY seq;
