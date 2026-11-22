-- name: CountStorageCost :one
SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND storage_mib=? AND source_operation_id=?;

-- name: CountTenantCostHolders :one
SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=?;

-- name: ReservedCapacity :one
SELECT CAST(COALESCE(SUM(storage_bytes),0) AS INTEGER) AS storage_bytes, CAST(COALESCE(SUM(memory_mib),0) AS INTEGER) AS memory_mib, CAST(COALESCE(SUM(vcpus),0) AS INTEGER) AS vcpus FROM host_capacity_reservations;

-- name: DeductReservationBytes :execrows
UPDATE host_capacity_reservations SET storage_bytes=storage_bytes-sqlc.arg('amount') WHERE owner_key=sqlc.arg('owner_key') AND storage_bytes>=sqlc.arg('amount');

-- name: TransferStorageReservation :execrows
UPDATE host_capacity_reservations SET owner_key=sqlc.arg('next_owner_key')
WHERE owner_key=sqlc.arg('previous_owner_key') AND storage_bytes=sqlc.arg('storage_bytes') AND memory_mib=0 AND vcpus=0;

-- name: DeleteMatchingReservation :execrows
DELETE FROM host_capacity_reservations WHERE owner_key=? AND storage_bytes=? AND memory_mib=? AND vcpus=?;

-- name: ListResourceReferences :many
SELECT resource_kind,resource_id,holder_kind,holder_id FROM resource_refs ORDER BY resource_kind,resource_id;

-- sqlc v1.30.0 does not visit bindings below NOT. De Morgan's equivalent
-- preserves the original backing/checkpoint exclusion, including NULL logic.
-- name: CountActiveSnapshotReferences :one
SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=sqlc.arg(resource_id) AND holder_kind!='backing' AND (holder_kind!='checkpoint' OR holder_id!=sqlc.arg(holder_id));

-- name: CountStorageCostReference :one
SELECT COUNT(*) FROM quota_resources q JOIN quota_refs r USING(tenant_id,kind,resource_key)
WHERE q.tenant_id=? AND q.kind=? AND q.resource_key=? AND q.storage_mib=? AND q.source_operation_id=? AND r.holder_kind=? AND r.holder_id=?;

-- name: CountCostReference :one
SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?;

-- name: CountMatchingReservation :one
SELECT COUNT(*) FROM host_capacity_reservations WHERE owner_key=? AND storage_bytes=? AND memory_mib=? AND vcpus=?;

-- name: CountResourceReference :one
SELECT COUNT(*) FROM resource_refs WHERE resource_kind=? AND resource_id=? AND holder_kind=? AND holder_id=?;

-- name: TransferResourceReference :execrows
UPDATE resource_refs SET holder_kind=sqlc.arg('next_kind'),holder_id=sqlc.arg('next_id')
WHERE resource_kind=sqlc.arg('resource_kind') AND resource_id=sqlc.arg('resource_id')
AND holder_kind=sqlc.arg('previous_kind') AND holder_id=sqlc.arg('previous_id');

-- name: MoveCostReference :execrows
UPDATE quota_refs SET holder_id=sqlc.arg('next_id')
WHERE tenant_id=sqlc.arg('tenant_id') AND kind=sqlc.arg('cost_kind') AND resource_key=sqlc.arg('resource_key')
AND holder_kind=sqlc.arg('holder_kind') AND holder_id=sqlc.arg('previous_id');

-- name: TransferCostReference :execrows
UPDATE quota_refs SET holder_kind=sqlc.arg('next_kind'),holder_id=sqlc.arg('next_id')
WHERE tenant_id=sqlc.arg('tenant_id') AND kind=sqlc.arg('cost_kind') AND resource_key=sqlc.arg('resource_key')
AND holder_kind=sqlc.arg('previous_kind') AND holder_id=sqlc.arg('previous_id');

-- name: ResourceHolderIDs :many
SELECT holder_id FROM resource_refs WHERE resource_kind=? AND resource_id=? AND holder_kind=? ORDER BY holder_id;

-- name: CountResourceHolders :one
SELECT COUNT(*) FROM resource_refs WHERE resource_kind=? AND resource_id=?;

-- name: CountCostHolders :one
SELECT COUNT(*) FROM quota_refs WHERE kind=? AND resource_key=?;

-- name: CountReservation :one
SELECT COUNT(*) FROM host_capacity_reservations WHERE owner_key=?;

-- name: CountCost :one
SELECT COUNT(*) FROM quota_resources WHERE kind=? AND resource_key=?;

-- name: CountMatchingCost :one
SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=?
AND storage_mib=? AND sandbox_slots=? AND source_operation_id=?;

-- name: GetFundedUsage :one
SELECT CAST(SUM(storage_mib) AS INTEGER) AS storage_mib,CAST(SUM(sandbox_slots) AS INTEGER) AS sandbox_slots FROM quota_resources WHERE tenant_id=?;

-- name: GetUsage :one
SELECT CAST(COALESCE(SUM(sandbox_slots),0) AS INTEGER) AS slots,
CAST(COALESCE(SUM(storage_mib),0) AS INTEGER) AS storage_mib
FROM quota_resources WHERE tenant_id=?;

-- name: ReservedBytes :one
SELECT CAST(COALESCE(SUM(storage_bytes),0) AS INTEGER) FROM host_capacity_reservations;

-- name: InsertReservation :execrows
INSERT INTO host_capacity_reservations(owner_key,storage_bytes,memory_mib,vcpus) VALUES(?,?,?,?);

-- name: DeleteReservation :execrows
DELETE FROM host_capacity_reservations WHERE owner_key=?;

-- name: InsertCost :execrows
INSERT INTO quota_resources(tenant_id,kind,resource_key,sandbox_slots,storage_mib,source_operation_id) VALUES(?,?,?,?,?,?);

-- name: ChargeWarmSlot :execrows
UPDATE quota_resources SET sandbox_slots=1 WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=0 AND storage_mib=0 AND source_operation_id=?;

-- name: DeleteCostAtKey :execrows
DELETE FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=?;

-- name: DeleteMatchingCost :execrows
DELETE FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=? AND source_operation_id=?;

-- name: DeleteCost :execrows
DELETE FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND source_operation_id=?;

-- name: InsertResourceReference :execrows
INSERT INTO resource_refs(resource_kind,resource_id,holder_kind,holder_id) VALUES(?,?,?,?);

-- name: DeleteResourceReference :execrows
DELETE FROM resource_refs WHERE resource_kind=? AND resource_id=? AND holder_kind=? AND holder_id=?;

-- name: InsertCostReference :execrows
INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) VALUES(?,?,?,?,?);

-- name: DeleteCostReference :execrows
DELETE FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?;
