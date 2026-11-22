package quota

import (
	"context"

	"github.com/woxQAQ/haven/quota/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type Usage struct {
	Slots      int64
	StorageMiB int64
}

// Reservation holds host capacity, independently of a tenant's charged cost.
type Reservation struct {
	Owner     string
	Bytes     int64
	MemoryMiB int64
	CPUs      int64
}

func (reservation Reservation) params() dbgen.InsertReservationParams {
	return dbgen.InsertReservationParams{
		OwnerKey:     reservation.Owner,
		StorageBytes: reservation.Bytes,
		MemoryMib:    reservation.MemoryMiB,
		Vcpus:        reservation.CPUs,
	}
}

// CostIdentity retains the operation that originally assumed this cost.
type CostIdentity struct {
	TenantID        string
	Kind            string
	Key             string
	SourceOperation string
}

type Cost struct {
	CostIdentity

	Slots      int64
	StorageMiB int64
}

// CountCost includes every tenant, so duplicate custody cannot be hidden by a scoped lookup.
func CountCost(ctx context.Context, db store.DBTX, kind, key string) (int64, error) {
	return dbgen.New(db).CountCost(ctx, dbgen.CountCostParams{
		Kind:        kind,
		ResourceKey: key,
	})
}

func CountStorageCost(ctx context.Context, db store.DBTX, cost CostIdentity, storageMiB int64) (int64, error) {
	return dbgen.New(db).CountStorageCost(ctx, dbgen.CountStorageCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		StorageMib:        storageMiB,
		SourceOperationID: cost.SourceOperation,
	})
}

func CountMatchingCost(ctx context.Context, db store.DBTX, cost Cost) (int64, error) {
	return dbgen.New(db).CountMatchingCost(ctx, dbgen.CountMatchingCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
	})
}

func CountMatchingReservation(ctx context.Context, db store.DBTX, reservation Reservation) (int64, error) {
	return dbgen.New(db).CountMatchingReservation(ctx, dbgen.CountMatchingReservationParams(reservation.params()))
}

func CountReservation(ctx context.Context, db store.DBTX, owner string) (int64, error) {
	return dbgen.New(db).CountReservation(ctx, owner)
}

// FundedUsage requires existing charges. SUM remains NULL for missing funding,
// so the generated integer scan rejects it rather than interpreting it as zero.
func FundedUsage(ctx context.Context, db store.DBTX, tenant string) (Usage, error) {
	row, err := dbgen.New(db).GetFundedUsage(ctx, tenant)
	return Usage{
		Slots:      row.SandboxSlots,
		StorageMiB: row.StorageMib,
	}, err
}

func TenantUsage(ctx context.Context, db store.DBTX, tenant string) (Usage, error) {
	row, err := dbgen.New(db).GetUsage(ctx, tenant)
	return Usage{
		Slots:      row.Slots,
		StorageMiB: row.StorageMib,
	}, err
}

func ReservedBytes(ctx context.Context, db store.DBTX) (int64, error) {
	return dbgen.New(db).ReservedBytes(ctx)
}

func DeductReservationBytes(ctx context.Context, db store.DBTX, owner string, amount int64) error {
	return store.RequireOne(dbgen.New(db).DeductReservationBytes(ctx, dbgen.DeductReservationBytesParams{
		OwnerKey: owner,
		Amount:   amount,
	}))
}

func TransferStorageReservation(ctx context.Context, db store.DBTX, previous, next string, bytes int64) error {
	return store.RequireOne(dbgen.New(db).TransferStorageReservation(ctx, dbgen.TransferStorageReservationParams{
		PreviousOwnerKey: previous,
		NextOwnerKey:     next,
		StorageBytes:     bytes,
	}))
}

func ReleaseMatchingReservation(ctx context.Context, db store.DBTX, reservation Reservation) error {
	return store.RequireOne(dbgen.New(db).DeleteMatchingReservation(ctx, dbgen.DeleteMatchingReservationParams(reservation.params())))
}

func Reserve(ctx context.Context, db store.DBTX, reservation Reservation) error {
	return store.RequireOne(dbgen.New(db).InsertReservation(ctx, reservation.params()))
}

func ReleaseReservation(ctx context.Context, db store.DBTX, owner string) error {
	return store.RequireOne(dbgen.New(db).DeleteReservation(ctx, owner))
}

func AddCost(ctx context.Context, db store.DBTX, cost Cost) error {
	return store.RequireOne(dbgen.New(db).InsertCost(ctx, dbgen.InsertCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
	}))
}

func ChargeWarmSlot(ctx context.Context, db store.DBTX, tenant, key, sourceOperation string) error {
	return store.RequireOne(dbgen.New(db).ChargeWarmSlot(ctx, dbgen.ChargeWarmSlotParams{
		TenantID:          tenant,
		ResourceKey:       key,
		SourceOperationID: sourceOperation,
	}))
}

func RetireCostAtKey(ctx context.Context, db store.DBTX, tenant, kind, key string) error {
	return store.RequireOne(dbgen.New(db).DeleteCostAtKey(ctx, dbgen.DeleteCostAtKeyParams{
		TenantID:    tenant,
		Kind:        kind,
		ResourceKey: key,
	}))
}

func RetireMatchingCost(ctx context.Context, db store.DBTX, cost Cost) error {
	return store.RequireOne(dbgen.New(db).DeleteMatchingCost(ctx, dbgen.DeleteMatchingCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
		SourceOperationID: cost.SourceOperation,
	}))
}

func RetireCost(ctx context.Context, db store.DBTX, cost CostIdentity) error {
	return store.RequireOne(dbgen.New(db).DeleteCost(ctx, dbgen.DeleteCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
	}))
}
