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
func (r *Repository) CountCost(ctx context.Context, kind, key string) (int64, error) {
	return r.queries.CountCost(ctx, dbgen.CountCostParams{
		Kind:        kind,
		ResourceKey: key,
	})
}

func (r *Repository) CountStorageCost(ctx context.Context, cost CostIdentity, storageMiB int64) (int64, error) {
	return r.queries.CountStorageCost(ctx, dbgen.CountStorageCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		StorageMib:        storageMiB,
		SourceOperationID: cost.SourceOperation,
	})
}

func (r *Repository) CountMatchingCost(ctx context.Context, cost Cost) (int64, error) {
	return r.queries.CountMatchingCost(ctx, dbgen.CountMatchingCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
	})
}

func (r *Repository) CountMatchingReservation(ctx context.Context, reservation Reservation) (int64, error) {
	return r.queries.CountMatchingReservation(ctx, dbgen.CountMatchingReservationParams{
		Owner:  reservation.Owner,
		Bytes:  reservation.Bytes,
		Memory: reservation.MemoryMiB,
		Cpus:   reservation.CPUs,
	})
}

func (r *Repository) CountReservation(ctx context.Context, owner string) (int64, error) {
	return r.queries.CountReservation(ctx, owner)
}

// FundedUsage requires existing charges. SUM remains NULL for missing funding,
// so the generated integer scan rejects it rather than interpreting it as zero.
func (r *Repository) FundedUsage(ctx context.Context, tenant string) (Usage, error) {
	row, err := r.queries.GetFundedUsage(ctx, tenant)
	return Usage{
		Slots:      row.SandboxSlots,
		StorageMiB: row.StorageMib,
	}, err
}

func (r *Repository) Usage(ctx context.Context, tenant string) (Usage, error) {
	row, err := r.queries.GetUsage(ctx, tenant)
	return Usage{
		Slots:      row.Slots,
		StorageMiB: row.StorageMib,
	}, err
}

func (r *Repository) ReservedBytes(ctx context.Context) (int64, error) {
	return r.queries.ReservedBytes(ctx)
}

func (r *Repository) DeductReservationBytes(ctx context.Context, owner string, amount int64) error {
	return store.RequireOne(r.queries.DeductReservationBytes(ctx, dbgen.DeductReservationBytesParams{
		Owner:  owner,
		Amount: amount,
	}))
}

func (r *Repository) TransferStorageReservation(ctx context.Context, previous, next string, bytes int64) error {
	return store.RequireOne(r.queries.TransferStorageReservation(ctx, dbgen.TransferStorageReservationParams{
		PreviousOwner: previous,
		NextOwner:     next,
		Bytes:         bytes,
	}))
}

func (r *Repository) ReleaseMatchingReservation(ctx context.Context, reservation Reservation) error {
	return store.RequireOne(r.queries.DeleteMatchingReservation(ctx, dbgen.DeleteMatchingReservationParams{
		Owner:  reservation.Owner,
		Bytes:  reservation.Bytes,
		Memory: reservation.MemoryMiB,
		Cpus:   reservation.CPUs,
	}))
}

func (r *Repository) Reserve(ctx context.Context, reservation Reservation) error {
	return store.RequireOne(r.queries.InsertReservation(ctx, dbgen.InsertReservationParams{
		Owner:  reservation.Owner,
		Bytes:  reservation.Bytes,
		Memory: reservation.MemoryMiB,
		Cpus:   reservation.CPUs,
	}))
}

func (r *Repository) ReleaseReservation(ctx context.Context, owner string) error {
	return store.RequireOne(r.queries.DeleteReservation(ctx, owner))
}

func (r *Repository) AddCost(ctx context.Context, cost Cost) error {
	return store.RequireOne(r.queries.InsertCost(ctx, dbgen.InsertCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
	}))
}

func (r *Repository) ChargeWarmSlot(ctx context.Context, tenant, key, sourceOperation string) error {
	return store.RequireOne(r.queries.ChargeWarmSlot(ctx, dbgen.ChargeWarmSlotParams{
		TenantID:          tenant,
		ResourceKey:       key,
		SourceOperationID: sourceOperation,
	}))
}

func (r *Repository) RetireCostAtKey(ctx context.Context, tenant, kind, key string) error {
	return store.RequireOne(r.queries.DeleteCostAtKey(ctx, dbgen.DeleteCostAtKeyParams{
		TenantID:    tenant,
		Kind:        kind,
		ResourceKey: key,
	}))
}

func (r *Repository) RetireMatchingCost(ctx context.Context, cost Cost) error {
	return store.RequireOne(r.queries.DeleteMatchingCost(ctx, dbgen.DeleteMatchingCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SandboxSlots:      cost.Slots,
		StorageMib:        cost.StorageMiB,
		SourceOperationID: cost.SourceOperation,
	}))
}

func (r *Repository) RetireCost(ctx context.Context, cost CostIdentity) error {
	return store.RequireOne(r.queries.DeleteCost(ctx, dbgen.DeleteCostParams{
		TenantID:          cost.TenantID,
		Kind:              cost.Kind,
		ResourceKey:       cost.Key,
		SourceOperationID: cost.SourceOperation,
	}))
}
