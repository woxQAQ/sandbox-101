package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// SourceLease pins a source publication for one frozen operation step.
type SourceLease struct {
	SnapshotID  string
	OperationID string
	StepKey     string
}

func SourceLeases(ctx context.Context, query store.DBTX) ([]SourceLease, error) {
	rows, err := dbgen.New(query).ListSourceLeases(ctx)
	if err != nil {
		return nil, err
	}
	var leases []SourceLease
	for _, row := range rows {
		leases = append(leases, SourceLease{
			SnapshotID:  row.SnapshotID,
			OperationID: row.OperationID,
			StepKey:     row.StepKey,
		})
	}
	return leases, nil
}

// Source pinning follows the caller's frozen parent/source proof. These writes
// only join the lease and both responsibilities in that caller's transaction.
func PinSnapshotSource(ctx context.Context, tx *sql.Tx, tenant string, lease SourceLease) error {
	if err := store.RequireOne(dbgen.New(tx).InsertSourceLease(ctx, dbgen.InsertSourceLeaseParams{
		SnapshotID:  lease.SnapshotID,
		OperationID: lease.OperationID,
		StepKey:     lease.StepKey,
	})); err != nil {
		return err
	}
	holder := lease.OperationID + ":" + lease.StepKey
	responsibilities := quota.New(tx)
	if err := responsibilities.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   tenant,
		Kind:       "snapshot",
		Key:        lease.SnapshotID,
		HolderKind: "engine_dependency",
		HolderID:   holder,
	})
}

// Release requires the caller's completed handoff or reclamation proof; absence
// of a lease is not accepted as evidence that another path discharged it.
func ReleaseSnapshotSource(ctx context.Context, tx *sql.Tx, tenant string, lease SourceLease) error {
	if err := store.RequireOne(dbgen.New(tx).DeleteSourceLease(ctx, dbgen.DeleteSourceLeaseParams{
		SnapshotID:  lease.SnapshotID,
		OperationID: lease.OperationID,
		StepKey:     lease.StepKey,
	})); err != nil {
		return err
	}
	holder := lease.OperationID + ":" + lease.StepKey
	responsibilities := quota.New(tx)
	if err := responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         lease.SnapshotID,
		HolderKind: "step",
		HolderID:   holder,
	}); err != nil {
		return err
	}
	return responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   tenant,
		Kind:       "snapshot",
		Key:        lease.SnapshotID,
		HolderKind: "engine_dependency",
		HolderID:   holder,
	})
}
