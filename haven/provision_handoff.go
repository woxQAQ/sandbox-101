package haven

import (
	"context"
	"database/sql"
	"fmt"
)

type provisionTargetValidator interface {
	validateProvisionTarget(context.Context, *Service, *sql.Tx, sandboxRecord, restoreSource, string) (*quotaDestination, error)
}

func (parent *lifecycleAdmission) validateProvisionTarget(_ context.Context, _ *Service, _ *sql.Tx, sandbox sandboxRecord, source restoreSource, holder string) (*quotaDestination, error) {
	if parent == nil || parent.Input.Command.Action != "resume" || sandbox.State != "paused" || sandbox.VMID != "" || sandbox.Instance != "" || sandbox.SnapshotID != source.Snapshot.ID || holder != "sleep:"+sandbox.ID {
		return nil, fmt.Errorf("%w: resume target differs from frozen sleep responsibility", ErrConflict)
	}
	return &quotaDestination{
		OperationID: parent.Parent.ID,
		Kind:        "sleep",
		ID:          sandbox.ID,
		SnapshotID:  sandbox.SnapshotID,
	}, nil
}

func (parent *warmClaimAdmission) validateProvisionTarget(ctx context.Context, service *Service, tx *sql.Tx, sandbox sandboxRecord, source restoreSource, holder string) (*quotaDestination, error) {
	claim, err := loadWarmClaim(ctx, tx, parent.ParentID)
	if err != nil {
		return nil, err
	}
	if claim.State.Phase != "warm_retired" || terminal(claim.Status) || sandbox.State != "terminated" || holder != "claim:"+parent.ParentID || sandbox.ID != claim.Input.Pool.SandboxID || source.Snapshot.ID != claim.Input.Restore.Snapshot.ID {
		return nil, fmt.Errorf("%w: fallback target differs from retired warm responsibility", ErrConflict)
	}
	released, status, err := loadResourceLink(ctx, tx, parent.ParentID, "warm_release")
	if err != nil {
		return nil, err
	}
	if status != "succeeded" {
		return nil, fmt.Errorf("%w: fallback warm cleanup incomplete", ErrConflict)
	}
	receipt, err := loadStep(ctx, tx, released.ChildID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != claim.Input.Source.OperationID || receipt.Input.SlotDestination == nil || receipt.Input.SlotDestination.Kind != "claim" || receipt.Input.SlotDestination.OperationID != parent.ParentID {
		return nil, fmt.Errorf("%w: fallback original warm retirement proof changed", ErrManualRecovery)
	}
	return nil, nil
}
