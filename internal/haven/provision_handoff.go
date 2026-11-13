package haven

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/loom"
)

type provisionTargetValidator interface {
	validateProvisionTarget(context.Context, *Service, *sql.Tx, sandboxRecord, restoreSource, string) (*quotaDestination, error)
}

func (parent *lifecycleAdmission) validateProvisionTarget(_ context.Context, _ *Service, _ *sql.Tx, sandbox sandboxRecord, source restoreSource, holder string) (*quotaDestination, error) {
	if parent == nil || parent.Input.Command.Action != "resume" || sandbox.State != "paused" || sandbox.VMID != "" || sandbox.Instance != "" || sandbox.SnapshotID != source.Snapshot.ID || holder != "sleep:"+sandbox.ID {
		return nil, loom.Fail(loom.Conflict, "resume target differs from frozen sleep responsibility")
	}
	return &quotaDestination{OperationID: parent.Parent.ID, Kind: "sleep", ID: sandbox.ID, SnapshotID: sandbox.SnapshotID}, nil
}

func (parent *warmClaimAdmission) validateProvisionTarget(ctx context.Context, service *Service, tx *sql.Tx, sandbox sandboxRecord, source restoreSource, holder string) (*quotaDestination, error) {
	claim, err := loadWarmClaim(ctx, tx, parent.ParentID)
	if err != nil {
		return nil, err
	}
	if claim.State.Phase != "warm_retired" || terminal(claim.Status) || sandbox.State != "terminated" || holder != "claim:"+parent.ParentID || sandbox.ID != claim.Input.Pool.SandboxID || source.Snapshot.ID != claim.Input.Restore.Snapshot.ID {
		return nil, loom.Fail(loom.Conflict, "fallback target differs from retired warm responsibility")
	}
	released, status, err := loadResourceLink(ctx, tx, parent.ParentID, "warm_release")
	if err != nil {
		return nil, err
	}
	if status != "succeeded" {
		return nil, loom.Fail(loom.Conflict, "fallback warm cleanup incomplete")
	}
	receipt, err := loadStep(ctx, tx, released.ChildID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != claim.Input.Source.OperationID || receipt.Input.SlotDestination == nil || receipt.Input.SlotDestination.Kind != "claim" || receipt.Input.SlotDestination.OperationID != parent.ParentID {
		return nil, loom.Fail(loom.ManualRecovery, "fallback original warm retirement proof changed")
	}
	return nil, nil
}
