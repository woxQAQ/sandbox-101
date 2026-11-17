package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// StopHistory retains no running service or mutable connection.
type StopHistory struct {
	OwnershipHistory
}

func (StopHistory) VerifyStopParent(ctx context.Context, query store.DBTX, proof machine.StopParentProof) (string, error) {
	step, source, record := proof.Step, proof.Source, proof.Sandbox
	parent, err := LoadLifecycle(ctx, query, step.Input.Lifecycle)
	if err != nil {
		return "", err
	}
	link, _, err := machine.LoadResourceLink(ctx, query, parent.OperationID, "stop")
	if err != nil {
		return "", err
	}
	fence, err := LifecycleFence(ctx, query, record.ID)
	if err != nil {
		return "", err
	}
	if (parent.Input.Command.Action != "rollback" && parent.Input.Command.Action != "release") || operation.Terminal(parent.Status) || parent.Input.OwnerTenantID != source.Input.TenantID || parent.Input.Source.OperationID != source.OperationID || parent.Input.Source.InputHash != source.Hash || parent.Input.SourceInstance != step.Input.Request.ExpectedInstance || link.ChildID != step.OperationID || fence != parent.OperationID {
		return "", fmt.Errorf("rollback stop no longer owns original instance")
	}
	return parent.Input.Sandbox.State, nil
}
