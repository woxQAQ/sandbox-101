package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

// CaptureDiscardHistory is a stateless reader of retained release authority.
type CaptureDiscardHistory struct {
	OwnershipHistory
}

func (CaptureDiscardHistory) VerifyCaptureDiscardParent(ctx context.Context, query store.DBTX, step machine.CaptureDiscardStep, record ownership.Sandbox) (bool, error) {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	parent, err := LoadLifecycle(ctx, query, input.Lifecycle)
	if err != nil {
		return false, err
	}
	fence, err := LifecycleFence(ctx, query, record.ID)
	if err != nil {
		return false, err
	}
	found := false
	for _, ref := range parent.Input.CaptureFailures {
		if ref.OperationID == source.OperationID && ref.InputHash == source.Hash {
			found = true
		}
	}
	stopLink, _, err := machine.LoadResourceLink(ctx, query, parent.OperationID, "stop")
	if err != nil {
		return false, err
	}
	if !found || operation.Terminal(parent.Status) || parent.Input.Command.Action != "release" || parent.Input.OwnerTenantID != request.TenantID || parent.Input.Sandbox.ID != request.SandboxID || parent.Input.Source.OperationID != resource.OperationID || parent.Input.SourceInstance != request.ExpectedInstance || stopLink.ChildID != request.StopOperationID || fence != parent.OperationID {
		return false, fmt.Errorf("capture discard release authority changed")
	}
	return record.State == parent.Input.Sandbox.State, nil
}

// VerifyCaptureDiscardLink applies only after child admission has committed the
// original parent link. Parent authority is verified separately in the same snapshot.
func (CaptureDiscardHistory) VerifyCaptureDiscardLink(ctx context.Context, query store.DBTX, step machine.CaptureDiscardStep) error {
	link, _, err := machine.LoadResourceLink(ctx, query, step.Input.Lifecycle, "failed-capture:"+step.Input.Source.OperationID)
	if err != nil {
		return err
	}
	if link.ChildID != step.OperationID {
		return fmt.Errorf("capture discard release child changed")
	}
	return nil
}
