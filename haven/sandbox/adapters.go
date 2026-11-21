package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// OwnershipHistory reads canonical sandbox ownership without retaining a store,
// runtime or application. The transaction is always supplied by the workflow.
type OwnershipHistory struct{}

func (OwnershipHistory) ReadSandbox(ctx context.Context, query store.DBTX, id, tenant string) (ownership.Sandbox, error) {
	return LoadSandbox(ctx, query, id, tenant)
}

// SupplyCleanupHistory holds no connection or running service.
type SupplyCleanupHistory struct {
	OwnershipHistory
}

func (SupplyCleanupHistory) SettingsCount(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSandboxSettings(ctx, id)
}

// StopHistory retains no running service or mutable connection.
type StopHistory struct {
	OwnershipHistory
}

func (StopHistory) VerifyStopParent(ctx context.Context, query store.DBTX, proof machine.StopParentProof) (string, error) {
	step, source, record := proof.Step, proof.Source, proof.Sandbox
	parent, err := LoadLifecycleStep(ctx, query, step.Input.Lifecycle)
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

// CaptureDiscardHistory is a stateless reader of retained release authority.
type CaptureDiscardHistory struct {
	OwnershipHistory
}

func (CaptureDiscardHistory) VerifyCaptureDiscardParent(ctx context.Context, query store.DBTX, step machine.CaptureDiscardStep, record ownership.Sandbox) (bool, error) {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	parent, err := LoadLifecycleStep(ctx, query, input.Lifecycle)
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

// ActivationHistory is a stateless proof reader, usable with the read-only
// startup probe before application services or workers exist.
type ActivationHistory struct{}

func (ActivationHistory) ReadActivationProof(ctx context.Context, query store.DBTX, id string) (machine.ActivationProof, error) {
	step, err := LoadActivation(ctx, query, id)
	if err != nil {
		return machine.ActivationProof{}, err
	}
	return step.ResourceProof(), nil
}

// ResourceProof projects an already decoded frozen step without consulting
// current sandbox ownership or deriving a replacement resource witness.
func (step ActivationStep) ResourceProof() machine.ActivationProof {
	return machine.ActivationProof{
		OperationID:       step.OperationID,
		Key:               step.Key,
		Hash:              step.Hash,
		Status:            step.Status,
		Phase:             step.Phase,
		ResourceOperation: step.Input.ResourceOperation,
		ResourceHash:      step.Input.ResourceHash,
		ExpectedInstance:  step.Input.ExpectedInstance,
	}
}

// DeletionHistory reads the original business authority without retaining a
// connection or capturing a running application.
type DeletionHistory struct {
	OwnershipHistory
}

func (DeletionHistory) CurrentSlotBudget(ctx context.Context, query store.DBTX, record ownership.Sandbox, source machine.ResourceStep) (quota.SlotBudget, error) {
	return CurrentSlotBudget(ctx, query, record, source)
}

func (DeletionHistory) VerifyReleaseDestination(ctx context.Context, tx *sql.Tx, step machine.ResourceStep) error {
	return ValidateReleaseSlotDestination(ctx, tx, step)
}

func (DeletionHistory) ResumeDestination(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) (machine.ResumeDestinationProof, error) {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return machine.ResumeDestinationProof{}, err
	}
	return machine.ResumeDestinationProof{
		OperationID: parent.OperationID,
		TenantID:    parent.Input.OwnerTenantID,
		SandboxID:   parent.Input.Sandbox.ID,
	}, nil
}

// DeletionHandoff participates in the transaction owned by physical deletion.
type DeletionHandoff struct{}

func (DeletionHandoff) RestoreResumeSlot(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination, holder string) error {
	return RestoreResumeSlot(ctx, tx, destination, holder)
}

func (DeletionHandoff) RestorePausedSandbox(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) error {
	return RestorePausedSandbox(ctx, tx, destination)
}

func (DeletionHandoff) TerminateSandbox(ctx context.Context, tx *sql.Tx, termination machine.SandboxTermination) error {
	record := termination.Sandbox
	record.State, record.OperationID = "terminated", termination.OperationID
	return store.RequireOne(dbgen.New(tx).TerminateLiveSandbox(ctx, dbgen.TerminateLiveSandboxParams{
		ID:     record.ID,
		Tenant: termination.TenantID,
		Body:   operation.Encode(record),
	}))
}

func (DeletionHandoff) CompleteRelease(ctx context.Context, tx *sql.Tx, step machine.ResourceStep) error {
	input := step.Input
	if input.SlotDestination != nil && input.SlotDestination.Kind == "sleep" {
		parent, err := LoadLifecycleStep(ctx, tx, input.SlotDestination.OperationID)
		if err != nil {
			return err
		}
		if parent.Input.Command.Action == "resume" {
			if err = RestorePausedSandbox(ctx, tx, input.SlotDestination); err != nil {
				return err
			}
		}
	}
	if input.SlotDestination != nil && input.SlotDestination.Kind == "claim" {
		if err := RetireSelectedPool(ctx, tx, step); err != nil {
			return err
		}
	}
	return nil
}
