package sandbox

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

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
		parent, err := LoadLifecycle(ctx, tx, input.SlotDestination.OperationID)
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
