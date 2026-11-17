package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type LifecycleAdmission struct {
	History machine.SnapshotProofs
	Upper   machine.ResourceParent
	Parent  operation.Record
	Input   LifecycleInput
	MAC     string
	Role    string
}

func (admission *LifecycleAdmission) LifecycleOperation() string {
	if admission == nil {
		return ""
	}
	return admission.Parent.ID
}

func (admission *LifecycleAdmission) StopLifecycle() (string, bool) {
	if admission == nil {
		return "", false
	}
	action := admission.Input.Command.Action
	return admission.Parent.ID, action == "rollback" || action == "release"
}

// VerifyCaptureCleanup checks only the frozen rollback relationship. Child
// admission remains in AdmitResource and the caller owns the transaction.
func (admission *LifecycleAdmission) VerifyCaptureCleanup(ctx context.Context, tx *sql.Tx, capture machine.CaptureStep, stopOperationID string) error {
	if admission == nil {
		return nil
	}
	if admission.Input.Rollback == nil || admission.Input.Command.Action != "rollback" {
		return fmt.Errorf("%w: raw cleanup parent differs from rollback", errs.ErrInvalid)
	}
	found := false
	for _, ref := range admission.Input.Rollback.RawCaptures {
		if ref.OperationID == capture.OperationID && ref.InputHash == capture.Hash {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("%w: raw capture is absent from frozen rollback responsibilities", errs.ErrConflict)
	}
	stopped, _, err := machine.LoadResourceLink(ctx, tx, admission.Parent.ID, "stop")
	if err != nil {
		return err
	}
	if stopped.ChildID != stopOperationID {
		return fmt.Errorf("%w: raw cleanup stop differs from original rollback", errs.ErrConflict)
	}
	return nil
}

// AdmitResource commits the parent intent and its resource dependency in the
// resource admission transaction. It never polls another service or infers a
// missing resource receipt from a current query.
func (admission *LifecycleAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	if admission == nil {
		return nil
	}
	child := request.Child
	parent := admission.Parent
	input := admission.Input

	existing, err := LoadLifecycle(ctx, tx, parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		fence, fenceErr := LifecycleFence(ctx, tx, input.Sandbox.ID)
		if input.Command.Action == "release" && ((fenceErr == nil && fence != input.PriorFence) || (errors.Is(fenceErr, sql.ErrNoRows) && input.PriorFence != "")) {
			return fmt.Errorf("%w: release source configuration fence changed", errs.ErrConflict)
		}
		if fenceErr == nil && input.Command.Action != "release" {
			return fmt.Errorf("%w: sandbox configuration is fenced", errs.ErrConflict)
		}
		if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
			return fenceErr
		}
		current, err := LoadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, input.Sandbox) {
			return fmt.Errorf("%w: sandbox changed before lifecycle admission", errs.ErrConflict)
		}
		settings, err := LoadSettings(ctx, tx, current.ID)
		if err != nil {
			return err
		}
		if string(operation.Encode(settings)) != string(operation.Encode(input.Settings)) {
			return fmt.Errorf("%w: sandbox configuration changed before lifecycle admission", errs.ErrConflict)
		}
		if input.Command.Action == "release" && len(input.CaptureFailures) > 0 {
			if err = checkReleaseCaptures(ctx, tx, input); err != nil {
				return err
			}
		}
		frozen := operation.Encode(struct {
			Action  string
			Request operation.Command
		}{input.Command.Action, input.Command})
		payload := operation.Encode(input)
		parent.Result.SandboxID = current.ID
		parent.Result.VMID = current.VMID
		parent.Result.SnapshotID = child.Result.SnapshotID
		if input.Rollback != nil {
			parent.Result.SnapshotID = input.Rollback.Checkpoint.SnapshotID
		}
		if input.Command.Action == "resume" {
			parent.Result.VMID = child.Result.VMID
			parent.Result.SnapshotID = input.Sandbox.SnapshotID
		}
		operations := operation.New(tx)
		if err = operations.Insert(ctx, operation.Admission{
			ID:        parent.ID,
			ScopeKind: "tenant",
			ScopeID:   input.RequestTenantID,
			Key:       parent.Key,
			Hash:      operation.Digest(frozen),
			Request:   frozen,
			Status:    "pending",
			Body:      operation.Encode(parent.Result),
		}); err != nil {
			return err
		}
		if err = operations.InsertCredential(ctx, parent.ID, admission.MAC); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
			OperationID:         parent.ID,
			StepKey:             "lifecycle",
			Action:              input.Command.Action,
			InputHash:           operation.Digest(payload),
			Input:               payload,
			CandidateResourceID: input.Sandbox.ID,
			ExpectedInstance:    input.Sandbox.Instance,
			Status:              "pending",
			Body: operation.Encode(LifecycleState{
				Phase: "admitted",
			}),
		})); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).UpsertFence(ctx, dbgen.UpsertFenceParams{
			SandboxID:   current.ID,
			OperationID: parent.ID,
		})); err != nil {
			return err
		}
		if input.Rollback != nil {
			if err = admission.admitRollbackLease(ctx, tx); err != nil {
				return err
			}
		}
		if admission.Upper != nil {
			if err = admission.Upper.AdmitResource(ctx, tx, machine.ResourceAdmission{
				Child:       parent,
				Kind:        "lifecycle",
				Candidate:   input.Sandbox.ID,
				RequestHash: operation.Digest(frozen),
			}); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	} else {
		if operation.Terminal(existing.Status) || !reflect.DeepEqual(existing.Input, input) {
			return fmt.Errorf("%w: lifecycle no longer owns resource admission", errs.ErrConflict)
		}
		fence, err := LifecycleFence(ctx, tx, input.Sandbox.ID)
		if err != nil {
			return err
		}
		if fence != parent.ID {
			return fmt.Errorf("%w: lifecycle configuration was superseded", errs.ErrConflict)
		}
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      parent.ID,
		Role:             admission.Role,
		ExpectedInstance: input.Sandbox.Instance,
	}, request)
}
