package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func RetireWarmClaimLease(ctx context.Context, tx *sql.Tx, parent WarmClaimStep) error {
	return machine.ReleaseSnapshotSource(ctx, tx, parent.Input.Pool.Template.TenantID, machine.SourceLease{
		SnapshotID:  parent.Input.Restore.Snapshot.ID,
		OperationID: parent.OperationID,
		StepKey:     "warm_claim",
	})
}

// CompleteWarmClaim transfers the selected pool responsibility in the caller's
// activation transaction. A later publication failure rolls back the handoff.
func CompleteWarmClaim(ctx context.Context, tx *sql.Tx, activation ActivationStep) error {
	parent, err := LoadWarmClaim(ctx, tx, activation.OperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || parent.Input.Pool.SandboxID != activation.Input.SandboxID || parent.Input.Pool.Template.TenantID != activation.Input.TenantID {
		return fmt.Errorf("warm completion current claim changed")
	}
	switch activation.Input.Attempt {
	case "warm":
		if parent.State.Phase != "selected" || activation.Input.ResourceOperation != parent.Input.Source.OperationID || activation.Input.ExpectedInstance != parent.Input.Pool.Instance {
			return fmt.Errorf("warm completion original selection changed")
		}
		reservation, status, err := supply.LoadPoolReservation(ctx, tx, parent.Input.PoolID)
		if err != nil {
			return err
		}
		if status != "selected" || !reflect.DeepEqual(reservation, parent.Input.Pool) {
			return fmt.Errorf("warm completion pool responsibility changed")
		}
		if err = supply.RetireSelectedReservation(ctx, tx, parent.Input.PoolID, operation.Encode(reservation)); err != nil {
			return err
		}
	case "golden":
		link, status, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "fallback")
		if err != nil {
			return err
		}
		if status != "succeeded" || parent.State.Phase != "warm_retired" || link.ChildID != activation.Input.ResourceOperation {
			return fmt.Errorf("fallback completion lacks original clone handoff")
		}
		released, status, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "warm_release")
		if err != nil {
			return err
		}
		if status != "succeeded" {
			return fmt.Errorf("fallback completion lacks warm retirement")
		}
		retired, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
		if err != nil {
			return err
		}
		if retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != parent.Input.Source.OperationID {
			return fmt.Errorf("fallback warm retirement receipt changed")
		}
		if err = supply.RequireRetiredWarm(ctx, tx, parent.Input.PoolID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown warm claim activation attempt")
	}
	if err = RetireWarmClaimLease(ctx, tx, parent); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  parent.OperationID,
		StepKey:      "warm_claim",
		InputHash:    parent.Hash,
		PreviousBody: operation.Encode(parent.State),
		NewBody: operation.Encode(WarmClaimState{
			Phase: "complete",
		}),
	})); err != nil {
		return err
	}
	resource, err := machine.LoadResourceStep(ctx, tx, activation.Input.ResourceOperation)
	if err != nil {
		return err
	}
	body, err := operation.New(tx).LiveBody(ctx, parent.OperationID)
	if err != nil {
		return err
	}
	var result operation.Result
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.VMID = resource.Candidate
	result.ResourceReady = true
	return operation.New(tx).ReplaceLiveBody(ctx, parent.OperationID, operation.Encode(result))
}
