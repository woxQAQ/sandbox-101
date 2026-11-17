package sandbox

import (
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

func (service *WarmClaims) failWarmClaim(step WarmClaimStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadWarmClaim(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("warm failure intent changed")
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	known := false
	if _, status, lookup := machine.LoadResourceLink(service.ctx, tx, step.OperationID, "fallback"); lookup == nil {
		known = status == "failed"
	} else if !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	activation, err := LoadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	known = known || (activation.Input.Attempt == "golden" && activation.Status == "failed")
	if !known {
		return nil
	}
	fence, err := LifecycleFence(service.ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	operations := operation.New(tx)
	status, body, err := operations.ResultState(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "warm_claim",
		InputHash:   step.Hash,
		Body: operation.Encode(WarmClaimState{
			Phase: "failed",
		}),
	})); err != nil {
		return err
	}
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "warm claim fallback failed; resource responsibility retained"
		if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
			return err
		}
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "warm_claim:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "warm_claim",
		ResourceID:   step.Input.Pool.SandboxID,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

// RetireSupersededIn joins warm pool/lease retirement to the release transaction.
// The caller retains transaction, publication and checkpoint ownership.
func (service *WarmClaims) RetireSupersededIn(tx *sql.Tx, release LifecycleStep) error {
	parent, err := LoadWarmClaim(service.ctx, tx, release.Input.PriorFence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if parent.Input.Pool.SandboxID != release.Input.Sandbox.ID || parent.Input.Pool.Template.TenantID != release.Input.OwnerTenantID {
		return fmt.Errorf("release superseded warm claim scope changed")
	}
	if parent.Status == "succeeded" {
		return fmt.Errorf("completed warm claim retained a configuration fence")
	}
	reservation, status, err := supply.LoadPoolReservation(service.ctx, tx, parent.Input.PoolID)
	if err == nil {
		if status != "selected" || !reflect.DeepEqual(reservation, parent.Input.Pool) || release.Input.Sandbox.VMID != reservation.VMID {
			return fmt.Errorf("release superseded pool responsibility changed")
		}
		if err = supply.RetireSelectedReservation(service.ctx, tx, parent.Input.PoolID, operation.Encode(reservation)); err != nil {
			return err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = RetireWarmClaimLease(service.ctx, tx, parent); err != nil {
		return err
	}
	operations := operation.New(tx)
	if !operation.Terminal(parent.Status) {
		if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
			OperationID: parent.OperationID,
			StepKey:     "warm_claim",
			InputHash:   parent.Hash,
			Body: operation.Encode(WarmClaimState{
				Phase: "superseded",
			}),
		})); err != nil {
			return err
		}
	}
	state, body, err := operations.ResultState(service.ctx, parent.OperationID)
	if err != nil {
		return err
	}
	if !operation.Terminal(state) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "warm claim superseded by release"
		if err = operations.FailResult(service.ctx, parent.OperationID, operation.Encode(result)); err != nil {
			return err
		}
	}
	return dbgen.New(tx).CompletePendingCleanupIfPresent(service.ctx, dbgen.CompletePendingCleanupIfPresentParams{
		ID:           "warm_claim:" + parent.OperationID,
		OperationID:  parent.OperationID,
		ResourceKind: "warm_claim",
	})
}
