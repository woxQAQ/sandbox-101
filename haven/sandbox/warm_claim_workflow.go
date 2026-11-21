package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

func (wc *WarmClaims) setWarmClaimPhase(step *WarmClaimStep, phase string) error {
	if err := store.RequireOne(dbgen.New(wc.store.Database).AdvanceLiveStep(wc.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "warm_claim",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody: operation.Encode(WarmClaimState{
			Phase: phase,
		}),
	})); err != nil {
		return err
	}
	step.State.Phase = phase
	step.Status = "running"
	return wc.store.Checkpoint()
}

func (wc *WarmClaims) seedFallbackActivation(step WarmClaimStep, source machine.ResourceStep) error {
	unlock := wc.sandboxConfigLocks.Lock(step.Input.Pool.SandboxID)
	defer unlock()
	wc.storeWriteMu.Lock()
	defer wc.storeWriteMu.Unlock()
	tx, err := wc.store.Database.BeginTx(wc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	latest, err := LoadActivation(wc.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if latest.Input.Attempt == "golden" {
		return nil
	}
	current, err := LoadWarmClaim(wc.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.State.Phase != "warm_retired" || latest.Status != "failed" || latest.Input.Attempt != "warm" {
		return fmt.Errorf("%w: fallback activation no longer owns warm retirement", errs.ErrConflict)
	}
	fence, err := LifecycleFence(wc.ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", errs.ErrConflict)
	}
	if source.Status != "succeeded" || source.State.Witness == "" || source.Input.SandboxID != step.Input.Pool.SandboxID || source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != step.Input.Restore.Snapshot.ID {
		return fmt.Errorf("fallback lacks successful frozen clone witness")
	}
	input := ActivationInput{
		Attempt:           "golden",
		RequestTenantID:   step.Input.RequestTenantID,
		TenantID:          step.Input.Pool.Template.TenantID,
		SandboxID:         step.Input.Pool.SandboxID,
		ResourceOperation: source.OperationID,
		ResourceHash:      source.Hash,
		ExpectedInstance:  source.State.Witness,
		Settings:          step.Input.Settings,
		ResetIdentity:     true,
	}
	payload := operation.Encode(input)
	if err = store.RequireOne(dbgen.New(tx).InsertStep(wc.ctx, dbgen.InsertStepParams{
		OperationID:         step.OperationID,
		StepKey:             "activation:golden",
		Action:              "activate",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return wc.store.Checkpoint()
}

func (wc *WarmClaims) runWarmClaim(step WarmClaimStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && wc.ctx.Err() == nil {
			runErr = errors.Join(runErr, wc.failWarmClaim(step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.State.Phase == "selected" {
		err := wc.activations.activateVM(step.Input.Pool.VMID, step.OperationID)
		activation, lookup := LoadActivation(wc.ctx, wc.store.Database, step.OperationID)
		if lookup != nil {
			return errors.Join(err, lookup)
		}
		if activation.Status == "succeeded" {
			return nil
		}
		if activation.Status != "failed" {
			return err
		}
		if err = wc.setWarmClaimPhase(&step, "warm_failed"); err != nil {
			return err
		}
	}
	if step.State.Phase == "warm_failed" {
		if _, _, lookup := machine.LoadResourceLink(wc.ctx, wc.store.Database, step.OperationID, "warm_release"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &WarmClaimAdmission{
				ParentID: step.OperationID,
				Input:    step.Input,
				Role:     "warm_release",
			}
			if _, err := wc.deletion.AdmitReleaseForParent(wc.ctx, machine.ReleaseRequest{
				TenantID:  step.Input.Pool.Template.TenantID,
				Key:       step.OperationID + ":warm_release",
				SandboxID: step.Input.Pool.SandboxID,
			}, parent); err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := wc.runResourceLink(step.OperationID, "warm_release"); err != nil {
			return err
		}
		if err := wc.setWarmClaimPhase(&step, "warm_retired"); err != nil {
			return err
		}
	}
	if step.State.Phase != "warm_retired" {
		return fmt.Errorf("unsupported warm claim phase")
	}
	if _, _, lookup := machine.LoadResourceLink(wc.ctx, wc.store.Database, step.OperationID, "fallback"); errors.Is(lookup, sql.ErrNoRows) {
		template := step.Input.Pool.Template
		manifest, err := image.LoadManifest(wc.ctx, wc.store.Database, template.Config.Image)
		if err != nil {
			return err
		}
		if err = image.VerifyFiles(manifest, filepath.Join(wc.store.Directory, "images", manifest.Digest[7:])); err != nil {
			return err
		}
		source, err := wc.proofs.Snapshots().LoadSnapshotSource(wc.ctx, wc.store.Database, template.TenantID, template.SnapshotID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(source, step.Input.Restore) {
			return fmt.Errorf("%w: frozen fallback source publication changed", errs.ErrConflict)
		}
		if err = machine.VerifyRestoreSourceFiles(source); err != nil {
			return err
		}
		destination, err := LoadSandbox(wc.ctx, wc.store.Database, step.Input.Pool.SandboxID, template.TenantID)
		if err != nil {
			return err
		}
		request := machine.CloneRequest{
			TenantID:   template.TenantID,
			Key:        step.OperationID + ":fallback",
			SnapshotID: template.SnapshotID,
		}
		frozen := machine.FreezeProvisionRequest(request, nil)
		parent := &WarmClaimAdmission{
			ParentID: step.OperationID,
			Input:    step.Input,
			Role:     "fallback",
		}
		identity := destination.Identity
		identity.ProvisionSource = "golden"
		if _, err = wc.provisioning.Admit(wc.ctx, ProvisionAdmission{
			Request: supply.ProvisionRequest{
				TenantID: template.TenantID,
				Key:      request.Key,
				Frozen:   frozen,
				Template: template,
				Image:    manifest,
				Restore:  &source,
			},
			Target: &ProvisionTarget{
				Identity:      &identity,
				Sandbox:       destination,
				SlotOperation: step.Input.Source.Input.SlotOperation,
				SlotHolder:    "claim:" + step.OperationID,
				Parent:        parent,
			},
		}); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	fallback, err := wc.runResourceLink(step.OperationID, "fallback")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(wc.ctx, wc.store.Database, fallback.ChildID)
	if err != nil {
		return err
	}
	if err = wc.seedFallbackActivation(step, source); err != nil {
		return err
	}
	return wc.activations.activateVM(source.Candidate, step.OperationID)
}

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

func (wc *WarmClaims) failWarmClaim(step WarmClaimStep) error {
	wc.storeWriteMu.Lock()
	defer wc.storeWriteMu.Unlock()
	tx, err := wc.store.Database.BeginTx(wc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadWarmClaim(wc.ctx, tx, step.OperationID)
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
	if _, status, lookup := machine.LoadResourceLink(wc.ctx, tx, step.OperationID, "fallback"); lookup == nil {
		known = status == "failed"
	} else if !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	activation, err := LoadActivation(wc.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	known = known || (activation.Input.Attempt == "golden" && activation.Status == "failed")
	if !known {
		return nil
	}
	fence, err := LifecycleFence(wc.ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	operations := operation.New(tx)
	status, body, err := operations.ResultState(wc.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(wc.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "warm_claim",
		InputHash:   step.Hash,
		Body: operation.Encode(WarmClaimState{
			Phase: "failed",
		}),
	})); err != nil {
		return err
	}
	var completed []operation.Completion
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "warm claim fallback failed; resource responsibility retained"
		completed = append(completed, operation.Completion{
			ID:     step.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(wc.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "warm_claim:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "warm_claim",
		ResourceID:   step.Input.Pool.SandboxID,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return err
	}
	if err = wc.committer.Complete(tx, completed...); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return wc.store.Checkpoint()
}

// RetireSupersededIn joins warm pool/lease retirement to the release transaction.
// The caller retains transaction, publication and checkpoint ownership.
func (wc *WarmClaims) RetireSupersededIn(tx *sql.Tx, release LifecycleStep) ([]operation.Completion, error) {
	parent, err := LoadWarmClaim(wc.ctx, tx, release.Input.PriorFence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if parent.Input.Pool.SandboxID != release.Input.Sandbox.ID || parent.Input.Pool.Template.TenantID != release.Input.OwnerTenantID {
		return nil, fmt.Errorf("release superseded warm claim scope changed")
	}
	if parent.Status == "succeeded" {
		return nil, fmt.Errorf("completed warm claim retained a configuration fence")
	}
	reservation, status, err := supply.LoadPoolReservation(wc.ctx, tx, parent.Input.PoolID)
	if err == nil {
		if status != "selected" || !reflect.DeepEqual(reservation, parent.Input.Pool) || release.Input.Sandbox.VMID != reservation.VMID {
			return nil, fmt.Errorf("release superseded pool responsibility changed")
		}
		if err = supply.RetireSelectedReservation(wc.ctx, tx, parent.Input.PoolID, operation.Encode(reservation)); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if err = RetireWarmClaimLease(wc.ctx, tx, parent); err != nil {
		return nil, err
	}
	operations := operation.New(tx)
	if !operation.Terminal(parent.Status) {
		if err = store.RequireOne(dbgen.New(tx).FailLiveStep(wc.ctx, dbgen.FailLiveStepParams{
			OperationID: parent.OperationID,
			StepKey:     "warm_claim",
			InputHash:   parent.Hash,
			Body: operation.Encode(WarmClaimState{
				Phase: "superseded",
			}),
		})); err != nil {
			return nil, err
		}
	}
	state, body, err := operations.ResultState(wc.ctx, parent.OperationID)
	if err != nil {
		return nil, err
	}
	var completed []operation.Completion
	if !operation.Terminal(state) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		result.Error = "warm claim superseded by release"
		completed = append(completed, operation.Completion{
			ID:     parent.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if err = dbgen.New(tx).CompletePendingCleanupIfPresent(wc.ctx, dbgen.CompletePendingCleanupIfPresentParams{
		ID:           "warm_claim:" + parent.OperationID,
		OperationID:  parent.OperationID,
		ResourceKind: "warm_claim",
	}); err != nil {
		return nil, err
	}
	return completed, nil
}
