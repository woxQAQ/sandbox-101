package sandbox

import (
	"database/sql"
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
)

func (service *WarmClaims) setWarmClaimPhase(step *WarmClaimStep, phase string) error {
	if err := store.RequireOne(dbgen.New(service.store.Database).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
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
	return service.store.Checkpoint()
}

func (service *WarmClaims) seedFallbackActivation(step WarmClaimStep, source machine.ResourceStep) error {
	guard := service.guards.Configuration(step.Input.Pool.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	latest, err := LoadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if latest.Input.Attempt == "golden" {
		return nil
	}
	current, err := LoadWarmClaim(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.State.Phase != "warm_retired" || latest.Status != "failed" || latest.Input.Attempt != "warm" {
		return fmt.Errorf("%w: fallback activation no longer owns warm retirement", errs.ErrConflict)
	}
	fence, err := LifecycleFence(service.ctx, tx, step.Input.Pool.SandboxID)
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
	if err = store.RequireOne(dbgen.New(tx).InsertStep(service.ctx, dbgen.InsertStepParams{
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
	return service.store.Checkpoint()
}

func (service *WarmClaims) runWarmClaim(step WarmClaimStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.failWarmClaim(step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.State.Phase == "selected" {
		lock := service.guards.VM(step.Input.Pool.VMID)
		lock.Lock()
		err := service.activations.Continue(step.OperationID)
		lock.Unlock()
		activation, lookup := LoadActivation(service.ctx, service.store.Database, step.OperationID)
		if lookup != nil {
			return errors.Join(err, lookup)
		}
		if activation.Status == "succeeded" {
			return nil
		}
		if activation.Status != "failed" {
			return err
		}
		if err = service.setWarmClaimPhase(&step, "warm_failed"); err != nil {
			return err
		}
	}
	if step.State.Phase == "warm_failed" {
		if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "warm_release"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &WarmClaimAdmission{
				ParentID: step.OperationID,
				Input:    step.Input,
				Role:     "warm_release",
			}
			if _, err := service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
				TenantID:  step.Input.Pool.Template.TenantID,
				Key:       step.OperationID + ":warm_release",
				SandboxID: step.Input.Pool.SandboxID,
			}, parent); err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := service.runResourceLink(step.OperationID, "warm_release"); err != nil {
			return err
		}
		if err := service.setWarmClaimPhase(&step, "warm_retired"); err != nil {
			return err
		}
	}
	if step.State.Phase != "warm_retired" {
		return fmt.Errorf("unsupported warm claim phase")
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "fallback"); errors.Is(lookup, sql.ErrNoRows) {
		template := step.Input.Pool.Template
		manifest, err := image.LoadManifest(service.ctx, service.store.Database, template.Config.Image)
		if err != nil {
			return err
		}
		if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
			return err
		}
		source, err := service.proofs.Snapshots().LoadRestoreSource(service.ctx, service.store.Database, template.TenantID, template.SnapshotID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(source, step.Input.Restore) {
			return fmt.Errorf("%w: frozen fallback source publication changed", errs.ErrConflict)
		}
		if err = machine.VerifyRestoreSourceFiles(source); err != nil {
			return err
		}
		destination, err := LoadSandbox(service.ctx, service.store.Database, step.Input.Pool.SandboxID, template.TenantID)
		if err != nil {
			return err
		}
		request := machine.CloneRequest{
			TenantID:   template.TenantID,
			Key:        step.OperationID + ":fallback",
			SnapshotID: template.SnapshotID,
		}
		frozen := operation.Encode(struct {
			Action  string
			Request machine.CloneRequest
		}{"clone", request})
		parent := &WarmClaimAdmission{
			ParentID: step.OperationID,
			Input:    step.Input,
			Role:     "fallback",
		}
		identity := destination.Identity
		identity.ProvisionSource = "golden"
		if _, err = service.provisioning.Admit(service.ctx, template.TenantID, request.Key, frozen, operation.Digest(frozen), template, manifest, &source, nil, &ProvisionTarget{
			Identity:      &identity,
			Sandbox:       destination,
			SlotOperation: step.Input.Source.Input.SlotOperation,
			SlotHolder:    "claim:" + step.OperationID,
			Parent:        parent,
		}); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	fallback, err := service.runResourceLink(step.OperationID, "fallback")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(service.ctx, service.store.Database, fallback.ChildID)
	if err != nil {
		return err
	}
	if err = service.seedFallbackActivation(step, source); err != nil {
		return err
	}
	lock := service.guards.VM(source.Candidate)
	lock.Lock()
	defer lock.Unlock()
	return service.activations.Continue(step.OperationID)
}
