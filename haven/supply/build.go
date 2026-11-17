package supply

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) setSupplyState(step *SupplyStep, next SupplyState) error {
	if err := store.RequireOne(dbgen.New(service.store.Database).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	step.State = next
	step.Status = "running"
	return service.store.Checkpoint()
}

func (service *Supply) supplyCandidate(step SupplyStep, source machine.ResourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupply(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply current intent changed")
	}
	intent, err := dbgen.New(tx).GetIntent(service.ctx, step.Input.Template.ID)
	if err != nil {
		return err
	}
	if intent.TenantID != step.Input.Template.TenantID || intent.OperationID != step.OperationID {
		return fmt.Errorf("%w: template build was superseded", errs.ErrConflict)
	}
	if err = service.proofs.VerifyGeneration(service.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, ownerOperation, err := machine.LoadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	destination, err := service.ownership.ReadSandbox(service.ctx, tx, step.Input.SandboxID, step.Input.Template.TenantID)
	if err != nil {
		return err
	}
	if instance != source.State.Witness || ownerOperation != "" || !vm.Verified || !vm.AgentReady || vm.Deleting || destination.State != "provisioning" || destination.SourceOperation != source.OperationID || destination.Instance != instance {
		return fmt.Errorf("%w: build source instance ownership changed", errs.ErrConflict)
	}
	return nil
}

func (service *Supply) runBuild(step SupplyStep) error {
	linked, err := service.runResourceLink(step.OperationID, "cold")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(service.ctx, service.store.Database, linked.ChildID)
	if err != nil {
		return err
	}
	if step.State.Phase == "warmup_inflight" {
		return fmt.Errorf("%w: template warmup result unknown; original resource retained", errs.ErrUnknown)
	}
	if step.State.Phase == "admitted" {
		if err = service.supplyCandidate(step, source); err != nil {
			return err
		}
		if service.opts.Guest == nil {
			return fmt.Errorf("%w: guest effects unavailable", errs.ErrInvalid)
		}
		if err = service.opts.Guest.Info(service.ctx, source.Input.Resource); err != nil {
			return err
		}
		if step.Input.Template.Warmup != "" {
			if err = service.setSupplyState(&step, SupplyState{
				Phase: "warmup_inflight",
			}); err != nil {
				return err
			}
			if err = service.fault("build_warmup_intent"); err != nil {
				return err
			}
			if err = service.opts.Guest.Exec(service.ctx, source.Input.Resource, "haven-op:"+step.OperationID+":template-warmup", step.Input.Template.Warmup); err != nil {
				return err
			}
		}
		if err = service.setSupplyState(&step, SupplyState{
			Phase: "warmup_done",
		}); err != nil {
			return err
		}
		if err = service.fault("build_warmup_done"); err != nil {
			return err
		}
	}
	if step.State.Phase == "warmup_done" {
		if err = service.supplyCandidate(step, source); err != nil {
			return err
		}
		if err = service.opts.Backend.Network(service.ctx, source.Input.Resource, false); err != nil {
			return err
		}
		observed, err := service.opts.Backend.Inspect(service.ctx, source.Input.Resource)
		if err != nil {
			return err
		}
		if !observed.Alive || !observed.Booted || observed.Network {
			return fmt.Errorf("%w: build source network/readiness verification failed", errs.ErrConflict)
		}
		if err = service.opts.Guest.Activate(service.ctx, source.Input.Resource, guest.Activation{
			SandboxID:     "template-" + step.Input.Template.ID,
			Hostname:      "sandbox-template",
			ResetIdentity: true,
			Env:           map[string]string{},
			Network:       "none",
		}); err != nil {
			return err
		}
		if err = service.opts.Guest.PrepareTemplate(service.ctx, source.Input.Resource); err != nil {
			return err
		}
		if err = service.setSupplyState(&step, SupplyState{
			Phase: "prepared",
		}); err != nil {
			return err
		}
		if err = service.fault("build_prepared"); err != nil {
			return err
		}
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "capture"); errors.Is(lookup, sql.ErrNoRows) {
		if step.State.Phase != "prepared" {
			return fmt.Errorf("build capture outside prepared phase")
		}
		parent := &SupplyAdmission{
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "capture",
		}
		captures := service.captures
		if _, err = captures.AdmitForParent(service.ctx, machine.CaptureRequest{
			TenantID:         step.Input.Template.TenantID,
			Key:              step.OperationID + ":capture",
			SandboxID:        step.Input.SandboxID,
			ExpectedInstance: source.State.Witness,
		}, parent); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	captured, err := service.runResourceLink(step.OperationID, "capture")
	if err != nil {
		return err
	}
	capture, err := machine.LoadCapture(service.ctx, service.store.Database, captured.ChildID)
	if err != nil {
		return err
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		parent := &SupplyAdmission{
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.Template.TenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.SandboxID,
		}, parent); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	if _, err = service.runResourceLink(step.OperationID, "release"); err != nil {
		return err
	}
	if step.State.Phase != "retired" {
		if err = service.setSupplyState(&step, SupplyState{
			Phase:      "retired",
			SnapshotID: capture.Input.Snapshot.ID,
		}); err != nil {
			return err
		}
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.publishTemplate(service.ctx, tx, step, capture.Input.Snapshot.ID); err != nil {
		return err
	}
	if err = service.fault("template_publication_before_commit"); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (service *Supply) runSupply(step SupplyStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.failSupply(step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.Input.Command.Action == "refill" {
		return service.runRefill(step)
	}
	if step.Input.Command.Action == "template_build" {
		return service.runBuild(step)
	}
	return fmt.Errorf("unsupported supply workflow")
}
