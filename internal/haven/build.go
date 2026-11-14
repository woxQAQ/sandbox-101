package haven

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) setSupplyState(step *supplyStep, next supplyState) error {
	if err := requireWrite(service.store.db.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='supply' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(next), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	step.State = next
	step.Status = "running"
	return service.store.checkpoint()
}

func (service *Service) supplyCandidate(step supplyStep, source resourceStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadSupply(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply current intent changed")
	}
	var owner, pointer string
	if err = tx.QueryRowContext(service.ctx, "SELECT tenant_id,operation_id FROM template_intents WHERE template_id=?", step.Input.Template.ID).Scan(&owner, &pointer); err != nil {
		return err
	}
	if owner != step.Input.Template.TenantID || pointer != step.OperationID {
		return loom.Fail(loom.Conflict, "template build was superseded")
	}
	if err = service.verifyGeneration(service.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, ownerOperation, err := service.loadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	sandbox, err := loadSandbox(service.ctx, tx, step.Input.SandboxID, step.Input.Template.TenantID)
	if err != nil {
		return err
	}
	if instance != source.State.Witness || ownerOperation != "" || !vm.Verified || !vm.AgentReady || vm.Deleting || sandbox.State != "provisioning" || sandbox.SourceOperation != source.OperationID || sandbox.Instance != instance {
		return loom.Fail(loom.Conflict, "build source instance ownership changed")
	}
	return nil
}

func (service *Service) runBuild(step supplyStep) error {
	linked, err := service.runResourceLink(step.OperationID, "cold")
	if err != nil {
		return err
	}
	source, err := loadStep(service.ctx, service.store.db, linked.ChildID)
	if err != nil {
		return err
	}
	if step.State.Phase == "warmup_inflight" {
		return loom.Fail(loom.Unknown, "template warmup result unknown; original resource retained")
	}
	if step.State.Phase == "admitted" {
		if err = service.supplyCandidate(step, source); err != nil {
			return err
		}
		if service.opts.Guest == nil {
			return loom.Fail(loom.Invalid, "guest effects unavailable")
		}
		if err = service.opts.Guest.Info(service.ctx, source.Input.Resource); err != nil {
			return err
		}
		if step.Input.Template.Warmup != "" {
			if err = service.setSupplyState(&step, supplyState{
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
		if err = service.setSupplyState(&step, supplyState{
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
			return loom.Fail(loom.Conflict, "build source network/readiness verification failed")
		}
		if err = service.opts.Guest.Activate(service.ctx, source.Input.Resource, Activation{
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
		if err = service.setSupplyState(&step, supplyState{
			Phase: "prepared",
		}); err != nil {
			return err
		}
		if err = service.fault("build_prepared"); err != nil {
			return err
		}
	}
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "capture"); errors.Is(lookup, sql.ErrNoRows) {
		if step.State.Phase != "prepared" {
			return fmt.Errorf("build capture outside prepared phase")
		}
		parent := &supplyAdmission{
			Parent: Operation{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "capture",
		}
		if _, err = service.admitCapture(service.ctx, CaptureRequest{
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
	capture, err := loadCapture(service.ctx, service.store.db, captured.ChildID)
	if err != nil {
		return err
	}
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		parent := &supplyAdmission{
			Parent: Operation{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.admitRelease(service.ctx, ReleaseRequest{
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
		if err = service.setSupplyState(&step, supplyState{
			Phase:      "retired",
			SnapshotID: capture.Input.Snapshot.ID,
		}); err != nil {
			return err
		}
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
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
	if err = commitPublications(tx); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) runSupply(step supplyStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.failSupply(step))
		}
	}()
	if terminal(step.Status) {
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
