package haven

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
)

func (service *Service) setWarmClaimPhase(step *warmClaimStep, phase string) error {
	if err := requireWrite(service.store.db.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='warm_claim' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(warmClaimState{
		Phase: phase,
	}), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	step.State.Phase = phase
	step.Status = "running"
	return service.store.checkpoint()
}

func (service *Service) seedFallbackActivation(step warmClaimStep, source resourceStep) error {
	guard := service.vmLock("configuration:" + step.Input.Pool.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	latest, err := loadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if latest.Input.Attempt == "golden" {
		return nil
	}
	current, err := loadWarmClaim(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.State.Phase != "warm_retired" || latest.Status != "failed" || latest.Input.Attempt != "warm" {
		return fmt.Errorf("%w: fallback activation no longer owns warm retirement", ErrConflict)
	}
	var fence string
	if err = tx.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.Pool.SandboxID).Scan(&fence); err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", ErrConflict)
	}
	if source.Status != "succeeded" || source.State.Witness == "" || source.Input.SandboxID != step.Input.Pool.SandboxID || source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != step.Input.Restore.Snapshot.ID {
		return fmt.Errorf("fallback lacks successful frozen clone witness")
	}
	input := activationInput{
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
	payload := encode(input)
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO operation_steps VALUES(?,'activation:golden','activate',?,?,?,?,'pending',?)", step.OperationID, digest(payload), payload, source.Candidate, source.State.Witness, encode("admitted"))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) runWarmClaim(step warmClaimStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, ErrInterrupted) && service.ctx.Err() == nil {
			runErr = errors.Join(runErr, service.failWarmClaim(step))
		}
	}()
	if terminal(step.Status) {
		return nil
	}
	if step.State.Phase == "selected" {
		lock := service.vmLock(step.Input.Pool.VMID)
		lock.Lock()
		err := service.continueActivation(step.OperationID)
		lock.Unlock()
		activation, lookup := loadActivation(service.ctx, service.store.db, step.OperationID)
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
		if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "warm_release"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &warmClaimAdmission{
				ParentID: step.OperationID,
				Input:    step.Input,
				Role:     "warm_release",
			}
			if _, err := service.admitRelease(service.ctx, ReleaseRequest{
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
	if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "fallback"); errors.Is(lookup, sql.ErrNoRows) {
		template := step.Input.Pool.Template
		image, err := loadImage(service.ctx, service.store.db, template.Config.Image)
		if err != nil {
			return err
		}
		if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
			return err
		}
		source, err := service.loadRestoreSource(service.ctx, service.store.db, template.TenantID, template.SnapshotID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(source, step.Input.Restore) {
			return fmt.Errorf("%w: frozen fallback source publication changed", ErrConflict)
		}
		if err = verifyRestoreSourceFiles(source); err != nil {
			return err
		}
		sandbox, err := loadSandbox(service.ctx, service.store.db, step.Input.Pool.SandboxID, template.TenantID)
		if err != nil {
			return err
		}
		request := CloneRequest{
			TenantID:   template.TenantID,
			Key:        step.OperationID + ":fallback",
			SnapshotID: template.SnapshotID,
		}
		frozen := encode(struct {
			Action  string
			Request CloneRequest
		}{"clone", request})
		parent := &warmClaimAdmission{
			ParentID: step.OperationID,
			Input:    step.Input,
			Role:     "fallback",
		}
		identity := sandbox.Identity
		identity.ProvisionSource = "golden"
		if _, err = service.admitProvision(service.ctx, template.TenantID, request.Key, frozen, digest(frozen), template, image, &source, nil, &provisionTarget{
			Identity:      &identity,
			Sandbox:       sandbox,
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
	source, err := loadStep(service.ctx, service.store.db, fallback.ChildID)
	if err != nil {
		return err
	}
	if err = service.seedFallbackActivation(step, source); err != nil {
		return err
	}
	lock := service.vmLock(source.Candidate)
	lock.Lock()
	defer lock.Unlock()
	return service.continueActivation(step.OperationID)
}
