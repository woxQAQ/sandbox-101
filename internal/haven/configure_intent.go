package haven

import (
	"fmt"
	"reflect"
)

// saveConfigurationIntent separates accepted revocation from publication of new
// settings, so a rejected settings write leaves the durable fence and old policy.
func (service *Service) saveConfigurationIntent(step *activationStep) error {
	guard := service.vmLock("configuration:" + step.Input.SandboxID)
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
	current, err := loadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *step) || step.Phase != "admitted" || step.Input.PreviousSettings == nil {
		return fmt.Errorf("configuration accepted revocation intent changed")
	}
	if _, _, err = service.activationProof(service.ctx, tx, *step); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", encode(step.Input.Settings), step.Input.SandboxID, encode(step.Input.PreviousSettings))); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND body=? AND status='pending'", encode("configuration_saved"), step.OperationID, step.Key, step.Hash, encode(step.Phase))); err != nil {
		return err
	}
	if err = service.fault("configure_settings_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "configuration_saved"
	step.Status = "running"
	return service.store.checkpoint()
}
