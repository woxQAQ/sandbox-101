package sandbox

import (
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// saveConfigurationIntent separates accepted revocation from publication of new
// settings, so a rejected settings write leaves the durable fence and old policy.
func (service *Activations) saveConfigurationIntent(step *ActivationStep) error {
	guard := service.guards.Configuration(step.Input.SandboxID)
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
	current, err := LoadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *step) || step.Phase != "admitted" || step.Input.PreviousSettings == nil {
		return fmt.Errorf("configuration accepted revocation intent changed")
	}
	if _, _, err = VerifyActivation(service.proofs, service.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(service.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    step.Input.SandboxID,
		PreviousBody: operation.Encode(step.Input.PreviousSettings),
		NextBody:     operation.Encode(step.Input.Settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(service.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("configuration_saved"),
	})); err != nil {
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
	return service.store.Checkpoint()
}
