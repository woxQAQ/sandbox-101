package machine

import (
	"database/sql"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func (service *VMDeletion) markRelease(step *ResourceStep, phase string) error {
	next := step.State
	next.Phase = phase
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, err = service.releaseProof(tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody:      operation.Encode(next),
	})); err != nil {
		return err
	}
	if phase == "stopped" {
		currentMachine, _, _, err := LoadVM(service.ctx, tx, *step)
		if err != nil {
			return err
		}
		currentMachine.AgentReady, currentMachine.Verified, currentMachine.Observation = false, true, nil
		if err = store.RequireOne(dbgen.New(tx).RecordStopped(service.ctx, dbgen.RecordStoppedParams{
			ID:              step.Candidate,
			CurrentInstance: step.ExpectedInstance,
			OperationID: sql.NullString{
				String: step.OperationID,
				Valid:  true,
			},
			Body: operation.Encode(currentMachine),
		})); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("step_" + phase)
}
