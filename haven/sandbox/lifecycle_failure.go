package sandbox

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// failLifecycle commits failure only from a durable child terminal result.
// An effect error with a pending child retains an unresolved accepted intent.
func (service *Lifecycle) failLifecycle(step LifecycleStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadLifecycle(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("%w: lifecycle failure input changed", errs.ErrManualRecovery)
	}
	operations := operation.New(tx)
	failed, err := dbgen.New(tx).CountFailedLifecycleChildren(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if failed == 0 {
		return nil
	}
	fence, err := LifecycleFence(service.ctx, tx, step.Input.Sandbox.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	if fence != step.OperationID {
		return nil
	}
	status, body, err := operations.ResultState(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if status == "succeeded" {
		return fmt.Errorf("%w: successful lifecycle cannot adopt failed child", errs.ErrManualRecovery)
	}
	next := current.State
	next.Phase = "failed"
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "lifecycle",
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "sandbox workflow failed; resource responsibility retained"
		if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
			return err
		}
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
