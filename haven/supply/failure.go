package supply

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) failSupply(step SupplyStep) error {
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
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash {
		return fmt.Errorf("supply failure input changed")
	}
	operations := operation.New(tx)
	pending, err := dbgen.New(tx).CountLiveResourceLinks(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	body, err := operations.LiveBody(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	var result operation.Result
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.Error = "supply workflow failed; owned resources retained"
	next := current.State
	next.Phase = "failed"
	next.Error = result.Error
	next.FinishedAt = time.Now().UTC()
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "supply",
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "supply:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "supply",
		ResourceID:   step.Input.SandboxID,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return err
	}
	if step.Input.Command.Action == "refill" {
		if err = store.RequireOne(dbgen.New(tx).MarkPendingCleanup(service.ctx, step.OperationID)); err != nil {
			return err
		}
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
