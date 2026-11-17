package supply

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (s *Supply) failSupply(step SupplyStep) error {
	s.admission.Lock()
	defer s.admission.Unlock()
	tx, err := s.store.Database.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupply(s.ctx, tx, step.OperationID)
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
	pending, err := dbgen.New(tx).CountLiveResourceLinks(s.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if pending != 0 {
		return nil
	}
	body, err := operations.LiveBody(s.ctx, step.OperationID)
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
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(s.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "supply",
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	if err = operations.FailResult(s.ctx, step.OperationID, operation.Encode(result)); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(s.ctx, dbgen.InsertCleanupIntentParams{
		ID:           "supply:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "supply",
		ResourceID:   step.Input.SandboxID,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return err
	}
	if step.Input.Command.Action == "refill" {
		if err = store.RequireOne(dbgen.New(tx).MarkPendingCleanup(s.ctx, step.OperationID)); err != nil {
			return err
		}
	}
	if err = s.committer.Commit(tx); err != nil {
		return err
	}
	return s.store.Checkpoint()
}
