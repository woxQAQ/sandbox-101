package supply

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (s *Supply) setSupplyState(step *SupplyStep, next SupplyState) error {
	if err := store.RequireOne(dbgen.New(s.store.Database).AdvanceLiveStep(s.ctx, dbgen.AdvanceLiveStepParams{
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
	return s.store.Checkpoint()
}

func (s *Supply) supplyCandidate(step SupplyStep, source machine.ResourceStep) error {
	s.storeWriteMu.Lock()
	defer s.storeWriteMu.Unlock()
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
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply current intent changed")
	}
	intent, err := dbgen.New(tx).GetIntent(s.ctx, step.Input.Template.ID)
	if err != nil {
		return err
	}
	if intent.TenantID != step.Input.Template.TenantID || intent.OperationID != step.OperationID {
		return fmt.Errorf("%w: template build was superseded", errs.ErrConflict)
	}
	if err = s.proofs.VerifyGeneration(s.ctx, tx, source); err != nil {
		return err
	}
	vm, instance, ownerOperation, err := machine.LoadVM(s.ctx, tx, source)
	if err != nil {
		return err
	}
	destination, err := s.ownership.ReadSandbox(s.ctx, tx, step.Input.SandboxID, step.Input.Template.TenantID)
	if err != nil {
		return err
	}
	if instance != source.State.Witness || ownerOperation != "" || !vm.Verified || !vm.AgentReady || vm.Deleting || destination.State != "provisioning" || destination.SourceOperation != source.OperationID || destination.Instance != instance {
		return fmt.Errorf("%w: build source instance ownership changed", errs.ErrConflict)
	}
	return nil
}

func (s *Supply) runBuild(step SupplyStep) error {
	linked, err := s.runResourceLink(step.OperationID, "cold")
	if err != nil {
		return err
	}
	source, err := machine.LoadResourceStep(s.ctx, s.store.Database, linked.ChildID)
	if err != nil {
		return err
	}
	if step.State.Phase == "warmup_inflight" {
		return fmt.Errorf("%w: template warmup result unknown; original resource retained", errs.ErrUnknown)
	}
	if step.State.Phase == "admitted" {
		if err = s.supplyCandidate(step, source); err != nil {
			return err
		}
		if s.opts.Guest == nil {
			return fmt.Errorf("%w: guest effects unavailable", errs.ErrInvalid)
		}
		if err = s.opts.Guest.Info(s.ctx, source.Input.Resource); err != nil {
			return err
		}
		if step.Input.Template.Warmup != "" {
			if err = s.setSupplyState(&step, SupplyState{
				Phase: "warmup_inflight",
			}); err != nil {
				return err
			}
			if err = s.fault("build_warmup_intent"); err != nil {
				return err
			}
			if err = s.opts.Guest.Exec(s.ctx, source.Input.Resource, "haven-op:"+step.OperationID+":template-warmup", step.Input.Template.Warmup); err != nil {
				return err
			}
		}
		if err = s.setSupplyState(&step, SupplyState{
			Phase: "warmup_done",
		}); err != nil {
			return err
		}
		if err = s.fault("build_warmup_done"); err != nil {
			return err
		}
	}
	if step.State.Phase == "warmup_done" {
		if err = s.supplyCandidate(step, source); err != nil {
			return err
		}
		if err = s.opts.Backend.SetNetworkEnabled(s.ctx, source.Input.Resource, false); err != nil {
			return err
		}
		observed, err := s.opts.Backend.Inspect(s.ctx, source.Input.Resource)
		if err != nil {
			return err
		}
		if !observed.Alive || !observed.Booted || observed.Network {
			return fmt.Errorf("%w: build source network/readiness verification failed", errs.ErrConflict)
		}
		if err = s.opts.Guest.Activate(s.ctx, source.Input.Resource, guest.Activation{
			SandboxID:     "template-" + step.Input.Template.ID,
			Hostname:      "sandbox-template",
			ResetIdentity: true,
			Env:           map[string]string{},
			Network:       "none",
		}); err != nil {
			return err
		}
		if err = s.opts.Guest.PrepareTemplate(s.ctx, source.Input.Resource); err != nil {
			return err
		}
		if err = s.setSupplyState(&step, SupplyState{
			Phase: "prepared",
		}); err != nil {
			return err
		}
		if err = s.fault("build_prepared"); err != nil {
			return err
		}
	}
	if _, _, lookup := machine.LoadResourceLink(s.ctx, s.store.Database, step.OperationID, "capture"); errors.Is(lookup, sql.ErrNoRows) {
		if step.State.Phase != "prepared" {
			return fmt.Errorf("build capture outside prepared phase")
		}
		parent := &SupplyAdmission{
			Parent: operation.State{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "capture",
		}
		captures := s.captures
		if _, err = captures.AdmitForParent(s.ctx, machine.CaptureRequest{
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
	captured, err := s.runResourceLink(step.OperationID, "capture")
	if err != nil {
		return err
	}
	capture, err := machine.LoadCapture(s.ctx, s.store.Database, captured.ChildID)
	if err != nil {
		return err
	}
	if _, _, lookup := machine.LoadResourceLink(s.ctx, s.store.Database, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		parent := &SupplyAdmission{
			Parent: operation.State{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = s.deletion.AdmitReleaseForParent(s.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.Template.TenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.SandboxID,
		}, parent); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	if _, err = s.runResourceLink(step.OperationID, "release"); err != nil {
		return err
	}
	if step.State.Phase != "retired" {
		if err = s.setSupplyState(&step, SupplyState{
			Phase:      "retired",
			SnapshotID: capture.Input.Snapshot.ID,
		}); err != nil {
			return err
		}
	}
	s.storeWriteMu.Lock()
	defer s.storeWriteMu.Unlock()
	tx, err := s.store.Database.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	result, err := s.publishTemplate(s.ctx, tx, step, capture.Input.Snapshot.ID)
	if err != nil {
		return err
	}
	if err = s.fault("template_publication_before_commit"); err != nil {
		return err
	}
	if err = s.committer.Complete(tx, operation.Completion{
		ID:     step.OperationID,
		Result: result,
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.store.Checkpoint()
}

func (s *Supply) runSupply(step SupplyStep) (runErr error) {
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && s.ctx.Err() == nil {
			runErr = errors.Join(runErr, s.failSupply(step))
		}
	}()
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.Input.Command.Action == "refill" {
		return s.runRefill(step)
	}
	if step.Input.Command.Action == "template_build" {
		return s.runBuild(step)
	}
	return fmt.Errorf("unsupported supply workflow")
}

func (s *Supply) failSupply(step SupplyStep) error {
	s.storeWriteMu.Lock()
	defer s.storeWriteMu.Unlock()
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
	if err = s.committer.Complete(tx, operation.Completion{
		ID:     step.OperationID,
		Result: result,
		Failed: true,
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.store.Checkpoint()
}
