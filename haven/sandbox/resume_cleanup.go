package sandbox

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (service *Lifecycle) finishResumeFailure(step LifecycleStep) error {
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
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("resume failure intent changed")
	}

	sandboxRecord, err := LoadSandbox(service.ctx, tx, step.Input.Sandbox.ID, step.Input.OwnerTenantID)
	if err != nil {
		return err
	}
	if sandboxRecord.State != "paused" || sandboxRecord.SnapshotID != step.Input.Sandbox.SnapshotID || sandboxRecord.VMID != "" {
		return fmt.Errorf("%w: failed resume has no trusted paused destination", errs.ErrManualRecovery)
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(service.ctx, dbgen.DeleteFenceParams{
		SandboxID:   sandboxRecord.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     "lifecycle",
		InputHash:   step.Hash,
		Body: operation.Encode(LifecycleState{
			Phase:      "failed",
			SnapshotID: sandboxRecord.SnapshotID,
		}),
	})); err != nil {
		return err
	}
	status, body, err := operations.ResultState(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "resume failed; sleep snapshot retained"
		if err = operations.FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
			return err
		}
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (service *Lifecycle) compensateResumeCandidate(step LifecycleStep, candidate machine.ResourceStep) error {
	role, kind := "discard", "discard"
	if candidate.Status == "succeeded" {
		role = "compensate"
		kind = "release"
	}
	_, _, err := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role)
	if errors.Is(err, sql.ErrNoRows) {
		admission := &LifecycleAdmission{
			History: service.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  role,
		}
		if kind == "discard" {
			_, err = service.deletion.AdmitDiscardForParent(service.ctx, machine.DiscardRequest{
				TenantID:          step.Input.OwnerTenantID,
				Key:               step.OperationID + ":discard",
				SandboxID:         step.Input.Sandbox.ID,
				FailedOperationID: candidate.OperationID,
				ExpectedInstance:  candidate.Input.Resource.Instance,
			}, admission)
		} else {
			_, err = service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
				TenantID:  step.Input.OwnerTenantID,
				Key:       step.OperationID + ":compensate",
				SandboxID: step.Input.Sandbox.ID,
			}, admission)
		}
	}
	if err != nil {
		return err
	}
	if _, err = service.runResourceLink(step.OperationID, role); err != nil {
		return err
	}
	return service.finishResumeFailure(step)
}
