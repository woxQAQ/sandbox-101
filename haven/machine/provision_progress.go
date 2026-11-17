package machine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func (service *Provisioning) markPhase(step *ResourceStep, phase string) error {
	next := step.State
	next.Phase = phase
	if step.Action == "cold_start" && phase == "prepared" {
		identity, err := loom.InspectWritableDisk(step.Input.Resource)
		if err != nil {
			return err
		}
		next.DiskIdentity = &identity
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, _, err = service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, *step); err != nil {
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
	if (step.Action == "cold_start" || step.Action == "clone" || step.Action == "restore") && (phase == "prepared" || phase == "launch_intent") {
		instance := ""
		if phase == "launch_intent" {
			instance = step.Input.Resource.Instance
		}
		if err = store.RequireOne(dbgen.New(tx).RecordCreatedInstance(service.ctx, dbgen.RecordCreatedInstanceParams{
			ID:              step.Candidate,
			CurrentInstance: instance,
			OperationID: sql.NullString{
				String: step.OperationID,
				Valid:  true,
			},
		})); err != nil {
			return err
		}
	}
	if step.Action == "cold_start" && phase == "prepared" {
		previous := RetainedVersion{
			Resource:     step.Input.Resource,
			DiskIdentity: step.State.DiskIdentity,
		}
		version := RetainedVersion{
			Resource:     step.Input.Resource,
			DiskIdentity: next.DiskIdentity,
		}
		if err = store.RequireOne(dbgen.New(tx).ReplaceVersionBody(service.ctx, dbgen.ReplaceVersionBodyParams{
			ID:           step.OperationID,
			VmID:         step.Candidate,
			OperationID:  step.OperationID,
			PreviousBody: operation.Encode(previous),
			NewBody:      operation.Encode(version),
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

// Runtime failures fix the original public failure but retain the candidate,
// quota, reservation and dependency. SQL/inspection failures never call this
// path: they leave the original unfinished step available for inspection/replay.
func (service *Provisioning) failStep(step ResourceStep, cause error) error {
	if service.ctx.Err() != nil {
		return service.ctx.Err()
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return errors.Join(cause, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadResourceStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return errors.Join(cause, err)
	}
	if operation.Terminal(current.Status) {
		return cause
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("failed step ownership changed")
	}
	next := step.State
	if step.Input.Restore != nil {
		next.RestoreBlocked = true
	}
	next.Error = "resource effect failed; responsibility retained for inspection"
	if err = service.handoff.BlockActivation(service.ctx, tx, step.OperationID); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     step.Key,
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return errors.Join(cause, err)
	}
	var result operation.Result
	status, body, err := operation.New(tx).ResultState(service.ctx, step.OperationID)
	if err != nil {
		return errors.Join(cause, err)
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return errors.Join(cause, err)
	}
	if !operation.Terminal(status) {
		result.Error = next.Error
		if err = operation.New(tx).FailResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
			return errors.Join(cause, err)
		}
	}
	if err = store.RequireOne(dbgen.New(tx).InsertBlockedCleanup(service.ctx, dbgen.InsertBlockedCleanupParams{
		ID:           "blocked:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "vm",
		ResourceID:   step.Candidate,
		Body:         operation.Encode(step.Input),
	})); err != nil {
		return errors.Join(cause, err)
	}
	if err = service.committer.Commit(tx); err != nil {
		return errors.Join(cause, err)
	}
	return errors.Join(cause, service.store.Checkpoint())
}
