package sandbox

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// blockActivation retains the configuration fence and all resource charges. A
// superseded worker records only its own failure and cannot revoke a successor.
func (service *Activations) blockActivation(step ActivationStep, source machine.ResourceStep) error {
	guard := service.guards.Configuration(step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	return service.BlockGuarded(step, source)
}

// BlockGuarded borrows the configuration guard already held by the caller.
func (service *Activations) BlockGuarded(step ActivationStep, source machine.ResourceStep) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if current, err := LoadActivation(ctx, service.store.Database, step.OperationID); err != nil {
		return err
	} else if current.Key != step.Key || current.Hash != step.Hash || operation.Terminal(current.Status) {
		return nil
	}
	var cleanupErr error
	fence, queryErr := LifecycleFence(ctx, service.store.Database, step.Input.SandboxID)
	if queryErr == nil && fence == step.OperationID {
		cleanupErr = service.backend.Network(ctx, source.Input.Resource, false)
		cleanupErr = errors.Join(cleanupErr, service.RevokeSandbox(step.Input.SandboxID))
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(ctx, tx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if operation.Terminal(current.Status) {
		return cleanupErr
	}
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     step.Key,
		InputHash:   step.Hash,
		Body:        operation.Encode("blocked"),
	})); err != nil {
		return errors.Join(cleanupErr, err)
	}
	status, body, err := operation.New(tx).ResultState(ctx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if !operation.Terminal(status) && !step.Input.AllowFallback {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return errors.Join(cleanupErr, err)
		}
		result.Error = "sandbox activation could not establish delivery"
		if err = operation.New(tx).FailResult(ctx, step.OperationID, operation.Encode(result)); err != nil {
			return errors.Join(cleanupErr, err)
		}
	}
	if queryErr == nil && fence == step.OperationID {
		record, err := LoadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
		if err != nil {
			return errors.Join(cleanupErr, err)
		}
		if record.SourceOperation == step.Input.ResourceOperation && record.Instance == step.Input.ExpectedInstance {
			record.LastError = "sandbox activation could not establish delivery"
			if err = store.RequireOne(dbgen.New(tx).UpdateSandboxBody(ctx, dbgen.UpdateSandboxBodyParams{
				Body:   operation.Encode(record),
				ID:     record.ID,
				Tenant: record.TenantID,
			})); err != nil {
				return errors.Join(cleanupErr, err)
			}
		}
	}
	if err = service.committer.Commit(tx); err != nil {
		return errors.Join(cleanupErr, err)
	}
	return errors.Join(cleanupErr, service.store.Checkpoint())
}
