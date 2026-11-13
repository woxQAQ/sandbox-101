package haven

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// blockActivation retains the configuration fence and all resource charges. A
// superseded worker records only its own failure and cannot revoke a successor.
func (service *Service) blockActivation(step activationStep, source resourceStep) error {
	guard := service.vmLock("configuration:" + step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	return service.blockActivationGuarded(step, source)
}

func (service *Service) blockActivationGuarded(step activationStep, source resourceStep) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if current, err := loadActivation(ctx, service.store.db, step.OperationID); err != nil {
		return err
	} else if current.Key != step.Key || current.Hash != step.Hash || terminal(current.Status) {
		return nil
	}
	var cleanupErr error
	var fence string
	queryErr := service.store.db.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", step.Input.SandboxID).Scan(&fence)
	if queryErr == nil && fence == step.OperationID {
		cleanupErr = service.opts.Backend.Network(ctx, source.Input.Resource, false)
		cleanupErr = errors.Join(cleanupErr, service.revokeSandbox(step.Input.SandboxID))
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadActivation(ctx, tx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if terminal(current.Status) {
		return cleanupErr
	}
	if err = requireWrite(tx.ExecContext(ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running')", encode("blocked"), step.OperationID, step.Key, step.Hash)); err != nil {
		return errors.Join(cleanupErr, err)
	}
	var status string
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT status,body FROM operations WHERE id=?", step.OperationID).Scan(&status, &body); err != nil {
		return errors.Join(cleanupErr, err)
	}
	if !terminal(status) && !step.Input.AllowFallback {
		var result operationResult
		if err = json.Unmarshal(body, &result); err != nil {
			return errors.Join(cleanupErr, err)
		}
		result.Error = "sandbox activation could not establish delivery"
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
			return errors.Join(cleanupErr, err)
		}
	}
	if queryErr == nil && fence == step.OperationID {
		sandbox, err := loadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
		if err != nil {
			return errors.Join(cleanupErr, err)
		}
		if sandbox.SourceOperation == step.Input.ResourceOperation && sandbox.Instance == step.Input.ExpectedInstance {
			sandbox.LastError = "sandbox activation could not establish delivery"
			if err = requireWrite(tx.ExecContext(ctx, "UPDATE sandboxes SET body=? WHERE id=? AND tenant=?", encode(sandbox), sandbox.ID, sandbox.TenantID)); err != nil {
				return errors.Join(cleanupErr, err)
			}
		}
	}
	if err = commitPublications(tx); err != nil {
		return errors.Join(cleanupErr, err)
	}
	return errors.Join(cleanupErr, service.store.checkpoint())
}
