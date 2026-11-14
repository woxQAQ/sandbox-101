package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) AdmitConfigure(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if principal.TenantID == "" || principal.SandboxID != "" || command.Action != "configure" || command.SandboxID == "" || key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: configure requires tenant authority, sandbox and key", ErrInvalid)
	}
	_, secrets, _, err := splitEnv(command.Env)
	if err != nil {
		return empty, err
	}
	requestGuard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := service.store.commandMAC(principal, command)
	retry := &claimAdmission{
		MAC:     mac,
		Secrets: secrets,
	}
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, retry); found || err != nil {
		return operation, err
	}
	guard := service.vmLock("configuration:" + command.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	operation, resource, paused, admissionErr := service.admitConfiguration(ctx, principal, key, command, mac)
	if operation.ID == "" || resource.VMID == "" {
		return operation, admissionErr
	}
	if paused {
		service.mu.Lock()
		service.secrets[command.SandboxID] = secrets
		service.mu.Unlock()
		return operation, admissionErr
	}
	service.closeConnections(command.SandboxID)
	// Durable revocation precedes any external call. Safety teardown has its own
	// bounded context, independent of the submitting connection's cancellation.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = errors.Join(admissionErr, service.opts.Backend.Network(cleanupCtx, resource, false), service.revokeSandbox(command.SandboxID))
	if err != nil {
		step, lookup := loadActivation(cleanupCtx, service.store.db, operation.ID)
		if lookup == nil {
			source, sourceErr := loadStep(cleanupCtx, service.store.db, step.Input.ResourceOperation)
			if sourceErr == nil && source.Hash == step.Input.ResourceHash {
				err = errors.Join(err, service.blockActivationGuarded(step, source))
			} else {
				err = errors.Join(err, sourceErr)
			}
		} else {
			err = errors.Join(err, lookup)
		}
		if current, lookup := service.GetOperation(cleanupCtx, principal.TenantID, operation.ID); lookup == nil {
			operation = current
		}
		return operation, err
	}
	service.mu.Lock()
	service.opSecrets[operation.ID] = secrets
	service.mu.Unlock()
	if err = service.fault("configure_admitted"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func (service *Service) admitConfiguration(ctx context.Context, principal Principal, key string, command Command, mac string) (Operation, loom.Resource, bool, error) {
	var empty Operation
	var noResource loom.Resource
	service.admission.Lock()
	defer service.admission.Unlock()
	service.mu.Lock()
	closing := service.closing || service.store.reviewed
	service.mu.Unlock()
	if closing {
		return empty, noResource, false, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, noResource, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	sandbox, err := ownedSandbox(ctx, tx, principal, command.SandboxID)
	if err != nil {
		return empty, noResource, false, err
	}
	if sandbox.State != "running" && sandbox.State != "paused" {
		return empty, noResource, false, fmt.Errorf("%w: sandbox cannot configure in current state", ErrConflict)
	}
	var priorFence string
	fenceErr := tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&priorFence)
	if fenceErr == nil {
		if lifecycle, lookup := loadLifecycle(ctx, tx, priorFence); lookup == nil && !terminal(lifecycle.Status) {
			return empty, noResource, false, fmt.Errorf("%w: sandbox lifecycle is in progress", ErrConflict)
		} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return empty, noResource, false, lookup
		}
	} else if !errors.Is(fenceErr, sql.ErrNoRows) {
		return empty, noResource, false, fenceErr
	}
	old, err := loadSettings(ctx, tx, sandbox.ID)
	if err != nil {
		return empty, noResource, false, err
	}
	if command.ObservedGeneration != "" && command.ObservedGeneration != old.Generation {
		return empty, noResource, false, fmt.Errorf("%w: observed configuration was superseded", ErrConflict)
	}
	desired := command
	if desired.Policy.Mode == "" {
		desired.Policy = old.Policy
	}
	desired.TTLSeconds = old.TTLSeconds
	desired.OnExpire = old.OnExpire
	desired.AutoRenew = old.AutoRenew
	settings, err := service.prepareClaim(desired)
	if err != nil {
		return empty, noResource, false, err
	}
	settings.Settings.Deadline = old.Deadline
	settings.Settings.PausedUntil = old.PausedUntil
	source, err := loadStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, noResource, false, err
	}
	resource := source.Input.Resource
	if sandbox.State != "paused" {
		if service.opts.Guest == nil {
			return empty, noResource, false, fmt.Errorf("%w: guest activation effects unavailable", ErrIncompatible)
		}
		if source.Input.TenantID != sandbox.TenantID || source.Input.SandboxID != sandbox.ID || source.Status != "succeeded" || source.State.Witness != sandbox.Instance || source.Candidate != sandbox.VMID {
			return empty, noResource, false, fmt.Errorf("%w: configuration lacks original resource delivery witness", ErrConflict)
		}
	}
	frozen := encode(struct {
		Action  string
		Request Command
	}{"configure", sanitizedCommand(command)})
	hash := digest(frozen)
	if cached, found, err := cachedOperation(ctx, tx, principal.TenantID, key, hash); found || err != nil {
		if err == nil {
			var prior string
			err = tx.QueryRowContext(ctx, "SELECT request_mac FROM operation_credentials WHERE operation_id=?", cached.ID).Scan(&prior)
			if err == nil && prior != mac {
				err = fmt.Errorf("%w: idempotency key has different parameters", ErrConflict)
			}
		}
		return cached, noResource, false, err
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			SandboxID:     sandbox.ID,
			VMID:          sandbox.VMID,
			ResourceReady: true,
		},
	}
	settings.Settings.Generation = operation.ID
	input := activationInput{
		RequestTenantID:   principal.TenantID,
		TenantID:          sandbox.TenantID,
		SandboxID:         sandbox.ID,
		ResourceOperation: source.OperationID,
		ResourceHash:      source.Hash,
		ExpectedInstance:  sandbox.Instance,
		Settings:          settings.Settings,
		ResetIdentity:     false,
	}
	if sandbox.State != "paused" {
		input.PreviousSettings = &old
	}
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			operation.ID,
			"tenant",
			principal.TenantID,
			key,
			hash,
			frozen,
			"pending",
			encode(operation.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			operation.ID,
			mac,
		}},
		{"INSERT INTO configuration_fences VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id", []any{
			sandbox.ID,
			operation.ID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, noResource, false, err
		}
	}
	paused := sandbox.State == "paused"
	if paused {
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{
				encode(settings.Settings),
				sandbox.ID,
				encode(old),
			}},
			{"INSERT INTO operation_steps VALUES(?,'configuration','configure_paused',?,?,?,?,'succeeded',?)", []any{
				operation.ID,
				digest(payload),
				payload,
				sandbox.ID,
				sandbox.Instance,
				encode("complete"),
			}},
			{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{
				sandbox.ID,
				operation.ID,
			}},
			{"UPDATE operations SET status='succeeded' WHERE id=? AND status='pending'", []any{
				operation.ID,
			}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return empty, noResource, false, err
			}
		}
		operation.Status = "succeeded"
	} else {
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_steps VALUES(?,'activation','activate',?,?,?,?,'pending',?)", operation.ID, digest(payload), payload, sandbox.VMID, sandbox.Instance, encode("admitted"))); err != nil {
			return empty, noResource, false, err
		}
	}
	if err = service.fault("configure_admission_before_commit"); err != nil {
		return empty, noResource, false, err
	}
	if err = commitPublications(tx); err != nil {
		return empty, noResource, false, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, resource, paused, err
	}
	return operation, resource, paused, nil
}
