package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// configurationRequest freezes the original command and credential MAC before
// preparing its replacement settings inside the admission transaction.
type configurationRequest struct {
	Command operation.Intent
	MAC     string
}

func (sb *Sandboxes) AdmitConfigure(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	var empty operation.State
	if principal.TenantID == "" || principal.SandboxID != "" || command.Action != "configure" || command.SandboxID == "" || key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: configure requires tenant authority, sandbox and key", errs.ErrInvalid)
	}
	_, secrets, _, err := SplitEnv(command.Env)
	if err != nil {
		return empty, err
	}
	releaseKey := sb.tenantIdempotencyLocks.Lock(principal.TenantID, key)
	defer releaseKey()
	mac := sb.identity.CommandMAC(principal, command)
	retry := &ClaimAdmission{
		MAC:     mac,
		Secrets: secrets,
	}
	if accepted, found, err := sb.existing(ctx, principal, key, retry); found || err != nil {
		return accepted, err
	}
	unlock := sb.sandboxConfigLocks.Lock(command.SandboxID)
	defer unlock()
	accepted, resource, paused, admissionErr := sb.admitConfiguration(ctx, principal, key, configurationRequest{
		Command: command,
		MAC:     mac,
	})
	if accepted.ID == "" || resource.VMID == "" {
		return accepted, admissionErr
	}
	if paused {
		sb.registry.SetSandboxSecrets(command.SandboxID, secrets)
		return accepted, admissionErr
	}
	sb.registry.CloseSandboxConnections(command.SandboxID)
	// Durable revocation precedes any external call. Safety teardown has its own
	// bounded context, independent of the submitting connection's cancellation.
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = errors.Join(admissionErr, sb.backend.SetNetworkEnabled(cleanupCtx, resource, false), sb.activations.RevokeSandbox(command.SandboxID))
	if err != nil {
		step, lookup := LoadActivation(cleanupCtx, sb.store.Database, accepted.ID)
		if lookup == nil {
			source, sourceErr := machine.LoadResourceStep(cleanupCtx, sb.store.Database, step.Input.ResourceOperation)
			if sourceErr == nil && source.Hash == step.Input.ResourceHash {
				err = errors.Join(err, sb.activations.BlockGuarded(step, source))
			} else {
				err = errors.Join(err, sourceErr)
			}
		} else {
			err = errors.Join(err, lookup)
		}
		if current, lookup := sb.operations.GetOperation(cleanupCtx, principal.TenantID, accepted.ID); lookup == nil {
			accepted = current
		}
		return accepted, err
	}
	sb.registry.SetOperationSecrets(accepted.ID, secrets)
	if err = sb.fault("configure_admitted"); err != nil {
		return accepted, err
	}
	sb.dispatch(accepted.ID)
	return accepted, nil
}

func (sb *Sandboxes) admitConfiguration(ctx context.Context, principal authorization.Principal, key string, request configurationRequest) (operation.State, loom.Resource, bool, error) {
	command, mac := request.Command, request.MAC
	var empty operation.State
	var noResource loom.Resource
	sb.storeWriteMu.Lock()
	defer sb.storeWriteMu.Unlock()
	if err := sb.checkConfigurationOpen(); err != nil {
		return empty, noResource, false, err
	}
	tx, err := sb.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, noResource, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := NewCatalog(tx).OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, noResource, false, err
	}
	if current.State != "running" && current.State != "paused" {
		return empty, noResource, false, fmt.Errorf("%w: sandbox cannot configure in current state", errs.ErrConflict)
	}

	priorFence, fenceErr := LifecycleFence(ctx, tx, current.ID)
	if fenceErr == nil {
		if lifecycle, lookup := LoadLifecycleStep(ctx, tx, priorFence); lookup == nil && !operation.Terminal(lifecycle.Status) {
			return empty, noResource, false, fmt.Errorf("%w: sandbox lifecycle is in progress", errs.ErrConflict)
		} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return empty, noResource, false, lookup
		}
	} else if !errors.Is(fenceErr, sql.ErrNoRows) {
		return empty, noResource, false, fenceErr
	}
	old, err := LoadSettings(ctx, tx, current.ID)
	if err != nil {
		return empty, noResource, false, err
	}
	if command.ObservedGeneration != "" && command.ObservedGeneration != old.Generation {
		return empty, noResource, false, fmt.Errorf("%w: observed configuration was superseded", errs.ErrConflict)
	}
	desired := command
	if desired.Policy.Mode == "" {
		desired.Policy = old.Policy
	}
	desired.TTLSeconds = old.TTLSeconds
	desired.OnExpire = old.OnExpire
	desired.AutoRenew = old.AutoRenew
	settings, err := sb.policy.Prepare(desired)
	if err != nil {
		return empty, noResource, false, err
	}
	settings.Settings.Deadline = old.Deadline
	settings.Settings.PausedUntil = old.PausedUntil
	source, err := machine.LoadResourceStep(ctx, tx, current.SourceOperation)
	if err != nil {
		return empty, noResource, false, err
	}
	resource := source.Input.Resource
	if current.State != "paused" {
		if sb.guest == nil {
			return empty, noResource, false, fmt.Errorf("%w: guest activation effects unavailable", errs.ErrIncompatible)
		}
		if source.Input.TenantID != current.TenantID || source.Input.SandboxID != current.ID || source.Status != "succeeded" || source.State.Witness != current.Instance || source.Candidate != current.VMID {
			return empty, noResource, false, fmt.Errorf("%w: configuration lacks original resource delivery witness", errs.ErrConflict)
		}
	}
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Intent
	}{"configure", operation.SanitizedCommand(command)})
	hash := operation.Digest(frozen)
	operations := operation.New(tx)
	if cached, found, err := operations.Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   principal.TenantID,
	}, key, hash); found || err != nil {
		if err == nil {
			var prior string
			prior, err = operations.CredentialMAC(ctx, cached.ID)
			if err == nil && prior != mac {
				err = fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
			}
		}
		return cached, noResource, false, err
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:     current.ID,
			VMID:          current.VMID,
			ResourceReady: true,
		},
	}
	settings.Settings.Generation = accepted.ID
	input := ActivationInput{
		RequestTenantID:   principal.TenantID,
		TenantID:          current.TenantID,
		SandboxID:         current.ID,
		ResourceOperation: source.OperationID,
		ResourceHash:      source.Hash,
		ExpectedInstance:  current.Instance,
		Settings:          settings.Settings,
		ResetIdentity:     false,
	}
	if current.State != "paused" {
		input.PreviousSettings = &old
	}
	payload := operation.Encode(input)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   principal.TenantID,
		Key:       key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, noResource, false, err
	}
	if err = operations.InsertCredential(ctx, accepted.ID, mac); err != nil {
		return empty, noResource, false, err
	}
	if err = store.RequireOne(dbgen.New(tx).UpsertFence(ctx, dbgen.UpsertFenceParams{
		SandboxID:   current.ID,
		OperationID: accepted.ID,
	})); err != nil {
		return empty, noResource, false, err
	}
	paused := current.State == "paused"
	var outcomes []operation.Outcome
	if paused {
		if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
			SandboxID:    current.ID,
			PreviousBody: operation.Encode(old),
			NextBody:     operation.Encode(settings.Settings),
		})); err != nil {
			return empty, noResource, false, err
		}
		if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
			OperationID:         accepted.ID,
			StepKey:             "configuration",
			Action:              "configure_paused",
			InputHash:           operation.Digest(payload),
			Input:               payload,
			CandidateResourceID: current.ID,
			ExpectedInstance:    current.Instance,
			Status:              "succeeded",
			Body:                operation.Encode("complete"),
		})); err != nil {
			return empty, noResource, false, err
		}
		if err = store.RequireOne(dbgen.New(tx).DeleteFence(ctx, dbgen.DeleteFenceParams{
			SandboxID:   current.ID,
			OperationID: accepted.ID,
		})); err != nil {
			return empty, noResource, false, err
		}
		accepted.Status = "succeeded"
		outcomes = append(outcomes, operation.Outcome{
			ID:          accepted.ID,
			AdoptResult: true,
		})
	} else {
		if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
			OperationID:         accepted.ID,
			StepKey:             "activation",
			Action:              "activate",
			InputHash:           operation.Digest(payload),
			Input:               payload,
			CandidateResourceID: current.VMID,
			ExpectedInstance:    current.Instance,
			Status:              "pending",
			Body:                operation.Encode("admitted"),
		})); err != nil {
			return empty, noResource, false, err
		}
	}
	if err = sb.fault("configure_admission_before_commit"); err != nil {
		return empty, noResource, false, err
	}
	if err = sb.committer.Complete(tx, outcomes...); err != nil {
		return empty, noResource, false, err
	}
	if err = tx.Commit(); err != nil {
		return empty, noResource, false, err
	}
	if err = sb.store.Checkpoint(); err != nil {
		return accepted, resource, paused, err
	}
	return accepted, resource, paused, nil
}

// saveConfigurationIntent separates accepted revocation from publication of new
// settings, so a rejected settings write leaves the durable fence and old policy.
func (a *Activations) saveConfigurationIntent(step *ActivationStep) error {
	unlock := a.sandboxConfigLocks.Lock(step.Input.SandboxID)
	defer unlock()
	a.storeWriteMu.Lock()
	defer a.storeWriteMu.Unlock()
	tx, err := a.store.Database.BeginTx(a.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(a.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, *step) || step.Phase != "admitted" || step.Input.PreviousSettings == nil {
		return fmt.Errorf("configuration accepted revocation intent changed")
	}
	if _, _, err = VerifyActivation(a.proofs, a.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(a.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    step.Input.SandboxID,
		PreviousBody: operation.Encode(step.Input.PreviousSettings),
		NextBody:     operation.Encode(step.Input.Settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(a.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("configuration_saved"),
	})); err != nil {
		return err
	}
	if err = a.fault("configure_settings_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = "configuration_saved"
	step.Status = "running"
	return a.store.Checkpoint()
}
