package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (a *Activations) actualActivation(source machine.ResourceStep) error {
	observation, err := a.backend.Inspect(a.ctx, source.Input.Resource)
	if err != nil {
		return err
	}
	if !observation.Alive || observation.Paused || !observation.Booted {
		return fmt.Errorf("%w: activation runtime readiness unconfirmed", errs.ErrUnknown)
	}
	return nil
}

func (a *Activations) activateVM(vmID, operationID string) error {
	return a.vmLifecycleLocks.With(vmID, func() error {
		return a.activate(vmID, operationID)
	})
}

// activate runs inside the VM lifecycle scope shared with physical provisioning.
func (a *Activations) activate(vmID, acceptedOperation string) (runErr error) {
	if a.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot activate sandbox", errs.ErrManualRecovery)
	}
	step, err := LoadActivation(a.ctx, a.store.Database, acceptedOperation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		return nil
	}
	source, err := machine.LoadResourceStep(a.ctx, a.store.Database, step.Input.ResourceOperation)
	if err != nil {
		return err
	}
	if source.Candidate != vmID || source.Hash != step.Input.ResourceHash || source.State.Witness != step.Input.ExpectedInstance {
		return fmt.Errorf("activation lacks original resource witness")
	}
	defer func() {
		if runErr != nil && !errors.Is(runErr, errs.ErrInterrupted) && a.ctx.Err() == nil {
			runErr = errors.Join(runErr, a.blockActivation(step, source))
		}
	}()
	source, err = a.checkActivation(step)
	if err != nil {
		return err
	}
	if a.guest == nil {
		return fmt.Errorf("%w: guest activation effects unavailable", errs.ErrIncompatible)
	}
	if _, err = a.activationSecrets(step); err != nil {
		return err
	}
	resource := source.Input.Resource
	if err = a.actualActivation(source); err != nil {
		return err
	}
	if err = a.backend.SetNetworkEnabled(a.ctx, resource, false); err != nil {
		return err
	}
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		if err = a.saveConfigurationIntent(&step); err != nil {
			return err
		}
	}
	if step.Phase == "admitted" || step.Phase == "configuration_saved" {
		if err = a.markActivation(&step, "off_intent"); err != nil {
			return err
		}
	}
	if step.Phase == "off_intent" {
		if err = a.backend.SetNetworkEnabled(a.ctx, resource, false); err != nil {
			return err
		}
		observation, err := a.backend.Inspect(a.ctx, resource)
		if err != nil {
			return err
		}
		if observation.Network || !observation.Alive {
			return fmt.Errorf("%w: activation network-off unconfirmed", errs.ErrUnknown)
		}
		if err = a.markActivation(&step, "off"); err != nil {
			return err
		}
	}
	if step.Phase == "off" {
		if err = a.markActivation(&step, "policy_intent"); err != nil {
			return err
		}
	}
	if step.Phase == "policy_intent" {
		var policy *loom.NetworkPolicy
		if step.Input.Settings.Policy.Mode == "direct" {
			policy, err = machine.DirectNetworkPolicy(step.Input.Settings.Policy)
			if err != nil {
				return err
			}
		}
		if backend, ok := a.backend.(loom.NetworkPolicyBackend); ok {
			if err = backend.InstallNetworkPolicy(a.ctx, resource, policy); err != nil {
				return err
			}
			if err = backend.VerifyNetworkPolicy(a.ctx, resource, policy); err != nil {
				return err
			}
		} else if policy != nil {
			return fmt.Errorf("%w: backend lacks direct policy effects", errs.ErrIncompatible)
		}
		if err = a.commitActivationPolicy(step, source, policy); err != nil {
			return err
		}
		if err = a.markActivation(&step, "policy"); err != nil {
			return err
		}
	}
	if err = a.bindActivation(step, source); err != nil {
		return err
	}
	// Admin activation is replayable only under the exact frozen configuration.
	// A newer admission replaces the fence while this RPC may be blocked.
	if step.Phase == "policy" || step.Phase == "activated" || step.Phase == "workload_ready" || step.Phase == "network_on" {
		if err = a.guest.Info(a.ctx, resource); err != nil {
			return err
		}
		if _, err = a.checkActivation(step); err != nil {
			return err
		}
		activation := guest.Activation{
			SandboxID:     step.Input.SandboxID,
			Hostname:      strings.ReplaceAll(step.Input.SandboxID, "_", "-"),
			ResetIdentity: step.Input.ResetIdentity,
			Env:           step.Input.Settings.Env,
			Network:       step.Input.Settings.Policy.Mode,
			CAPEM:         a.caPEM,
		}
		if err = a.guest.Activate(a.ctx, resource, activation); err != nil {
			return err
		}
		if err = a.fault("activation_admin_effect"); err != nil {
			return err
		}
		if step.Phase == "policy" {
			if err = a.markActivation(&step, "activated"); err != nil {
				return err
			}
		}
		if err = a.guest.Info(a.ctx, resource); err != nil {
			return err
		}
		if step.Phase == "activated" {
			if err = a.markActivation(&step, "workload_ready"); err != nil {
				return err
			}
		}
	}
	unlock := a.sandboxConfigLocks.Lock(step.Input.SandboxID)
	defer unlock()
	if _, err = a.checkActivation(step); err != nil {
		return err
	}
	if err = a.actualActivation(source); err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "direct" {
		policy, err := machine.DirectNetworkPolicy(step.Input.Settings.Policy)
		if err != nil {
			return err
		}
		backend, ok := a.backend.(loom.NetworkPolicyBackend)
		if !ok {
			return fmt.Errorf("%w: backend lacks policy verification", errs.ErrIncompatible)
		}
		if err = backend.VerifyNetworkPolicy(a.ctx, resource, policy); err != nil {
			return err
		}
		if err = a.backend.SetNetworkEnabled(a.ctx, resource, true); err != nil {
			return err
		}
		observation, err := a.backend.Inspect(a.ctx, resource)
		if err != nil {
			return err
		}
		if !observation.Alive || !observation.Network {
			return fmt.Errorf("%w: direct network activation unconfirmed", errs.ErrUnknown)
		}
	}
	if step.Phase == "workload_ready" {
		if err = a.markActivation(&step, "network_on"); err != nil {
			return err
		}
	}
	return a.completeActivation(step)
}

func (a *Activations) checkActivation(step ActivationStep) (machine.ResourceStep, error) {
	a.storeWriteMu.Lock()
	defer a.storeWriteMu.Unlock()
	tx, err := a.store.Database.BeginTx(a.ctx, nil)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(a.ctx, tx, step.OperationID)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	if !reflect.DeepEqual(current, step) {
		return machine.ResourceStep{}, fmt.Errorf("activation current intent changed")
	}
	source, _, err := VerifyActivation(a.proofs, a.ctx, tx, step)
	return source, err
}

func (a *Activations) markActivation(step *ActivationStep, phase string) error {
	a.storeWriteMu.Lock()
	defer a.storeWriteMu.Unlock()
	tx, err := a.store.Database.BeginTx(a.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = VerifyActivation(a.proofs, a.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(a.ctx, dbgen.AdvanceLiveStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode(phase),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.Phase = phase
	step.Status = "running"
	if err = a.store.Checkpoint(); err != nil {
		return err
	}
	return a.fault("activation_" + phase)
}

func (a *Activations) commitActivationPolicy(step ActivationStep, source machine.ResourceStep, policy *loom.NetworkPolicy) error {
	a.storeWriteMu.Lock()
	defer a.storeWriteMu.Unlock()
	tx, err := a.store.Database.BeginTx(a.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = VerifyActivation(a.proofs, a.ctx, tx, step); err != nil {
		return err
	}
	policyRecord, lookup := machine.LoadPolicyWitness(a.ctx, tx, source.Candidate)
	previous, instance, priorPolicy := policyRecord.OperationID, policyRecord.Instance, policyRecord.Body
	if lookup == nil && previous != step.OperationID {
		if instance != source.State.Witness && string(priorPolicy) != "null" {
			old, err := machine.LoadDirectRetirement(a.ctx, tx, previous)
			if err != nil {
				return err
			}
			known := false
			for _, ancestor := range source.Input.Ancestors {
				if ancestor.OperationID == old.Input.Source.OperationID && ancestor.InputHash == old.Input.Source.Hash && old.Input.Source.State.Witness == instance && old.Input.Source.Input.Resource.NetworkToken == source.Input.Resource.NetworkToken {
					known = true
					break
				}
			}
			if !known {
				return fmt.Errorf("network policy replacement lacks original stopped generation")
			}
		}
		if err = machine.CompleteDirectReplacement(a.ctx, tx, machine.DirectPolicyReplacement{
			PriorOperationID: previous,
			OperationID:      step.OperationID,
			VMID:             source.Candidate,
			Instance:         instance,
		}); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	activation := step.ResourceProof()
	if err = machine.ArmDirectRetirement(a.ctx, tx, step.OperationID, machine.DirectRetirementInput{
		Source:         source,
		ActivationHash: activation.Hash,
		ActivationKey:  activation.Key,
		Policy:         policy,
	}); err != nil {
		return err
	}
	if err = machine.RecordPolicyWitness(a.ctx, tx, machine.NetworkPolicyRecord{
		VMID:        source.Candidate,
		Instance:    source.State.Witness,
		OperationID: step.OperationID,
		Body:        operation.Encode(policy),
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return a.store.Checkpoint()
}

func (a *Activations) completeActivation(step ActivationStep) error {
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
	if !reflect.DeepEqual(current, step) || step.Phase != "network_on" {
		return fmt.Errorf("sandbox completion lacks original activation proof")
	}
	source, record, err := VerifyActivation(a.proofs, a.ctx, tx, step)
	if err != nil {
		return err
	}
	settings := step.Input.Settings
	settings.Instance = source.State.Witness
	settings.ActiveEpoch = a.epoch
	if settings.Deadline.IsZero() {
		settings.Deadline = time.Now().UTC().Add(time.Duration(settings.TTLSeconds) * time.Second)
	}
	record.State = "running"
	record.LastError = ""
	record.OperationID = ""
	if err = CompleteWarmClaim(a.ctx, tx, step); err != nil {
		return err
	}
	if err = CompleteLifecycleActivation(a.ctx, tx, step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).PublishRunningSandbox(a.ctx, dbgen.PublishRunningSandboxParams{
		Body:   operation.Encode(record),
		ID:     record.ID,
		Tenant: record.TenantID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(a.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    record.ID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(a.ctx, dbgen.DeleteFenceParams{
		SandboxID:   record.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(a.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = a.fault("activation_completion_before_commit"); err != nil {
		return err
	}
	if err = a.committer.Complete(tx, operation.Outcome{
		ID:          step.OperationID,
		AdoptResult: true,
	}); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	a.registry.DropOperationSecrets(step.OperationID)
	if err = a.store.Checkpoint(); err != nil {
		return err
	}
	return a.fault("activation_completed")
}

// Activation verifies historical resource custody and the current configuration
// fence in the caller's transaction before any activation progress is published.
func VerifyActivation(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step ActivationStep) (machine.ResourceStep, ownership.Sandbox, error) {
	var record ownership.Sandbox
	source, err := machine.LoadResourceStep(ctx, tx, step.Input.ResourceOperation)
	if err != nil {
		return source, record, err
	}
	if source.Hash != step.Input.ResourceHash || source.Input.TenantID != step.Input.TenantID || source.Input.SandboxID != step.Input.SandboxID || source.Status != "succeeded" || source.State.Witness != step.Input.ExpectedInstance {
		return source, record, fmt.Errorf("activation original resource witness changed")
	}
	retiring, err := dbgen.New(tx).CountPendingCleanupResource(ctx, dbgen.CountPendingCleanupResourceParams{
		ResourceKind: "direct_policy",
		ResourceID:   source.Candidate,
	})
	if err != nil {
		return source, record, err
	}
	if retiring != 0 {
		return source, record, errs.ErrInterrupted
	}
	if err = proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return source, record, err
	}
	record, err = LoadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return source, record, err
	}
	if record.SourceOperation != source.OperationID || record.VMID != source.Candidate || record.Instance != source.State.Witness || (record.State != "provisioning" && record.State != "running") {
		return source, record, fmt.Errorf("activation sandbox ownership changed")
	}
	resource, current, owner, err := machine.LoadVM(ctx, tx, source)
	if err != nil {
		return source, record, err
	}
	if owner != "" || current != source.State.Witness || resource.Deleting || !resource.Verified || !resource.AgentReady {
		return source, record, fmt.Errorf("activation current VM is not ready")
	}
	fence, err := LifecycleFence(ctx, tx, record.ID)
	if err != nil {
		return source, record, err
	}
	if fence != step.OperationID {
		return source, record, fmt.Errorf("%w: activation configuration was superseded", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, tx, record.ID)
	if err != nil {
		return source, record, err
	}
	expectedSettings := step.Input.Settings
	if step.Input.PreviousSettings != nil && step.Phase == "admitted" {
		expectedSettings = *step.Input.PreviousSettings
	}
	if !reflect.DeepEqual(settings, expectedSettings) || step.Input.Settings.Generation != step.OperationID {
		return source, record, fmt.Errorf("activation desired settings changed")
	}
	identity, err := operation.New(tx).RequestIdentity(ctx, step.OperationID)
	if err != nil {
		return source, record, err
	}
	if operation.Terminal(identity.Status) || identity.ScopeKind != "tenant" || identity.ScopeID != step.Input.RequestTenantID || operation.Digest(identity.Request) != identity.Hash {
		return source, record, fmt.Errorf("activation parent identity changed")
	}
	candidate, expected, err := operation.New(tx).StepTarget(ctx, step.OperationID, step.Key, step.Hash)
	if err != nil {
		return source, record, err
	}
	if candidate != source.Candidate || expected != source.State.Witness {
		return source, record, fmt.Errorf("activation candidate differs from resource")
	}
	return source, record, nil
}
