package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type ActivationInput struct {
	PreviousSettings  *Settings `json:"previous_settings,omitempty"`
	Attempt           string    `json:"attempt,omitempty"`
	AllowFallback     bool      `json:"allow_fallback,omitempty"`
	RequestTenantID   string    `json:"request_tenant_id"`
	TenantID          string    `json:"tenant_id"`
	SandboxID         string    `json:"sandbox_id"`
	ResourceOperation string    `json:"resource_operation"`
	ResourceHash      string    `json:"resource_hash"`
	ExpectedInstance  string    `json:"expected_instance"`
	Settings          Settings  `json:"settings"`
	ResetIdentity     bool      `json:"reset_identity"`
}

type ActivationStep struct {
	OperationID string
	Key         string
	Hash        string
	Status      string
	Phase       string
	Input       ActivationInput
}

func ActivationKey(attempt string) string {
	if attempt == "" {
		return "activation"
	}
	return "activation:" + attempt
}

func LoadActivation(ctx context.Context, query store.DBTX, id string) (ActivationStep, error) {
	step := ActivationStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetLatestActivationStep(ctx, id)
	step.Key, step.Hash, step.Status = record.StepKey, record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(record.Input) != step.Hash {
		return step, fmt.Errorf("activation input checksum changed")
	}
	if err = json.Unmarshal(record.Input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(record.Body, &step.Phase); err != nil {
		return step, err
	}
	if step.Key != ActivationKey(step.Input.Attempt) {
		return step, fmt.Errorf("activation attempt key changed")
	}
	if record.Action != "activate" || record.ExpectedInstance != step.Input.ExpectedInstance || record.CandidateResourceID == "" {
		return step, fmt.Errorf("activation frozen identity changed")
	}
	return step, nil
}

func (service *Activations) checkActivation(step ActivationStep) (machine.ResourceStep, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	if !reflect.DeepEqual(current, step) {
		return machine.ResourceStep{}, fmt.Errorf("activation current intent changed")
	}
	source, _, err := VerifyActivation(service.proofs, service.ctx, tx, step)
	return source, err
}

func (service *Activations) markActivation(step *ActivationStep, phase string) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = VerifyActivation(service.proofs, service.ctx, tx, *step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
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
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("activation_" + phase)
}

func (service *Activations) commitActivationPolicy(step ActivationStep, source machine.ResourceStep, policy *loom.NetworkPolicy) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if _, _, err = VerifyActivation(service.proofs, service.ctx, tx, step); err != nil {
		return err
	}
	policyRecord, lookup := machine.LoadPolicyWitness(service.ctx, tx, source.Candidate)
	previous, instance, priorPolicy := policyRecord.OperationID, policyRecord.Instance, policyRecord.Body
	if lookup == nil && previous != step.OperationID {
		if instance != source.State.Witness && string(priorPolicy) != "null" {
			old, err := machine.LoadDirectRetirement(service.ctx, tx, previous)
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
		if err = machine.CompleteDirectReplacement(service.ctx, tx, machine.DirectPolicyReplacement{
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
	if err = machine.ArmDirectRetirement(service.ctx, tx, step.OperationID, machine.DirectRetirementInput{
		Source:         source,
		ActivationHash: activation.Hash,
		ActivationKey:  activation.Key,
		Policy:         policy,
	}); err != nil {
		return err
	}
	if err = machine.RecordPolicyWitness(service.ctx, tx, machine.NetworkPolicyRecord{
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
	return service.store.Checkpoint()
}

func (service *Activations) completeActivation(step ActivationStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Phase != "network_on" {
		return fmt.Errorf("sandbox completion lacks original activation proof")
	}
	source, record, err := VerifyActivation(service.proofs, service.ctx, tx, step)
	if err != nil {
		return err
	}
	settings := step.Input.Settings
	settings.Instance = source.State.Witness
	settings.ActiveEpoch = service.epoch
	if settings.Deadline.IsZero() {
		settings.Deadline = time.Now().UTC().Add(time.Duration(settings.TTLSeconds) * time.Second)
	}
	record.State = "running"
	record.LastError = ""
	record.OperationID = ""
	if err = CompleteWarmClaim(service.ctx, tx, step); err != nil {
		return err
	}
	if err = CompleteLifecycleActivation(service.ctx, tx, step); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).PublishRunningSandbox(service.ctx, dbgen.PublishRunningSandboxParams{
		Body:   operation.Encode(record),
		ID:     record.ID,
		Tenant: record.TenantID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(service.ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    record.ID,
		PreviousBody: operation.Encode(step.Input.Settings),
		NextBody:     operation.Encode(settings),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteFence(service.ctx, dbgen.DeleteFenceParams{
		SandboxID:   record.ID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      step.Key,
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operation.New(tx).Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("activation_completion_before_commit"); err != nil {
		return err
	}
	if err = service.committer.Commit(tx); err != nil {
		return err
	}
	service.access.DropOperationSecrets(step.OperationID)
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("activation_completed")
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
