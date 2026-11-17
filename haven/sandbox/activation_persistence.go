package sandbox

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

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
		if err = machine.CompleteDirectReplacement(service.ctx, tx, previous, step.OperationID, source.Candidate, instance); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	if err = machine.ArmDirectRetirement(service.ctx, tx, step.ResourceProof(), source, policy); err != nil {
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
