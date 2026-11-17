package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (service *Provisioning) runCold(step ResourceStep) error {
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err == nil {
		_, _, _, err = service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Resource
	if err := image.VerifyFiles(step.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if step.State.Phase == "admitted" {
		if err := service.markPhase(&step, "prepare_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepare_intent" {
		before, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.backend.Prepare(service.ctx, r); err != nil {
			return service.failStep(step, err)
		}
		if err := service.fault("effect_prepared"); err != nil {
			return err
		}
		if err := service.markPhase(&step, "prepared"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepared" {
		before, err := service.backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.markPhase(&step, "launch_intent"); err != nil {
			return err
		}
	}
	return service.startCandidate(step, false)
}

func (service *Provisioning) startCandidate(step ResourceStep, restore bool) error {
	r := step.Input.Resource
	if step.State.Phase != "launch_intent" && step.State.Phase != "launched" && step.State.Phase != "booted" && step.State.Phase != "agent_ready" {
		return fmt.Errorf("unknown cold resource phase")
	}
	if step.State.DiskIdentity == nil {
		return fmt.Errorf("prepared cold disk proof missing")
	}
	if err := loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	observation, err := service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Network {
		if err = service.backend.Network(service.ctx, r, false); err != nil {
			return err
		}
	}
	if !observation.Alive {
		if step.State.Phase != "launch_intent" {
			return service.failStep(step, fmt.Errorf("%w: previous candidate exited; refusing another launch", errs.ErrUnknown))
		}
		if err = service.backend.Network(service.ctx, r, false); err != nil {
			return service.failStep(step, err)
		}
		if err = service.backend.Launch(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.fault("effect_launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launch_intent" {
		if err = service.markPhase(&step, "launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launched" {
		if err = service.backend.Boot(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.markPhase(&step, "booted"); err != nil {
			return err
		}
	}
	if err = service.backend.Agent(service.ctx, r, restore); err != nil {
		networkErr := service.backend.Network(service.ctx, r, false)
		stopErr := service.backend.Stop(service.ctx, r)
		return service.failStep(step, errors.Join(err, networkErr, stopErr))
	}
	if step.State.Phase != "agent_ready" {
		if err = service.markPhase(&step, "agent_ready"); err != nil {
			return err
		}
	}
	observation, err = service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if !observation.Alive || !observation.Booted || !observation.Configured || observation.Paused || observation.Network || observation.PID <= 0 || observation.StartTime == 0 {
		return service.failStep(step, fmt.Errorf("%w: candidate is not verified System ready with network off", errs.ErrUnknown))
	}
	if restore {
		if step.Input.Restore == nil || !step.State.BackingReady {
			return fmt.Errorf("restore start lacks backing handoff")
		}
		if err := loom.VerifyRestoreMaterials(step.State.Materials, r, step.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
			return service.failStep(step, err)
		}
	}
	if err = loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	return service.completeCold(step, observation)
}

// ProvisionIntent fixes the generation candidate and predecessor before any
// ownership or cost transfer. ExpectedInstance is empty for a new logical VM.
type ProvisionIntent struct {
	OperationID      string
	Action           string
	ExpectedInstance string
	Input            ResourceInput
}

// AdmitProvisionIntent freezes the physical intent before the coordinator's
// sandbox and quota writes. It borrows the existing admission transaction.
func AdmitProvisionIntent(ctx context.Context, tx *sql.Tx, intent ProvisionIntent) error {
	payload := operation.Encode(intent.Input)
	return store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         intent.OperationID,
		StepKey:             "resource",
		Action:              intent.Action,
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: intent.Input.Resource.VMID,
		ExpectedInstance:    intent.ExpectedInstance,
		Status:              "pending",
		Body: operation.Encode(ResourceState{
			Phase: "admitted",
		}),
	}))
}

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

func (service *Provisioning) completeCold(step ResourceStep, observation loom.Observation) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadResourceStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(current.Status) {
		return nil
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.Input, step.Input) || !reflect.DeepEqual(current.State, step.State) || current.State.Phase != "agent_ready" || current.State.DiskIdentity == nil || current.State.Witness != "" {
		return fmt.Errorf("cold completion lacks its original ready candidate")
	}
	currentMachine, record, result, err := service.ownership.VerifyProvisionIntent(service.proofs, service.ctx, tx, current)
	if err != nil {
		return err
	}
	currentMachine.Verified, currentMachine.AgentReady, currentMachine.Observation = true, true, &observation
	r := currentMachine.Resource
	if err = store.RequireOne(dbgen.New(tx).CompleteStart(service.ctx, dbgen.CompleteStartParams{
		ID:              r.VMID,
		CurrentInstance: r.Instance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
		Body: operation.Encode(currentMachine),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceInstanceBody(service.ctx, dbgen.ReplaceInstanceBodyParams{
		ID:          r.Instance,
		VmID:        r.VMID,
		OperationID: step.OperationID,
		Body:        operation.Encode(currentMachine),
	})); err != nil {
		return err
	}
	if err = service.handoff.PublishInstance(service.ctx, tx, record, r.Instance); err != nil {
		return err
	}
	if err = quota.New(tx).AddCost(service.ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        record.TenantID,
			Kind:            "vm_disk",
			Key:             current.Input.DiskKey,
			SourceOperation: step.OperationID,
		},
		StorageMiB: r.Config.Shape.DiskMiB,
	}); err != nil {
		return err
	}
	if err = quota.New(tx).AddCostReference(service.ctx, quota.CostReference{
		TenantID:   record.TenantID,
		Kind:       "vm_disk",
		Key:        current.Input.DiskKey,
		HolderKind: "active_use",
		HolderID:   "sandbox:" + record.ID,
	}); err != nil {
		return err
	}
	if err = quota.New(tx).RemoveCostReference(service.ctx, quota.CostReference{
		TenantID:   record.TenantID,
		Kind:       "pending_storage",
		Key:        step.OperationID + ":resource",
		HolderKind: "engine_dependency",
		HolderID:   step.OperationID + ":resource",
	}); err != nil {
		return err
	}
	if err = quota.New(tx).RetireCostAtKey(service.ctx, record.TenantID, "pending_storage", step.OperationID+":resource"); err != nil {
		return err
	}
	for _, reference := range []struct {
		kind       string
		id         string
		holderKind string
		holder     string
	}{
		{"vm", r.VMID, "sandbox", record.ID},
		{"image", r.Config.Image, "vm", r.VMID},
	} {
		if current.Action != "restore" || reference.kind != "image" {
			if err = quota.New(tx).AddResourceReference(service.ctx, quota.ResourceReference{
				Kind:       reference.kind,
				ID:         reference.id,
				HolderKind: reference.holderKind,
				HolderID:   reference.holder,
			}); err != nil {
				return err
			}
		}
		if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
			Kind:       reference.kind,
			ID:         reference.id,
			HolderKind: "step",
			HolderID:   step.OperationID + ":resource",
		}); err != nil {
			return err
		}
	}
	next := current.State
	next.Phase, next.Witness, next.Observation = "complete", r.Instance, &observation
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStepByInput(service.ctx, dbgen.CompleteRunningStepByInputParams{
		OperationID: step.OperationID,
		StepKey:     step.Key,
		InputHash:   step.Hash,
		Body:        operation.Encode(next),
	})); err != nil {
		return err
	}
	result.ResourceReady = true
	if err = operation.New(tx).RunResult(service.ctx, step.OperationID, operation.Encode(result)); err != nil {
		return err
	}
	if err = service.fault("completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	return service.fault("step_completed")
}
