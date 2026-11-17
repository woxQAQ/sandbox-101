package machine

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

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
