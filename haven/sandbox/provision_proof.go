package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/supply"
)

func VerifyProvisionIntent(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step machine.ResourceStep) (machine.VMRecord, ownership.Sandbox, operation.Result, error) {
	var result operation.Result
	request, err := operation.New(tx).RequestState(ctx, step.OperationID)
	if err != nil {
		return machine.VMRecord{}, ownership.Sandbox{}, result, err
	}
	tenant := request.ScopeID
	if operation.Digest(request.Request) != request.Hash || request.ScopeKind != "tenant" || tenant != step.Input.TenantID || operation.Terminal(request.Status) {
		return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("cold operation identity or lifecycle changed")
	}
	switch step.Action {
	case "restore":
		var frozen struct {
			Action  string
			Request machine.RestoreRequest
		}
		if err = json.Unmarshal(request.Request, &frozen); err != nil {
			return machine.VMRecord{}, ownership.Sandbox{}, result, err
		}
		if step.Input.Restore == nil || frozen.Action != "restore" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != step.Input.SandboxID || frozen.Request.SnapshotID != step.Input.Restore.Snapshot.ID || frozen.Request.ExpectedInstance != step.ExpectedInstance || frozen.Request.StopOperationID != step.Input.StopOperation {
			return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("restore request differs from frozen predecessor")
		}
		stop, err := machine.LoadStop(ctx, tx, step.Input.StopOperation)
		if err != nil {
			return machine.VMRecord{}, ownership.Sandbox{}, result, err
		}
		if stop.Status != "succeeded" || stop.Phase != "complete" || len(step.Input.Ancestors) == 0 || stop.Input.SourceOperation != step.Input.Ancestors[len(step.Input.Ancestors)-1].OperationID || stop.Input.Request.ExpectedInstance != step.ExpectedInstance {
			return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("restore stop history changed")
		}
	case "clone":
		var frozen struct {
			Action  string
			Request machine.CloneRequest
		}
		if err = json.Unmarshal(request.Request, &frozen); err != nil {
			return machine.VMRecord{}, ownership.Sandbox{}, result, err
		}
		if step.Input.Restore == nil || frozen.Action != "clone" || frozen.Request.TenantID != tenant || frozen.Request.SnapshotID != step.Input.Restore.Snapshot.ID {
			return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("clone request differs from frozen source")
		}
	default:
		var frozen struct {
			Action  string
			Request machine.ColdRequest
		}
		if err = json.Unmarshal(request.Request, &frozen); err != nil {
			return machine.VMRecord{}, ownership.Sandbox{}, result, err
		}
		if step.Action != "cold_start" || frozen.Action != "claim" || frozen.Request.TenantID != tenant || frozen.Request.TemplateID != step.Input.TemplateID {
			return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("cold request differs from frozen step")
		}
	}
	if err = json.Unmarshal(request.Body, &result); err != nil {
		return machine.VMRecord{}, ownership.Sandbox{}, result, err
	}
	if result.SandboxID != step.Input.SandboxID || result.VMID != step.Candidate {
		return machine.VMRecord{}, ownership.Sandbox{}, result, fmt.Errorf("cold result destination changed")
	}
	record, err := LoadSandbox(ctx, tx, step.Input.SandboxID, tenant)
	if err != nil {
		return machine.VMRecord{}, record, result, err
	}
	if record.OperationID != step.OperationID || record.SourceOperation != step.OperationID || record.VMID != step.Candidate || record.State != "provisioning" {
		return machine.VMRecord{}, record, result, fmt.Errorf("cold sandbox destination changed")
	}
	currentMachine, instance, owner, err := machine.LoadVM(ctx, tx, step)
	if err != nil {
		return currentMachine, record, result, err
	}
	if owner != step.OperationID || currentMachine.Deleting || instance != "" && !machine.ExpectedCurrentInstance(step, instance) {
		return currentMachine, record, result, fmt.Errorf("cold VM ownership changed")
	}
	if _, err = proofs.GenerationHistory(ctx, tx, step); err != nil {
		return currentMachine, record, result, err
	}
	if step.Action == "cold_start" {
		if err = supply.RequireTemplateOwner(ctx, tx, step.Input.TemplateID, tenant); err != nil {
			return currentMachine, record, result, err
		}
	} else {
		if step.State.RestoreBlocked {
			return currentMachine, record, result, fmt.Errorf("%w: restore blocked; explicit cleanup required", errs.ErrManualRecovery)
		}
		if err = proofs.Snapshots().RestoreRelations(ctx, tx, step); err != nil {
			return currentMachine, record, result, err
		}
	}
	manifest, err := image.LoadManifest(ctx, tx, currentMachine.Resource.Config.Image)
	if err != nil || !reflect.DeepEqual(manifest, step.Input.Image) {
		return currentMachine, record, result, fmt.Errorf("image publication proof changed: %v", err)
	}
	var candidateBody, versionBody []byte
	if candidateBody, err = machine.InstanceHistory(ctx, tx, currentMachine.Resource.Instance, currentMachine.Resource.VMID, step.OperationID); err != nil {
		return currentMachine, record, result, err
	}
	var candidate machine.VMRecord
	if err = json.Unmarshal(candidateBody, &candidate); err != nil || !reflect.DeepEqual(candidate.Resource, currentMachine.Resource) {
		return currentMachine, record, result, fmt.Errorf("instance candidate changed: %v", err)
	}
	if versionBody, err = machine.VersionHistory(ctx, tx, step.OperationID, step.Candidate, step.Key); err != nil {
		return currentMachine, record, result, err
	}
	var version machine.Version
	if err = json.Unmarshal(versionBody, &version); err != nil || !reflect.DeepEqual(version.Resource, currentMachine.Resource) || !reflect.DeepEqual(version.DiskIdentity, step.State.DiskIdentity) {
		return currentMachine, record, result, fmt.Errorf("resource version changed: %v", err)
	}
	shape := currentMachine.Resource.Config.Shape
	vmBytes := step.Input.RuntimeBytes
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountMatchingReservation(ctx, quota.Reservation{
		Owner:     "vm:" + step.Candidate,
		Bytes:     vmBytes,
		MemoryMiB: shape.MemoryMiB + 128,
		CPUs:      int64(shape.VCPUs),
	}); err != nil || count != 1 {
		return currentMachine, record, result, fmt.Errorf("cold responsibility proof missing: %v", err)
	}
	for _, charge := range []struct {
		cost       quota.Cost
		holderKind string
		holderID   string
	}{
		{quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        tenant,
				Kind:            "pending_storage",
				Key:             step.OperationID + ":resource",
				SourceOperation: step.OperationID,
			},
			StorageMiB: shape.DiskMiB,
		}, "engine_dependency", step.OperationID + ":resource"},
		{quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        tenant,
				Kind:            step.Input.SlotBudget.Kind,
				Key:             record.ID,
				SourceOperation: step.Input.SlotOperation,
			},
			Slots: step.Input.SlotBudget.Units,
		}, "active_use", "sandbox:" + record.ID},
	} {
		if count, err := responsibilities.CountMatchingCost(ctx, charge.cost); err != nil || count != 1 {
			return currentMachine, record, result, fmt.Errorf("cold responsibility proof missing: %v", err)
		}
		if count, err := responsibilities.CountCostReference(ctx, quota.CostReference{
			TenantID:   charge.cost.TenantID,
			Kind:       charge.cost.Kind,
			Key:        charge.cost.Key,
			HolderKind: charge.holderKind,
			HolderID:   charge.holderID,
		}); err != nil || count != 1 {
			return currentMachine, record, result, fmt.Errorf("cold responsibility proof missing: %v", err)
		}
	}
	for _, reference := range []quota.ResourceReference{
		{
			Kind:       "vm",
			ID:         step.Candidate,
			HolderKind: "step",
			HolderID:   step.OperationID + ":resource",
		},
		{
			Kind:       "image",
			ID:         currentMachine.Resource.Config.Image,
			HolderKind: "step",
			HolderID:   step.OperationID + ":resource",
		},
	} {
		if count, err := responsibilities.CountResourceReference(ctx, reference); err != nil || count != 1 {
			return currentMachine, record, result, fmt.Errorf("cold responsibility proof missing: %v", err)
		}
	}
	return currentMachine, record, result, nil
}
