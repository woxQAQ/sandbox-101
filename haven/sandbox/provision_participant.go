package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// ProvisionHistory retains no connection or running service.
type ProvisionHistory struct{}

func (ProvisionHistory) VerifyProvisionIntent(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step machine.ResourceStep) (machine.VMRecord, ownership.Sandbox, operation.Result, error) {
	return VerifyProvisionIntent(proofs, ctx, tx, step)
}

// ProvisionHandoff writes only through the transaction owned by the resource
// workflow; activation delivery remains under the coordinator's VM guard.
type ProvisionHandoff struct{}

func (ProvisionHandoff) PublishInstance(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, instance string) error {
	record.Instance = instance
	return store.RequireOne(dbgen.New(tx).UpdateProvisioningSandboxBody(ctx, dbgen.UpdateProvisioningSandboxBodyParams{
		Body:   operation.Encode(record),
		ID:     record.ID,
		Tenant: record.TenantID,
	}))
}

func (ProvisionHandoff) BlockActivation(ctx context.Context, tx *sql.Tx, id string) error {
	if activation, lookup := LoadActivation(ctx, tx, id); lookup == nil && !operation.Terminal(activation.Status) {
		if err := store.RequireOne(dbgen.New(tx).FailLiveStep(ctx, dbgen.FailLiveStepParams{
			OperationID: id,
			StepKey:     activation.Key,
			InputHash:   activation.Hash,
			Body:        operation.Encode("blocked"),
		})); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	return nil
}

// AdmitIn joins resource intent and all charges to the caller's transaction.
// A batch commits every destination before dispatching any effect.
func (service *VMProvisioning) AdmitIn(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission, free int64) (operation.Record, error) {
	var empty operation.Record
	request := admission.Request
	tenantID, frozen := request.TenantID, request.Frozen
	hash := operation.Digest(frozen)
	selected, manifest, restore := request.Template, request.Image, request.Restore
	claim, target := admission.Claim, admission.Target
	if err := service.verifyProvisionSources(ctx, tx, admission); err != nil {
		return empty, err
	}
	allocation, err := service.checkProvisionBudget(ctx, tx, admission, free)
	if err != nil {
		return empty, err
	}
	budget, reuseSlot := allocation.Slot, allocation.ReuseSlot
	vmBytes, restoreBytes := allocation.VMBytes, allocation.RestoreBytes
	shape := selected.Config.Shape
	slot, err := machine.AvailableNetworkSlot(ctx, tx)
	if err != nil {
		return empty, err
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: operation.NewID("sandbox_"),
			VMID:      operation.NewID("vm-"),
		},
	}
	if target != nil {
		if reuseSlot {
			accepted.Result.SandboxID = target.Sandbox.ID
		} else if target.NewSandboxID != "" {
			accepted.Result.SandboxID = target.NewSandboxID
		}
	}
	resource := loom.Resource{
		VMID:         accepted.Result.VMID,
		Instance:     operation.NewID("run-"),
		Config:       selected.Config,
		Slot:         slot,
		NetworkToken: operation.NewID("net-"),
	}
	resource.Dir = filepath.Join(service.store.Directory, "vms", resource.VMID)
	resource.Disk = filepath.Join(resource.Dir, "disks", accepted.ID+".raw")
	resource.RunDir = filepath.Join(resource.Dir, "runs", resource.Instance)
	resource.ImageDir = filepath.Join(service.store.Directory, "images", resource.Config.Image[7:])
	action := "cold_start"
	if restore != nil {
		action = "clone"
		resource.Disk = filepath.Join(resource.Dir, "versions", accepted.ID, "disk.raw")
		resource.RestoreDir = filepath.Join(resource.Dir, "versions", accepted.ID, "restore")
	}
	input := machine.ResourceInput{
		SlotBudget:    budget,
		DiskKey:       resource.VMID,
		SlotOperation: accepted.ID,
		RuntimeBytes:  vmBytes,
		Resource:      resource,
		Image:         manifest,
		TemplateID:    selected.ID,
		TenantID:      request.TenantID,
		SandboxID:     accepted.Result.SandboxID,
		Restore:       restore,
	}
	if reuseSlot {
		current, err := LoadSandbox(ctx, tx, target.Sandbox.ID, tenantID)
		if err != nil {
			return empty, err
		}
		validator, ok := target.Parent.(ProvisionTargetValidator)
		if restore == nil || !ok || target.SlotOperation == "" || target.SlotHolder == "" || !reflect.DeepEqual(current, target.Sandbox) {
			return empty, fmt.Errorf("%w: provision target ownership changed", errs.ErrConflict)
		}
		failure, err := validator.ValidateProvisionTarget(ctx, tx, ProvisionSlotClaim{
			Sandbox: current,
			Source:  *restore,
			Holder:  target.SlotHolder,
		})
		if err != nil {
			return empty, err
		}
		count, lookup := quota.New(tx).CountMatchingCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        tenantID,
				Kind:            "sandbox_slot",
				Key:             current.ID,
				SourceOperation: target.SlotOperation,
			},
			Slots: 1,
		})
		if lookup != nil || count != 1 {
			return empty, fmt.Errorf("%w: provision slot responsibility missing", errs.ErrManualRecovery)
		}
		input.SlotOperation = target.SlotOperation
		input.SlotAssignment = current.SlotAssignment
		input.FailureSlotDestination = failure
		if err = machine.AdmitResourceParent(ctx, tx, target.Parent, machine.ResourceAdmission{
			Child:       accepted,
			Kind:        action,
			Candidate:   resource.VMID,
			RequestHash: hash,
		}); err != nil {
			return empty, err
		}
	}
	if restore != nil {
		input.BackingHolder = resource.VMID
	}
	provisionSource := "cold"
	if restore != nil {
		provisionSource = "golden"
	}
	identity := NewSandboxIdentity(selected, provisionSource)
	if reuseSlot {
		identity = target.Sandbox.Identity
	}
	if target != nil && target.Identity != nil {
		identity = *target.Identity
	}
	destination := ownership.Sandbox{
		Identity:        identity,
		SlotAssignment:  input.SlotAssignment,
		ID:              input.SandboxID,
		TenantID:        request.TenantID,
		OperationID:     accepted.ID,
		VMID:            resource.VMID,
		State:           "provisioning",
		SourceOperation: accepted.ID,
	}
	currentMachine := machine.VMRecord{
		Resource: resource,
	}
	if err = operation.New(tx).Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Hash:      hash,
		Request:   frozen,
		Status:    accepted.Status,
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = machine.AdmitProvisionIntent(ctx, tx, machine.ProvisionIntent{
		OperationID: accepted.ID,
		Action:      action,
		Input:       input,
	}); err != nil {
		return empty, err
	}
	if !reuseSlot {
		if err = store.RequireOne(dbgen.New(tx).InsertSandbox(ctx, dbgen.InsertSandboxParams{
			ID:     destination.ID,
			Tenant: destination.TenantID,
			State:  destination.State,
			Body:   operation.Encode(destination),
		})); err != nil {
			return empty, err
		}
	} else {
		if err = store.RequireOne(dbgen.New(tx).ReuseSandboxForProvision(ctx, dbgen.ReuseSandboxForProvisionParams{
			ID:            destination.ID,
			Tenant:        destination.TenantID,
			PreviousState: target.Sandbox.State,
			PreviousBody:  operation.Encode(target.Sandbox),
			NewBody:       operation.Encode(destination),
		})); err != nil {
			return empty, err
		}
		if err = quota.New(tx).MoveCostReference(ctx, quota.CostReference{
			TenantID:   tenantID,
			Kind:       "sandbox_slot",
			Key:        destination.ID,
			HolderKind: "active_use",
			HolderID:   target.SlotHolder,
		}, "sandbox:"+destination.ID); err != nil {
			return empty, err
		}
	}
	if target != nil && !reuseSlot && target.Parent != nil {
		if err = machine.AdmitResourceParent(ctx, tx, target.Parent, machine.ResourceAdmission{
			Child:       accepted,
			Kind:        action,
			Candidate:   resource.VMID,
			RequestHash: hash,
		}); err != nil {
			return empty, err
		}
	}
	if err = machine.AdmitCandidate(ctx, tx, accepted.ID, currentMachine); err != nil {
		return empty, err
	}
	if err = quota.New(tx).Reserve(ctx, quota.Reservation{
		Owner:     "vm:" + resource.VMID,
		Bytes:     vmBytes,
		MemoryMiB: shape.MemoryMiB + 128,
		CPUs:      int64(shape.VCPUs),
	}); err != nil {
		return empty, err
	}
	for _, charge := range []struct {
		kind       string
		key        string
		slots      int64
		storage    int64
		holderKind string
		holder     string
	}{
		{budget.Kind, destination.ID, budget.Units, 0, "active_use", "sandbox:" + destination.ID},
		{"pending_storage", accepted.ID + ":resource", 0, shape.DiskMiB, "engine_dependency", accepted.ID + ":resource"},
	} {
		if reuseSlot && charge.kind == budget.Kind {
			continue
		}
		if err = quota.New(tx).AddCost(ctx, quota.Cost{
			CostIdentity: quota.CostIdentity{
				TenantID:        request.TenantID,
				Kind:            charge.kind,
				Key:             charge.key,
				SourceOperation: accepted.ID,
			},
			Slots:      charge.slots,
			StorageMiB: charge.storage,
		}); err != nil {
			return empty, err
		}
		if err = quota.New(tx).AddCostReference(ctx, quota.CostReference{
			TenantID:   request.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: charge.holderKind,
			HolderID:   charge.holder,
		}); err != nil {
			return empty, err
		}
	}
	for _, reference := range []struct {
		kind string
		id   string
	}{
		{"vm", resource.VMID},
		{"image", resource.Config.Image},
	} {
		if err = quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
			Kind:       reference.kind,
			ID:         reference.id,
			HolderKind: "step",
			HolderID:   accepted.ID + ":resource",
		}); err != nil {
			return empty, err
		}
	}
	if restore != nil {
		if err = quota.New(tx).Reserve(ctx, quota.Reservation{
			Owner: "restore:" + accepted.ID,
			Bytes: restoreBytes,
		}); err != nil {
			return empty, err
		}
		if err = machine.PinSnapshotSource(ctx, tx, tenantID, machine.SourceLease{
			SnapshotID:  restore.Snapshot.ID,
			OperationID: accepted.ID,
			StepKey:     "resource",
		}); err != nil {
			return empty, err
		}
	}
	activation := claim
	if target != nil && target.Activation != nil {
		if activation != nil {
			return empty, errors.New("resource has two activation admissions")
		}
		activation = target.Activation
	}
	if activation != nil {
		if err = service.activations.AdmitIn(tx, accepted, input, *activation); err != nil {
			return empty, err
		}
	}
	return accepted, nil
}
