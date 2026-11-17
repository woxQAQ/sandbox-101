package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/supply"
)

// AdmitCold freezes a tenant-owned template and all resource identities with its
// slot, disk budget and source reference. Delivery remains separately fenced.
func (p *VMProvisioning) AdmitCold(ctx context.Context, request machine.FreshRequest) (operation.Record, error) {
	var empty operation.Record
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.TemplateID == "" {
		return empty, fmt.Errorf("%w: tenant, template and a key of 1..256 bytes are required", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.FreshRequest
	}{"claim", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(p.store.Database).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			p.dispatch(cached.ID)
		}
		return cached, err
	}
	selected, err := supply.NewTemplateCatalog(p.store.Database).Cold(ctx, request.TenantID, request.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&selected.Config); err != nil {
		return empty, err
	}
	if err = p.backend.Check(ctx, selected.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, p.store.Database, selected.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(p.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	return p.Admit(ctx, ProvisionAdmission{
		Request: supply.ProvisionRequest{
			TenantID: request.TenantID,
			Key:      request.Key,
			Frozen:   frozen,
			Template: selected,
			Image:    manifest,
		},
	})
}

func (p *VMProvisioning) AdmitClone(ctx context.Context, request machine.CloneRequest) (operation.Record, error) {
	var empty operation.Record
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, fmt.Errorf("%w: clone requires tenant, snapshot and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.CloneRequest
	}{"clone", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(p.store.Database).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			p.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := p.proofs.Snapshots().LoadRestoreSource(ctx, p.store.Database, request.TenantID, request.SnapshotID)
	if err == sql.ErrNoRows {
		return empty, fmt.Errorf("%w: snapshot not found", errs.ErrNotFound)
	}
	if err != nil {
		return empty, err
	}
	config := source.Snapshot.Manifest.Config
	if err = host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err = p.backend.Check(ctx, config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, p.store.Database, config.Image)
	if err != nil {
		return empty, err
	}
	var frozenImage host.ImageManifest
	if err = json.Unmarshal(source.Snapshot.Manifest.Image, &frozenImage); err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(manifest, frozenImage) {
		return empty, fmt.Errorf("clone image publication changed")
	}
	if err = image.VerifyFiles(manifest, filepath.Join(p.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	return p.Admit(ctx, ProvisionAdmission{
		Request: supply.ProvisionRequest{
			TenantID: request.TenantID,
			Key:      request.Key,
			Frozen:   frozen,
			Template: supply.TemplateRecord{
				TenantID: request.TenantID,
				Config:   config,
			},
			Image:   manifest,
			Restore: &source,
		},
	})
}

func (p *VMProvisioning) Admit(ctx context.Context, admission ProvisionAdmission) (operation.Record, error) {
	var empty operation.Record
	request, claim := admission.Request, admission.Claim
	tenantID, key := request.TenantID, request.Key
	hash := operation.Digest(request.Frozen)
	free, err := loom.FreeBytes(p.store.Directory)
	if err != nil {
		return empty, err
	}
	p.admission.Lock()
	defer p.admission.Unlock()
	if err = p.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := p.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if claim != nil {
		previous, lookup := operation.New(tx).TenantCredentialMAC(ctx, tenantID, key)
		if lookup == nil && previous != claim.MAC {
			return empty, fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
		}
		if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return empty, lookup
		}
	}
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   tenantID,
	}, key, hash); found || err != nil {
		if claim != nil && found && err == nil && !operation.Terminal(cached.Status) {
			p.registry.SetOperationSecrets(cached.ID, claim.Secrets)
		}
		if found && err == nil && !operation.Terminal(cached.Status) {
			p.dispatch(cached.ID)
		}
		return cached, err
	}
	accepted, err := p.AdmitIn(ctx, tx, admission, free)
	if err != nil {
		return empty, err
	}
	if err = p.fault("admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if claim != nil {
		p.registry.SetOperationSecrets(accepted.ID, claim.Secrets)
	}
	if err = p.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = p.fault("admission_committed"); err != nil {
		return accepted, err
	}
	p.dispatch(accepted.ID)
	return accepted, nil
}

func (p *VMProvisioning) dispatch(id string) {
	if p.store.Reviewed {
		return
	}
	p.workers.Start(id, func() error {
		if handled, err := p.policies.Resume(p.ctx, id); handled || err != nil {
			return err
		}
		return p.Continue(id)
	})
}

// verifyProvisionSources rechecks preflight facts inside the allocation
// transaction before writing any candidate, ownership or budget records.
func (p *VMProvisioning) verifyProvisionSources(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission) error {
	request := admission.Request
	selected, restore := request.Template, request.Restore
	if admission.Claim != nil {
		current, err := supply.NewTemplateCatalog(tx).Owned(ctx, request.TenantID, selected.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, selected) {
			return fmt.Errorf("%w: template publication changed during preflight", errs.ErrConflict)
		}
	}
	provisional := false
	if admission.Target != nil {
		if declared, ok := provisionalTemplate(admission.Target.Parent); ok {
			if !reflect.DeepEqual(declared, selected) {
				return fmt.Errorf("%w: provisional resource template differs from supply intent", errs.ErrConflict)
			}
			provisional = true
		}
	}
	if restore == nil && !provisional {
		catalog := supply.NewTemplateCatalog(tx)
		loader := catalog.Cold
		if admission.Claim != nil {
			loader = catalog.Owned
		}
		current, err := loader(ctx, request.TenantID, selected.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, selected) {
			return fmt.Errorf("%w: template publication changed during preflight", errs.ErrConflict)
		}
	} else if restore != nil {
		current, err := p.proofs.Snapshots().LoadRestoreSource(ctx, tx, request.TenantID, restore.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *restore) {
			return fmt.Errorf("%w: snapshot publication changed during preflight", errs.ErrConflict)
		}
	}
	currentImage, err := image.LoadManifest(ctx, tx, selected.Config.Image)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(currentImage, request.Image) {
		return fmt.Errorf("%w: image publication changed during preflight", errs.ErrConflict)
	}
	return nil
}

// provisionBudget is the checked allocation cost, reused unchanged by the
// subsequent intent, reservation and charge writes in the same transaction.
type provisionBudget struct {
	Slot         quota.SlotBudget
	ReuseSlot    bool
	VMBytes      int64
	RestoreBytes int64
}

func (p *VMProvisioning) checkProvisionBudget(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission, free int64) (provisionBudget, error) {
	var empty provisionBudget
	request, target := admission.Request, admission.Target
	tenant, err := authorization.New(tx).Limits(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	usage, err := quota.New(tx).Usage(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 {
		return empty, fmt.Errorf("invalid tenant budget")
	}
	budget := provisionBudget{
		Slot: quota.SlotBudget{
			Kind:  "sandbox_slot",
			Units: 1,
		},
		ReuseSlot: target != nil && target.Sandbox.ID != "",
	}
	if target != nil && target.Budget != nil {
		budget.Slot = *target.Budget
	}
	if (budget.Slot.Kind != "sandbox_slot" && budget.Slot.Kind != "build_slot") || budget.Slot.Units < 0 || budget.Slot.Units > 1 {
		return empty, fmt.Errorf("%w: invalid slot budget", errs.ErrInvalid)
	}
	newSlots := budget.Slot.Units
	if budget.ReuseSlot {
		newSlots = 0
	}
	shape := request.Template.Config.Shape
	if usage.Slots > tenant.MaxSandboxes || newSlots > tenant.MaxSandboxes-usage.Slots || usage.StorageMiB > tenant.MaxStorageMiB || shape.DiskMiB > tenant.MaxStorageMiB-usage.StorageMiB {
		return empty, fmt.Errorf("%w: tenant quota exhausted", errs.ErrExhausted)
	}
	reserved, err := quota.New(tx).ReservedCapacity(ctx)
	if err != nil {
		return empty, err
	}
	budget.VMBytes = shape.DiskMiB << 20
	if request.Restore != nil {
		budget.VMBytes += shape.MemoryMiB << 20
		budget.RestoreBytes = (shape.MemoryMiB + shape.DiskMiB + 64) << 20
	}
	if reserved.Bytes > free || p.capacity.SafetyBytes > free-reserved.Bytes || budget.VMBytes+budget.RestoreBytes > free-reserved.Bytes-p.capacity.SafetyBytes || reserved.MemoryMiB > p.capacity.MaxMemoryMiB || shape.MemoryMiB+128 > p.capacity.MaxMemoryMiB-reserved.MemoryMiB || reserved.CPUs > int64(p.capacity.MaxVCPUs) || int64(shape.VCPUs) > int64(p.capacity.MaxVCPUs)-reserved.CPUs {
		return empty, fmt.Errorf("%w: host reservation exhausted", errs.ErrExhausted)
	}
	return budget, nil
}

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
			Request machine.FreshRequest
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
	if candidateBody, err = machine.InstanceHistory(ctx, tx, machine.GenerationIdentity{
		VMID:        currentMachine.Resource.VMID,
		OperationID: step.OperationID,
	}, currentMachine.Resource.Instance); err != nil {
		return currentMachine, record, result, err
	}
	var candidate machine.VMRecord
	if err = json.Unmarshal(candidateBody, &candidate); err != nil || !reflect.DeepEqual(candidate.Resource, currentMachine.Resource) {
		return currentMachine, record, result, fmt.Errorf("instance candidate changed: %v", err)
	}
	if versionBody, err = machine.VersionHistory(ctx, tx, machine.GenerationIdentity{
		VMID:        step.Candidate,
		OperationID: step.OperationID,
	}, step.Key); err != nil {
		return currentMachine, record, result, err
	}
	var version machine.RetainedVersion
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
