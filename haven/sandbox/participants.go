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

// OwnershipHistory reads canonical sandbox ownership without retaining a store,
// runtime or application. The transaction is always supplied by the workflow.
type OwnershipHistory struct{}

func (OwnershipHistory) ReadSandbox(ctx context.Context, query store.DBTX, id, tenant string) (ownership.Sandbox, error) {
	return LoadSandbox(ctx, query, id, tenant)
}

// SupplyCleanupHistory holds no connection or running service.
type SupplyCleanupHistory struct {
	OwnershipHistory
}

func (SupplyCleanupHistory) SettingsCount(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSandboxSettings(ctx, id)
}

// StopHistory retains no running service or mutable connection.
type StopHistory struct {
	OwnershipHistory
}

func (StopHistory) VerifyStopParent(ctx context.Context, query store.DBTX, proof machine.StopParentProof) (string, error) {
	step, source, record := proof.Step, proof.Source, proof.Sandbox
	parent, err := LoadLifecycleStep(ctx, query, step.Input.Lifecycle)
	if err != nil {
		return "", err
	}
	link, _, err := machine.LoadResourceLink(ctx, query, parent.OperationID, "stop")
	if err != nil {
		return "", err
	}
	fence, err := LifecycleFence(ctx, query, record.ID)
	if err != nil {
		return "", err
	}
	if (parent.Input.Command.Action != "rollback" && parent.Input.Command.Action != "release") || operation.Terminal(parent.Status) || parent.Input.OwnerTenantID != source.Input.TenantID || parent.Input.Source.OperationID != source.OperationID || parent.Input.Source.InputHash != source.Hash || parent.Input.SourceInstance != step.Input.Request.ExpectedInstance || link.ChildID != step.OperationID || fence != parent.OperationID {
		return "", fmt.Errorf("rollback stop no longer owns original instance")
	}
	return parent.Input.Sandbox.State, nil
}

// CaptureDiscardHistory is a stateless reader of retained release authority.
type CaptureDiscardHistory struct {
	OwnershipHistory
}

func (CaptureDiscardHistory) VerifyCaptureDiscardParent(ctx context.Context, query store.DBTX, step machine.CaptureDiscardStep, record ownership.Sandbox) (bool, error) {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	parent, err := LoadLifecycleStep(ctx, query, input.Lifecycle)
	if err != nil {
		return false, err
	}
	fence, err := LifecycleFence(ctx, query, record.ID)
	if err != nil {
		return false, err
	}
	found := false
	for _, ref := range parent.Input.CaptureFailures {
		if ref.OperationID == source.OperationID && ref.InputHash == source.Hash {
			found = true
		}
	}
	stopLink, _, err := machine.LoadResourceLink(ctx, query, parent.OperationID, "stop")
	if err != nil {
		return false, err
	}
	if !found || operation.Terminal(parent.Status) || parent.Input.Command.Action != "release" || parent.Input.OwnerTenantID != request.TenantID || parent.Input.Sandbox.ID != request.SandboxID || parent.Input.Source.OperationID != resource.OperationID || parent.Input.SourceInstance != request.ExpectedInstance || stopLink.ChildID != request.StopOperationID || fence != parent.OperationID {
		return false, fmt.Errorf("capture discard release authority changed")
	}
	return record.State == parent.Input.Sandbox.State, nil
}

// VerifyCaptureDiscardLink applies only after child admission has committed the
// original parent link. Parent authority is verified separately in the same snapshot.
func (CaptureDiscardHistory) VerifyCaptureDiscardLink(ctx context.Context, query store.DBTX, step machine.CaptureDiscardStep) error {
	link, _, err := machine.LoadResourceLink(ctx, query, step.Input.Lifecycle, "failed-capture:"+step.Input.Source.OperationID)
	if err != nil {
		return err
	}
	if link.ChildID != step.OperationID {
		return fmt.Errorf("capture discard release child changed")
	}
	return nil
}

// ActivationHistory is a stateless proof reader, usable with the read-only
// startup probe before application services or workers exist.
type ActivationHistory struct{}

func (ActivationHistory) ReadActivationProof(ctx context.Context, query store.DBTX, id string) (machine.ActivationProof, error) {
	step, err := LoadActivation(ctx, query, id)
	if err != nil {
		return machine.ActivationProof{}, err
	}
	return step.ResourceProof(), nil
}

// ResourceProof projects an already decoded frozen step without consulting
// current sandbox ownership or deriving a replacement resource witness.
func (step ActivationStep) ResourceProof() machine.ActivationProof {
	return machine.ActivationProof{
		OperationID:       step.OperationID,
		Key:               step.Key,
		Hash:              step.Hash,
		Status:            step.Status,
		Phase:             step.Phase,
		ResourceOperation: step.Input.ResourceOperation,
		ResourceHash:      step.Input.ResourceHash,
		ExpectedInstance:  step.Input.ExpectedInstance,
	}
}

// DeletionHistory reads the original business authority without retaining a
// connection or capturing a running application.
type DeletionHistory struct {
	OwnershipHistory
}

func (DeletionHistory) CurrentSlotBudget(ctx context.Context, query store.DBTX, record ownership.Sandbox, source machine.ResourceStep) (quota.SlotBudget, error) {
	return CurrentSlotBudget(ctx, query, record, source)
}

func (DeletionHistory) VerifyReleaseDestination(ctx context.Context, tx *sql.Tx, step machine.ResourceStep) error {
	return ValidateReleaseSlotDestination(ctx, tx, step)
}

func (DeletionHistory) ResumeDestination(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) (machine.ResumeDestinationProof, error) {
	parent, err := ResumeDestination(ctx, tx, destination)
	if err != nil {
		return machine.ResumeDestinationProof{}, err
	}
	return machine.ResumeDestinationProof{
		OperationID: parent.OperationID,
		TenantID:    parent.Input.OwnerTenantID,
		SandboxID:   parent.Input.Sandbox.ID,
	}, nil
}

// DeletionHandoff participates in the transaction owned by physical deletion.
type DeletionHandoff struct{}

func (DeletionHandoff) RestoreResumeSlot(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination, holder string) error {
	return RestoreResumeSlot(ctx, tx, destination, holder)
}

func (DeletionHandoff) RestorePausedSandbox(ctx context.Context, tx *sql.Tx, destination *machine.QuotaDestination) error {
	return RestorePausedSandbox(ctx, tx, destination)
}

func (DeletionHandoff) TerminateSandbox(ctx context.Context, tx *sql.Tx, termination machine.SandboxTermination) error {
	record := termination.Sandbox
	return store.RequireOne(dbgen.New(tx).TerminateLiveSandbox(ctx, dbgen.TerminateLiveSandboxParams{
		ID:                record.ID,
		Tenant:            termination.TenantID,
		OperationID:       optionalID(termination.OperationID),
		SourceOperationID: optionalID(record.SourceOperation),
		Instance:          record.Instance,
	}))
}

func (DeletionHandoff) CompleteRelease(ctx context.Context, tx *sql.Tx, step machine.ResourceStep) error {
	input := step.Input
	if input.SlotDestination != nil && input.SlotDestination.Kind == "sleep" {
		parent, err := LoadLifecycleStep(ctx, tx, input.SlotDestination.OperationID)
		if err != nil {
			return err
		}
		if parent.Input.Command.Action == "resume" {
			if err = RestorePausedSandbox(ctx, tx, input.SlotDestination); err != nil {
				return err
			}
		}
	}
	if input.SlotDestination != nil && input.SlotDestination.Kind == "claim" {
		if err := RetireSelectedPool(ctx, tx, step); err != nil {
			return err
		}
	}
	return nil
}

// ProvisionHistory retains no connection or running service.
type ProvisionHistory struct{}

func (ProvisionHistory) VerifyProvisionIntent(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step machine.ResourceStep) (machine.VMRecord, ownership.Sandbox, operation.Result, error) {
	return VerifyProvisionIntent(proofs, ctx, tx, step)
}

// ProvisionHandoff writes only through the transaction owned by the resource
// workflow; activation delivery remains under the coordinator's VM guard.
type ProvisionHandoff struct{}

func (ProvisionHandoff) PublishInstance(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, instance string) error {
	return store.RequireOne(dbgen.New(tx).PublishSandboxInstance(ctx, dbgen.PublishSandboxInstanceParams{
		Instance:          instance,
		SourceOperationID: optionalID(record.SourceOperation),
		VmID:              record.VMID,
		ID:                record.ID,
		Tenant:            record.TenantID,
	}))
}

func (ProvisionHandoff) BlockActivation(ctx context.Context, tx *sql.Tx, id string) error {
	if activation, lookup := LoadActivation(ctx, tx, id); lookup == nil && !operation.Terminal(activation.Status) {
		if err := transitionActivation(ctx, tx, activation, "blocked", "failed"); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	return nil
}

// AdmitIn joins resource intent and all charges to the caller's transaction.
// A batch commits every destination before dispatching any effect.
func (p *Provisioning) AdmitIn(ctx context.Context, tx *sql.Tx, admission ProvisionAdmission, free int64) (operation.State, error) {
	var empty operation.State
	request := admission.Request
	tenantID, frozen := request.TenantID, request.Frozen
	selected, manifest, restore := request.Template, request.Image, request.Restore
	claim, target := admission.Claim, admission.Target
	if err := p.verifyProvisionSources(ctx, tx, admission); err != nil {
		return empty, err
	}
	allocation, err := p.checkProvisionBudget(ctx, tx, admission, free)
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
	accepted := operation.State{
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
	resource.Dir = filepath.Join(p.store.Directory, "vms", resource.VMID)
	resource.Disk = filepath.Join(resource.Dir, "disks", accepted.ID+".raw")
	resource.RunDir = filepath.Join(resource.Dir, "runs", resource.Instance)
	resource.ImageDir = filepath.Join(p.store.Directory, "images", resource.Config.Image[7:])
	action := "cold_start"
	if restore != nil {
		action = "clone"
		resource.Disk = filepath.Join(resource.Dir, "versions", accepted.ID, "disk.raw")
		resource.RestoreDir = filepath.Join(resource.Dir, "versions", accepted.ID, "restore")
	}
	childAdmission := machine.ResourceAdmission{
		Child:     accepted,
		Kind:      action,
		Candidate: resource.VMID,
		Request:   frozen,
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
		count, lookup := quota.CountMatchingCost(ctx, tx, quota.Cost{
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
		if err = machine.AdmitResourceParent(ctx, tx, target.Parent, childAdmission); err != nil {
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
	identity := NewSandboxBirth(selected, provisionSource)
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
		State:           ownership.StateProvisioning,
		SourceOperation: accepted.ID,
	}
	currentMachine := machine.VMRecord{
		Resource: resource,
	}
	if err = operation.New(tx).RecordAdmission(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Request:   frozen,
		Status:    accepted.Status,
		Result:    accepted.Result,
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
		if err = insertSandbox(ctx, tx, destination); err != nil {
			return empty, err
		}
	} else {
		if err = replaceSandbox(ctx, tx, target.Sandbox, destination); err != nil {
			return empty, err
		}
		if err = quota.MoveCostReference(ctx, tx, quota.CostReference{
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
		if err = machine.AdmitResourceParent(ctx, tx, target.Parent, childAdmission); err != nil {
			return empty, err
		}
	}
	if err = machine.AdmitCandidate(ctx, tx, accepted.ID, currentMachine); err != nil {
		return empty, err
	}
	if err = quota.Reserve(ctx, tx, quota.Reservation{
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
		if err = quota.AddCost(ctx, tx, quota.Cost{
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
		if err = quota.AddCostReference(ctx, tx, quota.CostReference{
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
		if err = quota.AddResourceReference(ctx, tx, quota.ResourceReference{
			Kind:       reference.kind,
			ID:         reference.id,
			HolderKind: "step",
			HolderID:   accepted.ID + ":resource",
		}); err != nil {
			return empty, err
		}
	}
	if restore != nil {
		if err = quota.Reserve(ctx, tx, quota.Reservation{
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
	var activation *Settings
	if target != nil {
		activation = target.Activation
	}
	if claim != nil {
		if activation != nil {
			return empty, errors.New("resource has two activation admissions")
		}
		activation = &claim.Settings
		if err = operation.New(tx).RecordCommandMAC(ctx, accepted.ID, claim.MAC); err != nil {
			return empty, err
		}
	}
	if activation != nil {
		if err = p.activations.AdmitIn(tx, accepted, input, *activation); err != nil {
			return empty, err
		}
	}
	return accepted, nil
}
