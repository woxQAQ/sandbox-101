package sandbox

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func (service *VMProvisioning) AdmitRestore(ctx context.Context, request machine.RestoreRequest) (operation.Record, error) {
	return service.AdmitRestoreWithParent(ctx, request, nil)
}

func (service *VMProvisioning) AdmitRestoreWithParent(ctx context.Context, request machine.RestoreRequest, parent machine.ResourceParent) (operation.Record, error) {
	link, _ := parent.(*LifecycleAdmission)
	var empty operation.Record
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: restore requires scoped snapshot, stopped instance, stop proof and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.RestoreRequest
	}{"restore", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(service.store.Database).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := service.proofs.Snapshots().LoadRestoreSource(ctx, service.store.Database, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = service.backend.Check(ctx, source.Snapshot.Manifest.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, source.Snapshot.Manifest.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	free, err := loom.FreeBytes(service.store.Directory)
	if err != nil {
		return empty, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	closing := service.lifetime.Closing() || service.store.Reviewed
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	currentImage, err := image.LoadManifest(ctx, tx, manifest.Digest)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(currentImage, manifest) {
		return empty, fmt.Errorf("%w: image publication changed during preflight", errs.ErrConflict)
	}
	if link != nil && (link.Input.Rollback == nil || !reflect.DeepEqual(source, link.Input.Rollback.Source)) {
		return empty, fmt.Errorf("%w: rollback frozen restore source changed", errs.ErrConflict)
	}
	currentSnapshot, err := service.proofs.Snapshots().LoadRestoreSource(ctx, tx, request.TenantID, request.SnapshotID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(currentSnapshot, source) {
		return empty, fmt.Errorf("restore source changed during preflight")
	}
	destination, err := LoadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if err = machine.AllocationCleanupFence(ctx, tx, destination.VMID, parent); err != nil {
		return empty, err
	}
	if (destination.State != "provisioning" && (link == nil || link.Input.Command.Action != "rollback" || destination.State != "running")) || destination.Instance != request.ExpectedInstance {
		return empty, fmt.Errorf("restore requires original undelivered stopped sandbox")
	}
	prior, err := machine.LoadResourceStep(ctx, tx, destination.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, prior); err != nil {
		return empty, err
	}
	if _, err = service.proofs.GenerationHistory(ctx, tx, prior); err != nil {
		return empty, err
	}
	stopped, err := machine.LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != prior.OperationID || stopped.Input.SourceHash != prior.Hash || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != destination.ID {
		return empty, fmt.Errorf("restore stop proof differs from current generation")
	}
	currentMachine, current, owner, err := machine.LoadVM(ctx, tx, prior)
	if err != nil {
		return empty, err
	}

	currentOwnership, err := machine.LoadOwnership(ctx, tx, prior.Candidate)
	if err != nil {
		return empty, err
	}
	if owner != "" || currentOwnership.State != "stopped" || current != request.ExpectedInstance || current != prior.State.Witness || !currentMachine.Verified || currentMachine.AgentReady || currentMachine.Deleting {
		return empty, fmt.Errorf("restore current stopped ownership changed")
	}
	config := source.Snapshot.Manifest.Config
	if !reflect.DeepEqual(currentMachine.Resource.Config.Shape, config.Shape) || currentMachine.Resource.Config.Image != config.Image {
		return empty, fmt.Errorf("restore target shape/image mismatch")
	}
	refs, err := quota.New(tx).CountResourceHolders(ctx, "vm", prior.Candidate)
	if err != nil {
		return empty, err
	}
	instances, err := machine.RetainedInstanceCount(ctx, tx, prior.Candidate)
	if err != nil {
		return empty, err
	}
	versions, err := machine.RetainedVersionCount(ctx, tx, prior.Candidate)
	if err != nil {
		return empty, err
	}
	if refs != 1 || instances != 1+int64(len(prior.Input.Ancestors)) || versions != instances {
		return empty, fmt.Errorf("restore target has unrecorded resource users or versions")
	}
	tenant, err := authorization.New(tx).Limits(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	usage, err := quota.New(tx).FundedUsage(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	shape := config.Shape
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || usage.Slots > tenant.MaxSandboxes || usage.StorageMiB > tenant.MaxStorageMiB || shape.DiskMiB > tenant.MaxStorageMiB-usage.StorageMiB {
		return empty, fmt.Errorf("restore tenant peak quota exhausted")
	}
	held, err := quota.New(tx).ReservedBytes(ctx)
	if err != nil {
		return empty, err
	}
	reserve := (shape.MemoryMiB + shape.DiskMiB + 64) << 20
	if held > free || service.capacity.SafetyBytes > free-held || reserve > free-held-service.capacity.SafetyBytes {
		return empty, fmt.Errorf("restore host peak reservation exhausted")
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  destination.ID,
			VMID:       prior.Candidate,
			SnapshotID: request.SnapshotID,
		},
	}
	resource := prior.Input.Resource
	resource.Instance = operation.NewID("run-")
	resource.Config = config
	resource.RunDir = filepath.Join(resource.Dir, "runs", resource.Instance)
	resource.RestoreDir = filepath.Join(resource.Dir, "versions", accepted.ID, "restore")
	resource.Disk = filepath.Join(resource.Dir, "versions", accepted.ID, "disk.raw")
	ancestors := append([]ownership.GenerationRef(nil), prior.Input.Ancestors...)
	ancestors = append(ancestors, ownership.GenerationRef{
		OperationID: prior.OperationID,
		InputHash:   prior.Hash,
	})
	budget, err := CurrentSlotBudget(ctx, tx, destination, prior)
	if err != nil {
		return empty, err
	}
	input := machine.ResourceInput{
		SlotAssignment: destination.SlotAssignment,
		SlotBudget:     budget,
		TemplateID:     prior.Input.TemplateID,
		Resource:       resource,
		Image:          manifest,
		TenantID:       request.TenantID,
		SandboxID:      destination.ID,
		Restore:        &source,
		Ancestors:      ancestors,
		DiskKey:        accepted.ID,
		SlotOperation:  prior.Input.SlotOperation,
		RuntimeBytes:   prior.Input.RuntimeBytes,
		BackingHolder:  accepted.ID,
		StopOperation:  request.StopOperationID,
	}
	if err = machine.AdmitResourceParent(ctx, tx, parent, machine.ResourceAdmission{
		Child:       accepted,
		Kind:        "restore",
		Candidate:   resource.VMID,
		RequestHash: hash,
	}); err != nil {
		return empty, err
	}
	previous := destination
	currentMachine = machine.VMRecord{
		Resource: resource,
	}
	destination.State = "provisioning"
	destination.OperationID = accepted.ID
	destination.SourceOperation = accepted.ID
	destination.Instance = ""
	if err = operation.New(tx).Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = machine.AdmitProvisionIntent(ctx, tx, machine.ProvisionIntent{
		OperationID:      accepted.ID,
		Action:           "restore",
		ExpectedInstance: request.ExpectedInstance,
		Input:            input,
	}); err != nil {
		return empty, err
	}
	if err = machine.ClaimRestoreGeneration(ctx, tx, machine.RestoreGenerationClaim{
		ExpectedInstance: request.ExpectedInstance,
		OperationID:      accepted.ID,
		VM:               currentMachine,
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ReuseSandboxForProvision(ctx, dbgen.ReuseSandboxForProvisionParams{
		ID:            destination.ID,
		Tenant:        request.TenantID,
		PreviousState: previous.State,
		PreviousBody:  operation.Encode(previous),
		NewBody:       operation.Encode(destination),
	})); err != nil {
		return empty, err
	}
	if err = machine.RetainGenerationIntent(ctx, tx, accepted.ID, currentMachine); err != nil {
		return empty, err
	}
	responsibilities := quota.New(tx)
	if err = responsibilities.Reserve(ctx, quota.Reservation{
		Owner: "restore:" + accepted.ID,
		Bytes: reserve,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        request.TenantID,
			Kind:            "pending_storage",
			Key:             accepted.ID + ":resource",
			SourceOperation: accepted.ID,
		},
		StorageMiB: shape.DiskMiB,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   request.TenantID,
		Kind:       "pending_storage",
		Key:        accepted.ID + ":resource",
		HolderKind: "engine_dependency",
		HolderID:   accepted.ID + ":resource",
	}); err != nil {
		return empty, err
	}
	if err = machine.PinSnapshotSource(ctx, tx, request.TenantID, machine.SourceLease{
		SnapshotID:  request.SnapshotID,
		OperationID: accepted.ID,
		StepKey:     "resource",
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.TransferResourceReference(ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         resource.VMID,
		HolderKind: "sandbox",
		HolderID:   destination.ID,
	}, "step", accepted.ID+":resource"); err != nil {
		return empty, err
	}
	if err = responsibilities.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         resource.Config.Image,
		HolderKind: "step",
		HolderID:   accepted.ID + ":resource",
	}); err != nil {
		return empty, err
	}
	if err = service.fault("restore_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("restore_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
