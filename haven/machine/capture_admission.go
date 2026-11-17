package machine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AdmitCapture fixes the source instance, destination, authorization and peak
// responsibility before any pause. Sandbox checkpoint/pause publication is separate.
func (service *Captures) AdmitCapture(ctx context.Context, request CaptureRequest) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
			Record{}, fmt.
			Errorf("%w: required Captures capability port unavailable",

				errs.ErrManualRecovery)
	}

	return service.AdmitForParent(ctx, request, nil)
}

func (service *Captures) AdmitForParent(ctx context.Context, request CaptureRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
			Record{}, fmt.
			Errorf("%w: required Captures capability port unavailable",

				errs.ErrManualRecovery)
	}

	var empty operation.Record
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.ExpectedInstance, "run-") {
		return empty, fmt.Errorf("%w: capture requires tenant, sandbox, key and expected instance", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request CaptureRequest
	}{"snapshot", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(service.store.Database).Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	free, err := loom.FreeBytes(service.store.Directory)
	if err != nil {
		return empty, err
	}
	compatibility, cpu := service.backend.Compatibility(), host.CPUIdentity()
	service.admission.Lock()
	defer service.admission.Unlock()
	closing := service.lifetime.Closing() || service.store.Reviewed
	if closing {
		return empty, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	sandbox, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err == sql.ErrNoRows {
		return empty, fmt.Errorf("%w: sandbox not found", errs.ErrNotFound)
	}
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if sandbox.Instance != request.ExpectedInstance || (sandbox.State != "provisioning" && sandbox.State != "running") {
		return empty, fmt.Errorf("%w: capture source instance or state changed", errs.ErrConflict)
	}
	source, err := LoadResourceStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone") || source.Status != "succeeded" || source.State.Witness != request.ExpectedInstance || source.State.Observation == nil || source.State.DiskIdentity == nil || source.Input.TenantID != request.TenantID || source.Input.SandboxID != sandbox.ID || source.Candidate != sandbox.VMID {
		return empty, fmt.Errorf("%w: capture requires original successful resource witness", errs.ErrManualRecovery)
	}
	currentMachine, instance, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != request.ExpectedInstance || currentMachine.Deleting || !currentMachine.Verified || !currentMachine.AgentReady || !reflect.DeepEqual(currentMachine.Resource, source.Input.Resource) {
		return empty, fmt.Errorf("%w: capture source is busy or unverified", errs.ErrConflict)
	}
	tenant, err := authorization.New(tx).Limits(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	shape := currentMachine.Resource.Config.Shape
	cost := shape.MemoryMiB + shape.DiskMiB
	responsibilities := quota.New(tx)
	usage, err := responsibilities.Usage(ctx, request.TenantID)
	if err != nil {
		return empty, err
	}
	if tenant.MaxSandboxes < 0 || tenant.MaxStorageMiB < 0 || usage.Slots > tenant.MaxSandboxes || usage.StorageMiB > tenant.MaxStorageMiB || cost > tenant.MaxStorageMiB-usage.StorageMiB {
		return empty, fmt.Errorf("%w: capture tenant quota exhausted", errs.ErrExhausted)
	}
	held, err := responsibilities.ReservedBytes(ctx)
	if err != nil {
		return empty, err
	}
	reserve := (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20
	if held > free || service.safetyBytes > free-held || reserve > free-held-service.safetyBytes {
		return empty, fmt.Errorf("%w: capture host peak reservation exhausted", errs.ErrExhausted)
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  sandbox.ID,
			VMID:       sandbox.VMID,
			SnapshotID: operation.NewID("snap-"),
		},
	}
	publication := SnapshotRecord{
		ID:             accepted.Result.SnapshotID,
		TenantID:       request.TenantID,
		OperationID:    accepted.ID,
		SourceVM:       sandbox.VMID,
		SourceInstance: request.ExpectedInstance,
		Dir:            filepath.Join(service.store.Directory, "snapshots", accepted.Result.SnapshotID),
	}
	input := CaptureInput{
		Request:      request,
		Source:       source,
		Sandbox:      sandbox,
		Snapshot:     publication,
		Backend:      compatibility,
		Architecture: runtime.GOARCH,
		CPU:          cpu,
	}
	payload := operation.Encode(input)
	if err = AdmitResourceParent(ctx, tx, parent, accepted, "capture", publication.ID, hash); err != nil {
		return empty, err
	}
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
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
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "capture",
		Action:              "snapshot",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: publication.ID,
		ExpectedInstance:    request.ExpectedInstance,
		Status:              "pending",
		Body: operation.Encode(CaptureState{
			Phase: "admitted",
		}),
	})); err != nil {
		return empty, err
	}
	if err = admitCapturePublication(ctx, tx, publication); err != nil {
		return empty, err
	}
	if err = responsibilities.Reserve(ctx, quota.Reservation{
		Owner: "snapshot:" + publication.ID,
		Bytes: reserve,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        request.TenantID,
			Kind:            "pending_storage",
			Key:             accepted.ID + ":capture",
			SourceOperation: accepted.ID,
		},
		StorageMiB: cost,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddCostReference(ctx, quota.CostReference{
		TenantID:   request.TenantID,
		Kind:       "pending_storage",
		Key:        accepted.ID + ":capture",
		HolderKind: "engine_dependency",
		HolderID:   accepted.ID + ":capture",
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         sandbox.VMID,
		HolderKind: "capture",
		HolderID:   accepted.ID,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         currentMachine.Resource.Config.Image,
		HolderKind: "capture",
		HolderID:   accepted.ID,
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ClaimRunning(ctx, dbgen.ClaimRunningParams{
		ID:       sandbox.VMID,
		Instance: request.ExpectedInstance,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	admitted, err := LoadCapture(ctx, tx, accepted.ID)
	if err != nil {
		return empty, err
	}
	if _, err = service.captureProof(ctx, tx, admitted); err != nil {
		return empty, err
	}
	if err = service.fault("capture_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("capture_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
