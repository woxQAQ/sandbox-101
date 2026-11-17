package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AdmitCaptureDiscard keeps the source VM and its disk. The caller explicitly
// supplies a completed stop operation; cleanup never infers that writers exited.
func (service *CaptureDiscard) AdmitCaptureDiscard(ctx context.Context, request CaptureDiscardRequest) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.Record{}, fmt.Errorf("%w: required CaptureDiscard capability port unavailable",

			errs.ErrManualRecovery)
	}

	return service.AdmitForParent(ctx, request, nil)
}

func (service *CaptureDiscard) AdmitForParent(ctx context.Context, request CaptureDiscardRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.Record{}, fmt.Errorf("%w: required CaptureDiscard capability port unavailable",

			errs.ErrManualRecovery)
	}

	var empty operation.Record
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.FailedOperationID, "op-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: capture discard requires tenant, sandbox, failed capture, stopped instance and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request CaptureDiscardRequest
	}{"capture_discard", request})
	hash := operation.Digest(frozen)
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
	source, err := LoadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, source.Input.Source.Candidate, parent); err != nil {
		return empty, err
	}
	stop, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  request.SandboxID,
			VMID:       source.Input.Source.Candidate,
			SnapshotID: source.Input.Snapshot.ID,
		},
	}
	input := CaptureDiscardInput{
		Request:  request,
		Source:   source,
		StopHash: stop.Hash,
	}
	if authority, ok := parent.(LifecycleParentAuthority); ok {
		input.Lifecycle = authority.LifecycleOperation()
	}
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:       accepted,
		Kind:        "capture_discard",
		Candidate:   source.Input.Snapshot.ID,
		RequestHash: hash,
	}); err != nil {
		return empty, err
	}
	payload := operation.Encode(input)
	step := CaptureDiscardStep{
		OperationID: accepted.ID,
		Hash:        operation.Digest(payload),
		Input:       input,
		Phase:       "admitted",
		Status:      "pending",
	}
	if err = service.captureDiscardAdmissionProof(ctx, tx, step); err != nil {
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
		StepKey:             "capture_discard",
		Action:              "capture_discard",
		InputHash:           step.Hash,
		Input:               payload,
		CandidateResourceID: source.Input.Snapshot.ID,
		ExpectedInstance:    request.ExpectedInstance,
		Status:              "pending",
		Body:                operation.Encode(step.Phase),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).MarkFailedDeleting(ctx, source.Input.Snapshot.ID)); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ClaimStopped(ctx, dbgen.ClaimStoppedParams{
		ID:       source.Input.Source.Candidate,
		Instance: request.ExpectedInstance,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	if err = quota.New(tx).TransferCostReference(ctx, quota.CostReference{
		TenantID:   request.TenantID,
		Kind:       "pending_storage",
		Key:        source.OperationID + ":capture",
		HolderKind: "engine_dependency",
		HolderID:   source.OperationID + ":capture",
	}, "cleanup", accepted.ID+":capture_discard"); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TransferBlockedCleanup(ctx, dbgen.TransferBlockedCleanupParams{
		ID:          "capture:" + source.OperationID,
		OperationID: source.OperationID,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "capture_discard:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "snapshot",
		ResourceID:   source.Input.Snapshot.ID,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = service.captureDiscardProof(ctx, tx, step); err != nil {
		return empty, err
	}
	if err = service.fault("capture_discard_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("capture_discard_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}

// verifyCaptureDiscardSource verifies the immutable failure and stopped source
// against the owner expected at this transaction boundary. Custody transfer and
// the admitted child operation are checked by their separate stage entrypoints.
func (service *CaptureDiscard) verifyCaptureDiscardSource(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep, expectedOwner string) (ownership.Sandbox, error) {
	var empty ownership.Sandbox
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	historical, err := LoadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(historical, source) || source.OperationID != request.FailedOperationID || source.Status != "failed" || source.State.ResumeRequired || source.State.Error == "" || source.Input.Request.TenantID != request.TenantID || source.Input.Request.SandboxID != request.SandboxID || source.Input.Request.ExpectedInstance != request.ExpectedInstance || resource.State.Witness != request.ExpectedInstance {
		return empty, fmt.Errorf("capture discard requires immutable resumed failure")
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, resource); err != nil {
		return empty, err
	}
	generations, err := service.proofs.GenerationHistory(ctx, tx, resource)
	if err != nil {
		return empty, err
	}
	if err = CheckGenerationCounts(ctx, tx, resource, append(generations, resource)); err != nil {
		return empty, err
	}
	stopped, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if stopped.Hash != input.StopHash || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != resource.OperationID || stopped.Input.SourceHash != resource.Hash || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != request.SandboxID || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance {
		return empty, fmt.Errorf("capture discard stop proof changed")
	}
	sandboxRecord, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	allowedState := sandboxRecord.State == "provisioning"
	if input.Lifecycle != "" {
		allowedState, err = service.ownership.VerifyCaptureDiscardParent(ctx, tx, step, sandboxRecord)
		if err != nil {
			return empty, err
		}
	}
	if !reflect.DeepEqual(sandboxRecord, source.Input.Sandbox) || sandboxRecord.SourceOperation != resource.OperationID || sandboxRecord.Instance != request.ExpectedInstance || !allowedState {
		return empty, fmt.Errorf("capture discard current sandbox ownership changed")
	}
	currentMachine, current, owner, err := LoadVM(ctx, tx, resource)
	if err != nil {
		return empty, err
	}
	currentOwnership, err := LoadOwnership(ctx, tx, resource.Candidate)
	if err != nil {
		return empty, err
	}
	if owner != expectedOwner || current != request.ExpectedInstance || currentOwnership.State != "stopped" || currentMachine.Deleting || !currentMachine.Verified || currentMachine.AgentReady || !reflect.DeepEqual(currentMachine.Resource, resource.Input.Resource) {
		return empty, fmt.Errorf("capture discard current stopped instance changed")
	}
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operation.Result
	projection, err := operation.New(tx).Projection(ctx, source.OperationID)
	if err != nil {
		return empty, err
	}
	if err = json.Unmarshal(projection.Request, &frozen); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(projection.Body, &result); err != nil {
		return empty, err
	}
	if operation.Digest(projection.Request) != projection.RequestHash || projection.Status != "failed" || projection.ScopeKind != "tenant" || projection.ScopeID != request.TenantID || frozen.Action != "snapshot" || frozen.Request != source.Input.Request || result.Error != source.State.Error || result.SnapshotID != source.Input.Snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandboxRecord.ID {
		return empty, fmt.Errorf("capture discard original terminal result changed")
	}
	snapshot := source.Input.Snapshot
	if snapshot.OperationID != source.OperationID || snapshot.TenantID != request.TenantID || snapshot.SourceVM != resource.Candidate || snapshot.SourceInstance != request.ExpectedInstance || snapshot.Manifest != nil || snapshot.Dir != filepath.Join(service.store.Directory, "snapshots", snapshot.ID) {
		return empty, fmt.Errorf("capture discard destination changed")
	}
	return sandboxRecord, nil
}
