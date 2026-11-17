package machine

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func (service *CaptureCleanup) Admit(ctx context.Context, request CaptureCleanupRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
				Record{},
			fmt.Errorf(
				"%w: required CaptureCleanup capability port unavailable",

				errs.ErrManualRecovery,
			)
	}

	var empty operation.Record
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.CaptureOperationID, "op-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: raw capture cleanup requires original capture and stop proof", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request CaptureCleanupRequest
	}{"capture_cleanup", request})
	hash := operation.Digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return empty, err
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
	capture, err := LoadCapture(ctx, tx, request.CaptureOperationID)
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, capture.Input.Source.Candidate, parent); err != nil {
		return empty, err
	}
	if authority, ok := parent.(CaptureCleanupParentAuthority); ok {
		if err = authority.VerifyCaptureCleanup(ctx, tx, capture, request.StopOperationID); err != nil {
			return empty, err
		}
	}
	stopped, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if capture.Input.Request.TenantID != request.TenantID || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != capture.Input.Source.OperationID || stopped.Input.SourceHash != capture.Input.Source.Hash || stopped.Input.Request.ExpectedInstance != capture.Input.Request.ExpectedInstance || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != capture.Input.Request.SandboxID {
		return empty, fmt.Errorf("%w: raw capture stop proof differs from captured generation", errs.ErrConflict)
	}
	if err = CaptureCleanupProof(ctx, tx, capture, CaptureCleanupTarget{
		VMID:     capture.Input.Source.Candidate,
		Instance: capture.Input.Request.ExpectedInstance,
		Status:   "pending",
	}); err != nil {
		return empty, err
	}
	source, err := LoadResourceStep(ctx, tx, capture.Input.Source.OperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(source, capture.Input.Source) {
		return empty, fmt.Errorf("%w: raw capture source history changed", errs.ErrConflict)
	}
	currentMachine, instance, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != source.State.Witness || currentMachine.Deleting || currentMachine.AgentReady || !currentMachine.Verified {
		return empty, fmt.Errorf("%w: raw capture source is not stopped and unowned", errs.ErrConflict)
	}
	sandbox, err := service.ownership.ReadSandbox(ctx, tx, capture.Input.Request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if sandbox.SourceOperation != source.OperationID || sandbox.Instance != source.State.Witness || sandbox.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw capture source ownership changed", errs.ErrConflict)
	}
	operations := operation.New(tx)
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  sandbox.ID,
			VMID:       source.Candidate,
			SnapshotID: capture.Input.Snapshot.ID,
		},
	}
	input := captureCleanupInput{
		Request: request,
		Capture: capture,
		Stop:    stopped,
	}
	payload := operation.Encode(input)
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:       accepted,
		Kind:        "capture_cleanup",
		Candidate:   source.Candidate,
		RequestHash: hash,
	}); err != nil {
		return empty, err
	}
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
		StepKey:             "capture_cleanup",
		Action:              "capture_cleanup",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TransferPendingCleanupResource(ctx, dbgen.TransferPendingCleanupResourceParams{
		ID:           "capture:" + capture.OperationID,
		OperationID:  capture.OperationID,
		ResourceKind: "capture",
		ResourceID:   source.Candidate,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "raw:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "capture_raw",
		ResourceID:   source.Candidate,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ClaimStopped(ctx, dbgen.ClaimStoppedParams{
		ID:       source.Candidate,
		Instance: source.State.Witness,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	if err = service.fault("capture_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
