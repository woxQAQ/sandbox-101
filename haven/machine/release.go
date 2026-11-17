package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AdmitRelease revokes the logical sandbox and gives cleanup an independent
// intent. An immutable successful source witness is required; unresolved or
// failed generation attempts retain their responsibility for review.
func (service *VMDeletion) AdmitRelease(ctx context.Context, request ReleaseRequest) (operation.Record, error) {
	if service.
		ownership ==
		nil || service.handoff ==
		nil ||
		service.
			usage == nil {
		return operation.Record{}, fmt.Errorf("%w: required VMDeletion capability port unavailable",
			errs.ErrManualRecovery,
		)
	}

	return service.AdmitReleaseForParent(ctx, request, nil)
}

func (service *VMDeletion) AdmitReleaseForParent(ctx context.Context, request ReleaseRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil || service.handoff ==
		nil ||
		service.
			usage == nil {
		return operation.Record{}, fmt.Errorf("%w: required VMDeletion capability port unavailable",
			errs.ErrManualRecovery,
		)
	}

	var empty operation.Record
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.SandboxID == "" {
		return empty, fmt.Errorf("%w: tenant, sandbox and a key of 1..256 bytes are required", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request ReleaseRequest
	}{"release", request})
	hash := operation.Digest(frozen)
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
	operations := operation.New(tx)
	if cached, found, err := operations.Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	record, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, fmt.Errorf("%w: sandbox not found", errs.ErrNotFound)
	}
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, record.VMID, parent); err != nil {
		return empty, err
	}
	if record.State == "terminated" {
		return empty, fmt.Errorf("%w: reuse the admitted release key", errs.ErrConflict)
	}
	source, err := LoadResourceStep(ctx, tx, record.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "succeeded" || source.State.Phase != "complete" || source.State.Observation == nil || source.State.DiskIdentity == nil || source.State.Witness == "" || source.State.Witness != record.Instance || source.State.Witness != source.Input.Resource.Instance || source.Input.SandboxID != record.ID || source.Input.TenantID != record.TenantID || source.Candidate != record.VMID {
		return empty, fmt.Errorf("%w: release requires the frozen successful generation witness", errs.ErrManualRecovery)
	}
	currentMachine, instance, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if instance != source.State.Witness || owner != "" || !currentMachine.Verified || currentMachine.Deleting {
		return empty, fmt.Errorf("%w: release current ownership differs from the historical witness", errs.ErrManualRecovery)
	}

	instances, err := RetainedInstanceCount(ctx, tx, record.VMID)
	if err != nil {
		return empty, err
	}
	versions, err := RetainedVersionCount(ctx, tx, record.VMID)
	if err != nil {
		return empty, err
	}
	if _, err = dbgen.New(tx).CountVMBackings(ctx, record.VMID); err != nil {
		return empty, err
	}
	holders, err := quota.New(tx).CountResourceHolders(ctx, "vm", record.VMID)
	if err != nil {
		return empty, err
	}
	warm, err := service.usage.VMUsers(ctx, tx, record.VMID)
	if err != nil {
		return empty, err
	}
	captures, err := ReleaseCaptures(ctx, tx, record.VMID, source.State.Witness)
	if err != nil {
		return empty, err
	}
	generations, err := service.proofs.CleanupGenerations(ctx, tx, source, quota.Holder{
		Kind: "active_use",
		ID:   "sandbox:" + record.ID,
	})
	if err != nil {
		return empty, err
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if err = CheckGenerationCounts(ctx, tx, source, generations); err != nil {
		return empty, err
	}
	if instances != int64(len(generations)) || versions != int64(len(generations)) || warm != 0 || holders != 1+int64(len(captures)) {
		return empty, fmt.Errorf("%w: cold release has additional or missing resource responsibilities", errs.ErrManualRecovery)
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: record.ID,
			VMID:      record.VMID,
		},
	}
	budget, err := service.ownership.CurrentSlotBudget(ctx, tx, record, source)
	if err != nil {
		return empty, err
	}
	input := source.Input
	input.SlotBudget = budget
	input.SlotAssignment = record.SlotAssignment
	input.Captures = captures
	input.SourceOperation = source.OperationID
	input.DiskIdentity = source.State.DiskIdentity
	input.RestoreMaterials = CopyRestoreMaterials(source.State.Materials)
	if authority, ok := parent.(ReleaseParentAuthority); ok {
		destination, selected, err := authority.ReleaseDestination(ctx, tx, record, source)
		if err != nil {
			return empty, err
		}
		if selected {
			input.SlotDestination = destination
		}
	}
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:       accepted,
		Kind:        "release",
		Candidate:   record.VMID,
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
		Status:    accepted.Status,
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	payload := operation.Encode(input)
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "resource",
		Action:              "release",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: record.VMID,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Body: operation.Encode(ResourceState{
			Phase: "admitted",
		}),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "cleanup:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "vm",
		ResourceID:   record.VMID,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = service.handoff.TerminateSandbox(ctx, tx, SandboxTermination{
		Sandbox:     record,
		OperationID: accepted.ID,
		TenantID:    record.TenantID,
	}); err != nil {
		return empty, err
	}
	currentMachine.Deleting = true
	if err = store.RequireOne(dbgen.New(tx).ClaimCurrentBody(ctx, dbgen.ClaimCurrentBodyParams{
		ID:       record.VMID,
		Instance: source.State.Witness,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
		Body: operation.Encode(currentMachine),
	})); err != nil {
		return empty, err
	}
	charges := []struct {
		kind string
		key  string
	}{
		{input.SlotBudget.Kind, record.ID},
	}
	for _, generation := range generations {
		charges = append(charges, struct {
			kind string
			key  string
		}{"vm_disk", generation.Input.DiskKey})
	}
	for _, charge := range charges {
		if err = quota.New(tx).TransferCostReference(ctx, quota.CostReference{
			TenantID:   record.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: "active_use",
			HolderID:   "sandbox:" + record.ID,
		}, "cleanup", accepted.ID+":resource"); err != nil {
			return empty, err
		}
	}
	if err = quota.New(tx).TransferResourceReference(ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         record.VMID,
		HolderKind: "sandbox",
		HolderID:   record.ID,
	}, "cleanup", accepted.ID+":resource"); err != nil {
		return empty, err
	}
	for _, capture := range captures {
		if err = store.RequireOne(dbgen.New(tx).TransferPendingCleanup(ctx, dbgen.TransferPendingCleanupParams{
			ID:          "capture:" + capture.OperationID,
			OperationID: capture.OperationID,
		})); err != nil {
			return empty, err
		}
		if err = quota.New(tx).RemoveResourceReference(ctx, quota.ResourceReference{
			Kind:       "vm",
			ID:         record.VMID,
			HolderKind: "capture",
			HolderID:   capture.OperationID,
		}); err != nil {
			return empty, err
		}
	}
	parentStatus, parentBody, err := operations.ResultState(ctx, source.OperationID)
	if err != nil {
		return empty, err
	}
	if !operation.Terminal(parentStatus) {
		var result operation.Result
		if err = json.Unmarshal(parentBody, &result); err != nil {
			return empty, err
		}
		result.Error = "released before sandbox delivery"
		if err = operations.FailResult(ctx, source.OperationID, operation.Encode(result)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("release_before_commit"); err != nil {
		return empty, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("release_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
