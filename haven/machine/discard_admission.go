package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AdmitDiscard is an explicit sandbox-scoped cleanup of one fixed failed intent.
// It never upgrades that failure to a successful generation or invents a witness.
func (service *VMDeletion) AdmitDiscard(ctx context.Context, request DiscardRequest) (operation.Record, error) {
	if service.
		ownership ==
		nil || service.
		handoff ==
		nil || service.usage == nil {
		return operation.Record{}, fmt.Errorf("%w: required VMDeletion capability port unavailable",
			errs.ErrManualRecovery)
	}

	return service.AdmitDiscardForParent(ctx, request, nil)
}

func (service *VMDeletion) AdmitDiscardForParent(ctx context.Context, request DiscardRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil || service.
		handoff ==
		nil || service.usage == nil {
		return operation.Record{}, fmt.Errorf("%w: required VMDeletion capability port unavailable",
			errs.ErrManualRecovery)
	}

	var empty operation.Record
	if request.TenantID == "" || request.SandboxID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.FailedOperationID, "op-") || !loom.ValidResourceID(request.ExpectedInstance, "run-") {
		return empty, fmt.Errorf("%w: discard requires sandbox, failed operation, candidate instance and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request DiscardRequest
	}{"discard", request})
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
	if cached, found, err := operations.Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
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
		return empty, fmt.Errorf("%w: reuse the admitted discard key", errs.ErrConflict)
	}
	if record.SourceOperation != request.FailedOperationID || record.OperationID != request.FailedOperationID || record.Instance != "" {
		return empty, fmt.Errorf("%w: discard source differs from original undelivered sandbox", errs.ErrConflict)
	}
	source, err := LoadResourceStep(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	currentMachine, current, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != source.OperationID || currentMachine.Deleting || currentMachine.Verified || currentMachine.AgentReady || source.Candidate != record.VMID || current != "" && !ExpectedCurrentInstance(source, current) {
		return empty, fmt.Errorf("%w: discard candidate no longer belongs to failed intent", errs.ErrManualRecovery)
	}
	input := discardInput{
		Request:         request,
		Source:          source,
		CurrentInstance: current,
	}
	if _, err = service.discardSource(ctx, tx, input); err != nil {
		return empty, err
	}
	if _, err = service.proofs.GenerationHistory(ctx, tx, source); err != nil {
		return empty, err
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: record.ID,
			VMID:      source.Candidate,
		},
	}
	if err = AdmitResourceParent(ctx, tx, parent, accepted, "discard", source.Candidate, hash); err != nil {
		return empty, err
	}
	payload := operation.Encode(input)
	currentMachine.Deleting = true
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
		StepKey:             "discard",
		Action:              "discard",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    request.ExpectedInstance,
		Status:              "pending",
		Body: operation.Encode(discardState{
			Phase: "admitted",
		}),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "discard:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "vm",
		ResourceID:   source.Candidate,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TransferBlockedCleanupResource(ctx, dbgen.TransferBlockedCleanupResourceParams{
		ID:           "blocked:" + source.OperationID,
		OperationID:  source.OperationID,
		ResourceKind: "vm",
		ResourceID:   source.Candidate,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TransferOwnership(ctx, dbgen.TransferOwnershipParams{
		ID:       source.Candidate,
		Instance: current,
		Body:     operation.Encode(currentMachine),
		PreviousOwner: sql.NullString{
			String: source.OperationID,
			Valid:  true,
		},
		NextOwner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	if err = service.handoff.TerminateSandbox(ctx, tx, record, accepted.ID, request.TenantID); err != nil {
		return empty, err
	}
	for _, reference := range []struct {
		kind string
		id   string
	}{
		{"vm", source.Candidate},
		{"image", source.Input.Resource.Config.Image},
	} {
		if err = quota.New(tx).TransferResourceReference(ctx, quota.ResourceReference{
			Kind:       reference.kind,
			ID:         reference.id,
			HolderKind: "step",
			HolderID:   source.OperationID + ":resource",
		}, "cleanup", accepted.ID+":discard"); err != nil {
			return empty, err
		}
	}
	for _, charge := range []struct {
		kind       string
		key        string
		holderKind string
		holder     string
	}{
		{"pending_storage", source.OperationID + ":resource", "engine_dependency", source.OperationID + ":resource"},
		{source.Input.SlotBudget.Kind, record.ID, "active_use", "sandbox:" + record.ID},
	} {
		if err = quota.New(tx).TransferCostReference(ctx, quota.CostReference{
			TenantID:   request.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: charge.holderKind,
			HolderID:   charge.holder,
		}, "cleanup", accepted.ID+":discard"); err != nil {
			return empty, err
		}
	}
	for _, reference := range source.Input.Ancestors {
		prior, err := LoadResourceStep(ctx, tx, reference.OperationID)
		if err != nil {
			return empty, err
		}
		if err = quota.New(tx).TransferCostReference(ctx, quota.CostReference{
			TenantID:   request.TenantID,
			Kind:       "vm_disk",
			Key:        prior.Input.DiskKey,
			HolderKind: "active_use",
			HolderID:   "sandbox:" + record.ID,
		}, "cleanup", accepted.ID+":discard"); err != nil {
			return empty, err
		}
	}
	step, err := LoadDiscard(ctx, tx, accepted.ID)
	if err != nil {
		return empty, err
	}
	if _, err = service.discardProof(ctx, tx, step); err != nil {
		return empty, fmt.Errorf("discard handoff: %w", err)
	}
	if err = service.fault("discard_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("discard_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
