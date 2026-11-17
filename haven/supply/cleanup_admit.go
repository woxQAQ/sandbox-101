package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *SupplyCleanup) AdmitSupplyCleanup(ctx context.Context, principal authorization.Principal, key string, request SupplyCleanupRequest) (operation.Record, error) {
	var empty operation.Record
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || !loom.ValidResourceID(request.OperationID, "op-") {
		return empty, fmt.Errorf("%w: supply cleanup requires management authority, original operation and key", errs.ErrInvalid)
	}
	requestGuard := service.guards.NodeRequest(service.nodeID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	frozen := operation.Encode(struct {
		Action  string
		Request SupplyCleanupRequest
	}{"supply_cleanup", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(service.store.Database).Cached(ctx, "node", service.nodeID, key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	original, err := LoadSupply(ctx, service.store.Database, request.OperationID)
	if err != nil {
		return empty, err
	}
	guard := service.guards.TemplateWork(original.Input.Template.ID)
	if !guard.TryLock() {
		return empty, fmt.Errorf("%w: supply workflow is active", errs.ErrConflict)
	}
	defer guard.Unlock()
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	original, err = LoadSupply(ctx, tx, request.OperationID)
	if err != nil {
		return empty, err
	}
	if original.Status != "failed" && (original.Status != "succeeded" || original.Input.Command.Action != "refill") {
		return empty, fmt.Errorf("%w: supply cleanup requires fixed failed intent or unclaimed warm publication", errs.ErrConflict)
	}
	role := "clone"
	if original.Input.Command.Action == "template_build" {
		role = "cold"
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, original.OperationID, role)
	if err != nil {
		return empty, err
	}
	source, err := machine.LoadResourceStep(ctx, tx, link.ChildID)
	if err != nil {
		return empty, err
	}
	if !operation.Terminal(source.Status) {
		return empty, fmt.Errorf("%w: supply resource outcome remains unresolved", errs.ErrConflict)
	}
	if err = machine.AllocationCleanupFence(ctx, tx, source.Candidate, nil); err != nil {
		return empty, err
	}
	if source.Input.SandboxID != original.Input.SandboxID || source.Input.TenantID != original.Input.Template.TenantID {
		return empty, fmt.Errorf("%w: supply source ownership changed", errs.ErrManualRecovery)
	}
	record, err := service.ownership.ReadSandbox(ctx, tx, source.Input.SandboxID, source.Input.TenantID)
	if err != nil {
		return empty, err
	}
	settings, err := service.ownership.SettingsCount(ctx, tx, record.ID)
	if err != nil {
		return empty, err
	}
	if settings != 0 || record.SlotAssignment != nil || record.SourceOperation != source.OperationID || record.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw cleanup cannot bypass sandbox ownership", errs.ErrConflict)
	}
	input := SupplyCleanupInput{
		RequestTenantID: principal.TenantID,
		Request:         request,
		Original:        original,
		Source:          source,
	}
	if released, status, lookup := machine.LoadResourceLink(ctx, tx, original.OperationID, "release"); lookup == nil && status == "succeeded" {
		receipt, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
		if err != nil {
			return empty, err
		}
		if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != source.OperationID || receipt.Candidate != source.Candidate || record.State != "terminated" {
			return empty, fmt.Errorf("%w: supply original retirement receipt changed", errs.ErrManualRecovery)
		}
		input.Retired = &ownership.GenerationRef{
			OperationID: receipt.OperationID,
			InputHash:   receipt.Hash,
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return empty, lookup
	}
	if input.Retired == nil {
		if record.State != "provisioning" {
			return empty, fmt.Errorf("%w: supply current allocation changed", errs.ErrConflict)
		}
		currentMachine, instance, owner, err := machine.LoadVM(ctx, tx, source)
		if err != nil {
			return empty, err
		}
		if currentMachine.Deleting || (source.Status == "succeeded" && (owner != "" || instance != source.State.Witness || !currentMachine.Verified || !currentMachine.AgentReady)) || (source.Status == "failed" && owner != source.OperationID) {
			return empty, fmt.Errorf("%w: supply current resource ownership changed", errs.ErrConflict)
		}
		if source.Status == "succeeded" {
			refs, err := machine.FailedCaptureReferences(ctx, tx, source)
			if err != nil {
				return empty, err
			}
			input.CaptureFailures = refs
		}
	}
	if original.Input.Command.Action == "refill" {
		reservation, status, err := LoadPoolReservation(ctx, tx, original.OperationID)
		if err != nil {
			return empty, err
		}
		if reservation.Source.OperationID != source.OperationID || reservation.Source.InputHash != source.Hash || (status != "ready" && status != "cleanup") {
			return empty, fmt.Errorf("%w: supply pool responsibility was selected or changed", errs.ErrConflict)
		}
		if status == "ready" {
			if _, _, err = VerifyReadyWarm(service.proofs, ctx, tx, original.OperationID); err != nil {
				return empty, err
			}
			if err = store.RequireOne(dbgen.New(tx).RemoveWarm(ctx, dbgen.RemoveWarmParams{
				VmID:       reservation.VMID,
				TemplateID: reservation.Template.ID,
				SnapshotID: reservation.Template.SnapshotID,
			})); err != nil {
				return empty, err
			}
			if err = quota.New(tx).RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         reservation.VMID,
				HolderKind: "warm",
				HolderID:   original.OperationID,
			}); err != nil {
				return empty, err
			}
			if err = store.RequireOne(dbgen.New(tx).MarkReadyCleanup(ctx, dbgen.MarkReadyCleanupParams{
				OperationID: original.OperationID,
				Body:        operation.Encode(reservation),
			})); err != nil {
				return empty, err
			}
		}
		input.Pool = &reservation
	}
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: record.ID,
			VMID:      source.Candidate,
		},
	}
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "node",
		ScopeID:   service.nodeID,
		Key:       key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "supply_cleanup",
		Action:              "supply_cleanup",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.Input.Resource.Instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "supply_cleanup:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "supply_cleanup",
		ResourceID:   source.Candidate,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "step",
		HolderID:   accepted.ID + ":supply_cleanup",
	}); err != nil {
		return empty, err
	}
	if err = service.fault("supply_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("supply_cleanup_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}
