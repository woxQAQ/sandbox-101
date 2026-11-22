package sandbox

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

// WarmClaimRequest carries the prepared claim credentials and the concrete
// golden publication used to verify ready-pool selection and fallback custody.
type WarmClaimRequest struct {
	Principal authorization.Principal
	Key       string
	Admission ClaimAdmission
	Template  supply.TemplateRecord
	Restore   machine.SnapshotSource
}

// WarmClaimAdmission binds one resource admission to the frozen warm claim
// step that owns the pool custody and its release or fallback role.
type WarmClaimAdmission struct {
	ParentID string
	Role     string
	Input    WarmClaimInput
}

// Admit borrows the caller's tenant request guard and selects a ready warm
// allocation atomically. A false found result permits the original cold path.
func (wc *WarmClaims) Admit(ctx context.Context, request WarmClaimRequest) (operation.State, bool, error) {
	principal, key, admission := request.Principal, request.Key, request.Admission
	selectedTemplate, restore := request.Template, request.Restore
	var empty operation.State
	wc.storeWriteMu.Lock()
	defer wc.storeWriteMu.Unlock()
	if err := wc.gate.Check(); err != nil {
		return empty, false, err
	}
	tx, err := wc.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := supply.NewTemplateCatalog(tx).Owned(ctx, selectedTemplate.TenantID, selectedTemplate.ID)
	if err != nil {
		return empty, false, err
	}
	if !reflect.DeepEqual(current, selectedTemplate) {
		return empty, false, fmt.Errorf("%w: warm claim template version changed", errs.ErrConflict)
	}
	if err = supply.ValidateWarmCatalog(wc.proofs, ctx, tx, selectedTemplate); err != nil {
		return empty, false, err
	}
	poolID, selected, source, found, err := supply.ReadyWarm(wc.proofs, ctx, tx, selectedTemplate)
	if !found || err != nil {
		return empty, false, err
	}
	currentRestore, err := wc.proofs.Snapshots().LoadSnapshotSource(ctx, tx, selectedTemplate.TenantID, selectedTemplate.SnapshotID)
	if err != nil {
		return empty, false, err
	}
	if !reflect.DeepEqual(currentRestore, restore) {
		return empty, false, fmt.Errorf("%w: warm fallback source publication changed", errs.ErrConflict)
	}
	tenant, err := authorization.New(tx).Limits(ctx, selectedTemplate.TenantID)
	if err != nil {
		return empty, false, err
	}
	usage, err := quota.TenantUsage(ctx, tx, selectedTemplate.TenantID)
	if err != nil {
		return empty, false, err
	}
	if usage.Slots >= tenant.MaxSandboxes || usage.StorageMiB > tenant.MaxStorageMiB {
		return empty, false, fmt.Errorf("%w: tenant claim quota exhausted", errs.ErrExhausted)
	}
	destination, err := LoadSandbox(ctx, tx, selected.SandboxID, selectedTemplate.TenantID)
	if err != nil {
		return empty, false, err
	}
	previous := destination
	if destination.State != "provisioning" || destination.SourceOperation != source.OperationID || destination.Instance != selected.Instance || destination.SlotAssignment != nil {
		return empty, false, fmt.Errorf("%w: warm allocation current sandbox identity changed", errs.ErrManualRecovery)
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:     selected.SandboxID,
			VMID:          selected.VMID,
			SnapshotID:    selectedTemplate.SnapshotID,
			ResourceReady: true,
		},
	}
	settings := admission.Settings
	settings.Generation = accepted.ID
	input := WarmClaimInput{
		RequestTenantID: principal.TenantID,
		Command:         admission.Command,
		PoolID:          poolID,
		Pool:            selected,
		Source:          source,
		Settings:        settings,
		Restore:         restore,
	}
	payload := operation.Encode(input)
	hash := operation.Digest(payload)
	destination.OperationID = accepted.ID
	destination.SlotAssignment = &ownership.GenerationRef{
		OperationID: accepted.ID,
		InputHash:   hash,
	}
	destination.Identity = NewSandboxBirth(selectedTemplate, "warm")
	activation := ActivationInput{
		Attempt:           "warm",
		AllowFallback:     true,
		RequestTenantID:   principal.TenantID,
		TenantID:          selectedTemplate.TenantID,
		SandboxID:         selected.SandboxID,
		ResourceOperation: source.OperationID,
		ResourceHash:      source.Hash,
		ExpectedInstance:  selected.Instance,
		Settings:          settings,
		ResetIdentity:     true,
	}
	activationPayload := operation.Encode(activation)
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Intent
	}{"claim", admission.Command})
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   principal.TenantID,
		Key:       key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, false, err
	}
	if err = operations.InsertCredential(ctx, accepted.ID, admission.MAC); err != nil {
		return empty, false, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "warm_claim",
		Action:              "claim",
		InputHash:           hash,
		Input:               payload,
		CandidateResourceID: selected.SandboxID,
		ExpectedInstance:    selected.Instance,
		Status:              "pending",
		Body: operation.Encode(WarmClaimState{
			Phase: "selected",
		}),
	})); err != nil {
		return empty, false, err
	}

	if err = store.RequireOne(dbgen.New(tx).InsertSandboxSettings(ctx, dbgen.InsertSandboxSettingsParams{
		SandboxID: selected.SandboxID,
		Body:      operation.Encode(settings),
	})); err != nil {
		return empty, false, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertFence(ctx, dbgen.InsertFenceParams{
		SandboxID:   selected.SandboxID,
		OperationID: accepted.ID,
	})); err != nil {
		return empty, false, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "activation:warm",
		Action:              "activate",
		InputHash:           operation.Digest(activationPayload),
		Input:               activationPayload,
		CandidateResourceID: selected.VMID,
		ExpectedInstance:    selected.Instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return empty, false, err
	}
	if err = store.RequireOne(dbgen.New(tx).AssignWarmSandbox(ctx, dbgen.AssignWarmSandboxParams{
		ID:           destination.ID,
		Tenant:       selectedTemplate.TenantID,
		PreviousBody: operation.Encode(previous),
		NextBody:     operation.Encode(destination),
	})); err != nil {
		return empty, false, err
	}
	if err = quota.ChargeWarmSlot(ctx, tx, selectedTemplate.TenantID, selected.SandboxID, source.Input.SlotOperation); err != nil {
		return empty, false, err
	}
	if err = supply.SelectWarm(ctx, tx, poolID, selected); err != nil {
		return empty, false, err
	}
	if err = machine.PinSnapshotSource(ctx, tx, selectedTemplate.TenantID, machine.SourceLease{
		SnapshotID:  selectedTemplate.SnapshotID,
		OperationID: accepted.ID,
		StepKey:     "warm_claim",
	}); err != nil {
		return empty, false, err
	}
	if err = wc.fault("warm_claim_admission_before_commit"); err != nil {
		return empty, false, err
	}
	if err = tx.Commit(); err != nil {
		return empty, false, err
	}
	wc.registry.SetOperationSecrets(accepted.ID, admission.Secrets)
	if err = wc.store.Checkpoint(); err != nil {
		return accepted, true, err
	}
	if err = wc.fault("warm_claim_admitted"); err != nil {
		return accepted, true, err
	}
	wc.dispatch(accepted.ID)
	return accepted, true, nil
}

func (admission *WarmClaimAdmission) ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, slot ProvisionSlotClaim) (*machine.QuotaDestination, error) {
	record, source, holder := slot.Sandbox, slot.Source, slot.Holder
	claim, err := LoadWarmClaim(ctx, tx, admission.ParentID)
	if err != nil {
		return nil, err
	}
	if claim.State.Phase != "warm_retired" || operation.Terminal(claim.Status) || record.State != "terminated" || holder != "claim:"+admission.ParentID || record.ID != claim.Input.Pool.SandboxID || source.Snapshot.ID != claim.Input.Restore.Snapshot.ID {
		return nil, fmt.Errorf("%w: fallback target differs from retired warm responsibility", errs.ErrConflict)
	}
	released, status, err := machine.LoadResourceLink(ctx, tx, admission.ParentID, "warm_release")
	if err != nil {
		return nil, err
	}
	if status != "succeeded" {
		return nil, fmt.Errorf("%w: fallback warm cleanup incomplete", errs.ErrConflict)
	}
	receipt, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != claim.Input.Source.OperationID || receipt.Input.SlotDestination == nil || receipt.Input.SlotDestination.Kind != "claim" || receipt.Input.SlotDestination.OperationID != admission.ParentID {
		return nil, fmt.Errorf("%w: fallback original warm retirement proof changed", errs.ErrManualRecovery)
	}
	return nil, nil
}

func (admission *WarmClaimAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	kind, candidate := request.Kind, request.Candidate
	step, err := LoadWarmClaim(ctx, tx, admission.ParentID)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) || !reflect.DeepEqual(step.Input, admission.Input) {
		return fmt.Errorf("%w: warm claim no longer owns resource admission", errs.ErrConflict)
	}
	fence, err := LifecycleFence(ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", errs.ErrConflict)
	}
	switch admission.Role {
	case "warm_release":
		activation, err := LoadActivation(ctx, tx, admission.ParentID)
		if err != nil {
			return err
		}
		if kind != "release" || candidate != step.Input.Pool.VMID || step.State.Phase != "warm_failed" || activation.Status != "failed" || activation.Input.Attempt != "warm" || activation.Input.ResourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("%w: warm release lacks original failed attempt", errs.ErrConflict)
		}
	case "fallback":
		if kind != "clone" || step.State.Phase != "warm_retired" {
			return fmt.Errorf("%w: fallback has no completed warm retirement", errs.ErrConflict)
		}
	default:
		return fmt.Errorf("%w: unsupported warm claim resource role", errs.ErrInvalid)
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      admission.ParentID,
		Role:             admission.Role,
		ExpectedInstance: step.Input.Pool.Instance,
	}, request)
}
