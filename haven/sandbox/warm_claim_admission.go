package sandbox

import (
	"context"
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

// Admit borrows the caller's tenant request guard and selects a ready warm
// allocation atomically. A false found result permits the original cold path.
func (service *WarmClaims) Admit(ctx context.Context, principal authorization.Principal, key string, admission ClaimAdmission, selectedTemplate supply.TemplateRecord, restore machine.Source) (operation.Record, bool, error) {
	var empty operation.Record
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return empty, false, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
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
	if err = supply.ValidateWarmCatalog(service.proofs, ctx, tx, selectedTemplate); err != nil {
		return empty, false, err
	}
	poolID, selected, source, found, err := supply.ReadyWarm(service.proofs, ctx, tx, selectedTemplate)
	if !found || err != nil {
		return empty, false, err
	}
	currentRestore, err := service.proofs.Snapshots().LoadRestoreSource(ctx, tx, selectedTemplate.TenantID, selectedTemplate.SnapshotID)
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
	usage, err := quota.New(tx).Usage(ctx, selectedTemplate.TenantID)
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
	accepted := operation.Record{
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
	destination.Identity = NewSandboxIdentity(selectedTemplate, "warm")
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
		Request operation.Command
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
	if err = store.RequireOne(dbgen.New(tx).InsertConfigurationFence(ctx, dbgen.InsertConfigurationFenceParams{
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
	if err = quota.New(tx).ChargeWarmSlot(ctx, selectedTemplate.TenantID, selected.SandboxID, source.Input.SlotOperation); err != nil {
		return empty, false, err
	}
	if err = supply.SelectWarm(ctx, tx, poolID, selected); err != nil {
		return empty, false, err
	}
	if err = machine.PinSnapshotSource(ctx, tx, selectedTemplate.TenantID, machine.Lease{
		SnapshotID:  selectedTemplate.SnapshotID,
		OperationID: accepted.ID,
		StepKey:     "warm_claim",
	}); err != nil {
		return empty, false, err
	}
	if err = service.fault("warm_claim_admission_before_commit"); err != nil {
		return empty, false, err
	}
	if err = tx.Commit(); err != nil {
		return empty, false, err
	}
	service.access.SetOperationSecrets(accepted.ID, admission.Secrets)
	if err = service.store.Checkpoint(); err != nil {
		return accepted, true, err
	}
	if err = service.fault("warm_claim_admitted"); err != nil {
		return accepted, true, err
	}
	service.dispatch(accepted.ID)
	return accepted, true, nil
}
