package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func pausedReceipt(ctx context.Context, q store.DBTX, current ownership.Sandbox, source machine.ResourceStep) (string, error) {
	ids, err := dbgen.New(q).ListSuccessfulPauses(ctx, current.ID)
	if err != nil {
		return "", err
	}
	for _, id := range ids {
		parent, err := LoadLifecycle(ctx, q, id)
		if err != nil {
			return "", err
		}
		if parent.Input.Source.OperationID != source.OperationID {
			continue
		}
		if parent.Input.Source.InputHash != source.Hash || parent.Input.SourceInstance != source.State.Witness || parent.Input.Sandbox.ID != current.ID || parent.Input.OwnerTenantID != current.TenantID || parent.State.Phase != "complete" {
			return "", fmt.Errorf("%w: paused release history changed", errs.ErrManualRecovery)
		}
		link, status, err := machine.LoadResourceLink(ctx, q, id, "release")
		if err != nil {
			return "", err
		}
		if status != "succeeded" {
			return "", fmt.Errorf("%w: paused release link has no successful deletion", errs.ErrManualRecovery)
		}
		released, err := machine.LoadResourceStep(ctx, q, link.ChildID)
		if err != nil {
			return "", err
		}
		if released.Status != "succeeded" || released.State.Phase != "complete" || released.ExpectedInstance != source.State.Witness || released.Input.SourceOperation != source.OperationID || released.Input.SlotDestination == nil || released.Input.SlotDestination.OperationID != id || released.Input.SlotDestination.ID != current.ID {
			return "", fmt.Errorf("%w: paused release original deletion receipt changed", errs.ErrManualRecovery)
		}
		return id, nil
	}
	return "", fmt.Errorf("%w: paused sandbox lacks original successful VM deletion proof", errs.ErrManualRecovery)
}

func (service *Lifecycle) admitPausedRelease(ctx context.Context, admission *LifecycleAdmission) (operation.Record, error) {
	parent, input, mac, upper := admission.Parent, admission.Input, admission.MAC, admission.Upper
	var empty operation.Record
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

	current, err := LoadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
	if err != nil {
		return empty, err
	}
	settings, err := LoadSettings(ctx, tx, current.ID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(current, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) || current.State != "paused" || current.VMID != "" || current.Instance != "" {
		return empty, fmt.Errorf("%w: paused release source changed", errs.ErrConflict)
	}
	fence, fenceErr := LifecycleFence(ctx, tx, current.ID)
	if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
		return empty, fenceErr
	}
	if fence != input.PriorFence {
		return empty, fmt.Errorf("%w: paused release source fence changed", errs.ErrConflict)
	}
	source, err := machine.LoadResourceStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return empty, err
	}
	if source.Hash != input.Source.InputHash || source.State.Witness != input.SourceInstance || source.Status != "succeeded" {
		return empty, fmt.Errorf("%w: paused release original source changed", errs.ErrManualRecovery)
	}
	proof, err := pausedReceipt(ctx, tx, current, source)
	if err != nil {
		return empty, err
	}
	input.PauseOperation = proof
	snapshot, err := service.proofs.Snapshots().SnapshotPublication(ctx, tx, input.OwnerTenantID, current.SnapshotID)
	if err != nil {
		return empty, err
	}
	desired := settings
	desired.Generation = parent.ID
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	next := current
	next.State = "terminated"
	next.OperationID = ""
	parent.Result.SnapshotID = current.SnapshotID
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Command
	}{"release", input.Command})
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        parent.ID,
		ScopeKind: "tenant",
		ScopeID:   input.RequestTenantID,
		Key:       parent.Key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "succeeded",
		Body:      operation.Encode(parent.Result),
	}); err != nil {
		return empty, err
	}
	if err = operations.InsertCredential(ctx, parent.ID, mac); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent.ID,
		StepKey:             "lifecycle",
		Action:              "release",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: current.ID,
		ExpectedInstance:    "",
		Status:              "succeeded",
		Body: operation.Encode(LifecycleState{
			Phase:      "complete",
			SnapshotID: current.SnapshotID,
		}),
	})); err != nil {
		return empty, err
	}
	responsibilities := quota.New(tx)
	if err = responsibilities.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         current.SnapshotID,
		HolderKind: "sleep",
		HolderID:   current.ID,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   input.OwnerTenantID,
		Kind:       "snapshot",
		Key:        current.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + current.ID,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.RemoveCostReference(ctx, quota.CostReference{
		TenantID:   input.OwnerTenantID,
		Kind:       "sandbox_slot",
		Key:        current.ID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + current.ID,
	}); err != nil {
		return empty, err
	}
	if err = responsibilities.RetireMatchingCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        input.OwnerTenantID,
			Kind:            "sandbox_slot",
			Key:             current.ID,
			SourceOperation: source.Input.SlotOperation,
		},
		Slots: 1,
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TerminatePausedSandbox(ctx, dbgen.TerminatePausedSandboxParams{
		ID:           current.ID,
		Tenant:       input.OwnerTenantID,
		PreviousBody: operation.Encode(current),
		NextBody:     operation.Encode(next),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    current.ID,
		PreviousBody: operation.Encode(settings),
		NextBody:     operation.Encode(desired),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "sleep:" + parent.ID,
		OperationID:  parent.ID,
		ResourceKind: "snapshot",
		ResourceID:   current.SnapshotID,
		Body:         operation.Encode(snapshot),
	})); err != nil {
		return empty, err
	}
	if input.PriorFence != "" {
		if err = store.RequireOne(dbgen.New(tx).DeleteFence(ctx, dbgen.DeleteFenceParams{
			SandboxID:   current.ID,
			OperationID: input.PriorFence,
		})); err != nil {
			return empty, err
		}
	}
	if upper != nil {
		if err = upper.AdmitResource(ctx, tx, machine.ResourceAdmission{
			Child:       parent,
			Kind:        "lifecycle",
			Candidate:   input.Sandbox.ID,
			RequestHash: operation.Digest(frozen),
		}); err != nil {
			return empty, err
		}
	}
	if err = service.fault("paused_release_before_commit"); err != nil {
		return empty, err
	}
	if err = service.committer.Commit(tx); err != nil {
		return empty, err
	}
	service.access.DropSandboxSecrets(current.ID)
	parent.Status = "succeeded"
	return parent, service.store.Checkpoint()
}

func (service *Lifecycle) retireSupersededLifecycle(tx *sql.Tx, step LifecycleStep) error {
	id := step.Input.PriorFence
	if id == "" {
		return nil
	}
	prior, err := LoadLifecycle(service.ctx, tx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return service.warmClaims.RetireSupersededIn(tx, step)
	}
	if err != nil {
		return err
	}
	if prior.Input.Sandbox.ID != step.Input.Sandbox.ID || prior.Input.OwnerTenantID != step.Input.OwnerTenantID {
		return fmt.Errorf("release superseded lifecycle scope changed")
	}
	if prior.Status == "succeeded" {
		return fmt.Errorf("%w: release fence referred to completed lifecycle", errs.ErrManualRecovery)
	}
	sleepSnapshot := ""
	if prior.Input.Command.Action == "resume" {
		sleepSnapshot = prior.Input.Sandbox.SnapshotID
	} else if prior.Input.Command.Action == "pause" && prior.State.Phase == "captured" {
		sleepSnapshot = prior.State.SnapshotID
	}
	operations := operation.New(tx)
	if sleepSnapshot != "" {
		source, err := service.proofs.Snapshots().SnapshotPublication(service.ctx, tx, step.Input.OwnerTenantID, sleepSnapshot)
		if err != nil {
			return err
		}
		if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
			Kind:       "snapshot",
			ID:         sleepSnapshot,
			HolderKind: "sleep",
			HolderID:   step.Input.Sandbox.ID,
		}); err != nil {
			return err
		}
		if err = quota.New(tx).RemoveCostReference(service.ctx, quota.CostReference{
			TenantID:   step.Input.OwnerTenantID,
			Kind:       "snapshot",
			Key:        sleepSnapshot,
			HolderKind: "active_use",
			HolderID:   "sleep:" + step.Input.Sandbox.ID,
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(service.ctx, dbgen.InsertCleanupIntentParams{
			ID:           "sleep:" + step.OperationID,
			OperationID:  step.OperationID,
			ResourceKind: "snapshot",
			ResourceID:   sleepSnapshot,
			Body:         operation.Encode(source),
		})); err != nil {
			return err
		}
	}
	if prior.Input.Rollback != nil {
		if err = RetireRollbackLease(service.ctx, tx, prior); err != nil {
			return err
		}
	}
	if !operation.Terminal(prior.Status) {
		if err = store.RequireOne(dbgen.New(tx).FailLiveStep(service.ctx, dbgen.FailLiveStepParams{
			OperationID: id,
			StepKey:     "lifecycle",
			InputHash:   prior.Hash,
			Body: operation.Encode(LifecycleState{
				Phase: "superseded",
			}),
		})); err != nil {
			return err
		}
	}
	status, body, err := operations.ResultState(service.ctx, id)
	if err != nil {
		return err
	}
	if !operation.Terminal(status) {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		result.Error = "sandbox operation superseded by release"
		if err = operations.FailResult(service.ctx, id, operation.Encode(result)); err != nil {
			return err
		}
	}
	return nil
}

func (service *Lifecycle) runReleaseCaptures(step LifecycleStep) error {
	stopped, err := service.runResourceLink(step.OperationID, "stop")
	if err != nil {
		return err
	}
	for _, ref := range step.Input.CaptureFailures {
		role := "failed-capture:" + ref.OperationID
		if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
			link := &LifecycleAdmission{
				History: service.proofs.Snapshots(),
				Parent: operation.Record{
					ID: step.OperationID,
				},
				Input: step.Input,
				Role:  role,
			}
			discards := service.captureDiscard
			_, err = discards.AdmitForParent(service.ctx, machine.CaptureDiscardRequest{
				TenantID:          step.Input.OwnerTenantID,
				Key:               step.OperationID + ":" + role,
				SandboxID:         step.Input.Sandbox.ID,
				FailedOperationID: ref.OperationID,
				ExpectedInstance:  step.Input.SourceInstance,
				StopOperationID:   stopped.ChildID,
			}, link)
			if err != nil {
				return err
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err = service.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "release"); errors.Is(lookup, sql.ErrNoRows) {
		link := &LifecycleAdmission{
			History: service.proofs.Snapshots(),
			Parent: operation.Record{
				ID: step.OperationID,
			},
			Input: step.Input,
			Role:  "release",
		}
		if _, err = service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
			TenantID:  step.Input.OwnerTenantID,
			Key:       step.OperationID + ":release",
			SandboxID: step.Input.Sandbox.ID,
		}, link); err != nil {
			return err
		}
	} else if lookup != nil {
		return lookup
	}
	return nil
}

func (admission *WarmClaimAdmission) ReleaseDestination(_ context.Context, _ *sql.Tx, record ownership.Sandbox, _ machine.ResourceStep) (*machine.QuotaDestination, bool, error) {
	if admission == nil {
		return nil, false, nil
	}
	if admission.Role != "warm_release" {
		return nil, false, fmt.Errorf("%w: warm claim release role invalid", errs.ErrInvalid)
	}
	return &machine.QuotaDestination{
		OperationID: admission.ParentID,
		Kind:        "claim",
		ID:          record.ID,
		SnapshotID:  admission.Input.Pool.Template.SnapshotID,
	}, true, nil
}

func (admission *LifecycleAdmission) ReleaseDestination(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, source machine.ResourceStep) (*machine.QuotaDestination, bool, error) {
	if admission == nil {
		return nil, false, nil
	}
	if admission.Input.Command.Action == "pause" {
		parent, err := LoadLifecycle(ctx, tx, admission.Parent.ID)
		if err != nil {
			return nil, false, err
		}
		if parent.State.Phase != "captured" || parent.State.SnapshotID == "" {
			return nil, false, fmt.Errorf("%w: pause source release lacks published snapshot", errs.ErrConflict)
		}
		return &machine.QuotaDestination{
			OperationID: admission.Parent.ID,
			Kind:        "sleep",
			ID:          record.ID,
			SnapshotID:  parent.State.SnapshotID,
		}, true, nil
	}
	if admission.Input.Command.Action == "resume" && admission.Role == "compensate" {
		activation, err := LoadActivation(ctx, tx, admission.Parent.ID)
		if err != nil {
			return nil, false, err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != source.OperationID || activation.Input.ExpectedInstance != source.State.Witness {
			return nil, false, fmt.Errorf("%w: resume compensation lacks failed delivery proof", errs.ErrConflict)
		}
		return &machine.QuotaDestination{
			OperationID: admission.Parent.ID,
			Kind:        "sleep",
			ID:          record.ID,
			SnapshotID:  admission.Input.Sandbox.SnapshotID,
		}, true, nil
	}
	return nil, false, nil
}

func ValidateReleaseSlotDestination(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	destination := resource.Input.SlotDestination
	if destination == nil {
		return nil
	}
	if destination.Kind == "claim" {
		return validateWarmSlotDestination(ctx, tx, resource)
	}
	parent, err := LoadLifecycle(ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || destination.Kind != "sleep" || destination.ID != resource.Input.SandboxID || parent.Input.Sandbox.ID != destination.ID || parent.Input.OwnerTenantID != resource.Input.TenantID {
		return fmt.Errorf("release slot handoff owner changed")
	}
	role := "release"
	switch parent.Input.Command.Action {
	case "pause":
		if parent.State.Phase != "captured" || destination.SnapshotID != parent.State.SnapshotID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Sandbox.Instance != resource.ExpectedInstance {
			return fmt.Errorf("pause release handoff source changed")
		}
	case "resume":
		role = "compensate"
		if _, err = ResumeDestination(ctx, tx, destination); err != nil {
			return err
		}
		activation, err := LoadActivation(ctx, tx, parent.OperationID)
		if err != nil {
			return err
		}
		if activation.Status != "failed" || activation.Input.ResourceOperation != resource.Input.SourceOperation || activation.Input.ExpectedInstance != resource.ExpectedInstance {
			return fmt.Errorf("resume release handoff lacks failed activation proof")
		}
	default:
		return fmt.Errorf("unsupported release slot destination")
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, role)
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("release slot handoff child changed")
	}
	if count, err := quota.New(tx).CountResourceReference(ctx, quota.ResourceReference{
		Kind:       "snapshot",
		ID:         destination.SnapshotID,
		HolderKind: "sleep",
		HolderID:   destination.ID,
	}); err != nil || count != 1 {
		return fmt.Errorf("pause sleep holder missing: %v", err)
	}
	if count, err := quota.New(tx).CountCostReference(ctx, quota.CostReference{
		TenantID:   resource.Input.TenantID,
		Kind:       "snapshot",
		Key:        destination.SnapshotID,
		HolderKind: "active_use",
		HolderID:   "sleep:" + destination.ID,
	}); err != nil || count != 1 {
		return fmt.Errorf("pause sleep fee holder missing: %v", err)
	}
	return nil
}
