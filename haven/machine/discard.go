package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

type DiscardRequest struct {
	TenantID          string `json:"tenant_id"`
	Key               string `json:"key"`
	SandboxID         string `json:"sandbox_id"`
	FailedOperationID string `json:"failed_operation_id"`
	ExpectedInstance  string `json:"expected_instance"`
}

// discardInput freezes a failed intent, not a synthesized success witness.
// CurrentInstance can remain empty for a candidate that never reached launch.
type discardInput struct {
	Request         DiscardRequest `json:"request"`
	Source          ResourceStep   `json:"source"`
	CurrentInstance string         `json:"current_instance"`
}

type discardState struct {
	Phase string `json:"phase"`
}

type DiscardStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       discardInput
	State       discardState
}

func LoadDiscard(ctx context.Context, query store.DBTX, operationID string) (DiscardStep, error) {
	step := DiscardStep{
		OperationID: operationID,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: operationID,
		StepKey:     "discard",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("discard input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "discard" || candidate != step.Input.Source.Candidate || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("discard identity changed")
	}
	return step, nil
}

// AdmitDiscard is an explicit sandbox-scoped cleanup of one fixed failed intent.
// It never upgrades that failure to a successful generation or invents a witness.
func (d *VMDeletion) AdmitDiscard(ctx context.Context, request DiscardRequest) (operation.Record, error) {
	if d.
		ownership ==
		nil || d.
		handoff ==
		nil || d.usage == nil {
		return operation.Record{}, fmt.Errorf("%w: required VMDeletion capability port unavailable",
			errs.ErrManualRecovery)
	}

	return d.AdmitDiscardForParent(ctx, request, nil)
}

func (d *VMDeletion) AdmitDiscardForParent(ctx context.Context, request DiscardRequest, parent ResourceParent) (operation.Record, error) {
	if d.
		ownership ==
		nil || d.
		handoff ==
		nil || d.usage == nil {
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
	d.admission.Lock()
	defer d.admission.Unlock()
	closing := d.lifetime.Closing() || d.store.Reviewed
	if closing {
		return empty, fmt.Errorf("%w: Haven is closing", errs.ErrConflict)
	}
	tx, err := d.store.Database.BeginTx(ctx, nil)
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
			d.dispatch(cached.ID)
		}
		return cached, err
	}
	record, err := d.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
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
	if _, err = d.discardSource(ctx, tx, input); err != nil {
		return empty, err
	}
	if _, err = d.proofs.GenerationHistory(ctx, tx, source); err != nil {
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
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:       accepted,
		Kind:        "discard",
		Candidate:   source.Candidate,
		RequestHash: hash,
	}); err != nil {
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
	if err = d.handoff.TerminateSandbox(ctx, tx, SandboxTermination{
		Sandbox:     record,
		OperationID: accepted.ID,
		TenantID:    request.TenantID,
	}); err != nil {
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
	if _, err = d.discardProof(ctx, tx, step); err != nil {
		return empty, fmt.Errorf("discard handoff: %w", err)
	}
	if err = d.fault("discard_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = d.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = d.fault("discard_admitted"); err != nil {
		return accepted, err
	}
	d.dispatch(accepted.ID)
	return accepted, nil
}

func (d *VMDeletion) discardSource(ctx context.Context, tx *sql.Tx, input discardInput) (VMRecord, error) {
	var currentMachine VMRecord
	source, request := input.Source, input.Request
	if err := d.validateStep(source); err != nil {
		return currentMachine, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "failed" || source.State.Witness != "" || (source.Input.Restore != nil && !source.State.RestoreBlocked) || source.OperationID != request.FailedOperationID || source.Input.SandboxID != request.SandboxID || source.Input.TenantID != request.TenantID || source.Input.Resource.Instance != request.ExpectedInstance || (input.CurrentInstance != "" && !ExpectedCurrentInstance(source, input.CurrentInstance)) {
		return currentMachine, fmt.Errorf("discard requires frozen failed generation intent")
	}
	historical, err := LoadResourceStep(ctx, tx, source.OperationID)
	if err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(historical, source) {
		return currentMachine, fmt.Errorf("discard failed history changed")
	}
	if source.Input.FailureSlotDestination != nil {
		parent, err := d.ownership.ResumeDestination(d.ctx, tx, source.Input.FailureSlotDestination)
		if err != nil {
			return currentMachine, err
		}
		link, _, err := LoadResourceLink(ctx, tx, parent.OperationID, "clone")
		if err != nil {
			return currentMachine, err
		}
		if link.ChildID != source.OperationID || parent.TenantID != source.Input.TenantID || parent.SandboxID != source.Input.SandboxID {
			return currentMachine, fmt.Errorf("discard resume slot source changed")
		}
	}
	projection, err := operation.New(tx).Projection(ctx, source.OperationID)
	if err != nil {
		return currentMachine, err
	}
	status, kind, tenant, requestHash := projection.Status, projection.ScopeKind, projection.ScopeID, projection.RequestHash
	body, originalRequest := projection.Body, projection.Request
	var result operation.Result
	if err = json.Unmarshal(body, &result); err != nil {
		return currentMachine, err
	}
	if status != "failed" || kind != "tenant" || tenant != request.TenantID || result.VMID != source.Candidate || result.SandboxID != request.SandboxID || result.ResourceReady {
		return currentMachine, fmt.Errorf("discard public failure identity changed")
	}
	if operation.Digest(originalRequest) != requestHash {
		return currentMachine, fmt.Errorf("discard original request checksum changed")
	}
	switch source.Action {
	case "restore":
		var frozen struct {
			Action  string
			Request RestoreRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return currentMachine, err
		}
		if source.Input.Restore == nil || frozen.Action != "restore" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != source.Input.SandboxID || frozen.Request.SnapshotID != source.Input.Restore.Snapshot.ID || frozen.Request.ExpectedInstance != source.ExpectedInstance {
			return currentMachine, fmt.Errorf("discard original restore request changed")
		}
	case "clone":
		var frozen struct {
			Action  string
			Request CloneRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return currentMachine, err
		}
		if source.Input.Restore == nil || frozen.Action != "clone" || frozen.Request.TenantID != tenant || frozen.Request.SnapshotID != source.Input.Restore.Snapshot.ID {
			return currentMachine, fmt.Errorf("discard original clone request changed")
		}
	default:
		var frozen struct {
			Action  string
			Request FreshRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return currentMachine, err
		}
		if frozen.Action != "claim" || frozen.Request.TenantID != tenant || frozen.Request.TemplateID != source.Input.TemplateID {
			return currentMachine, fmt.Errorf("discard original cold request changed")
		}
	}
	var instanceBody, versionBody []byte
	if instanceBody, err = InstanceHistory(ctx, tx, GenerationIdentity{
		VMID:        source.Candidate,
		OperationID: source.OperationID,
	}, request.ExpectedInstance); err != nil {
		return currentMachine, err
	}
	var instance VMRecord
	if err = json.Unmarshal(instanceBody, &instance); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(instance.Resource, source.Input.Resource) || instance.Verified || instance.AgentReady {
		return currentMachine, fmt.Errorf("discard instance is not the original undelivered candidate")
	}
	if versionBody, err = VersionHistory(ctx, tx, GenerationIdentity{
		VMID:        source.Candidate,
		OperationID: source.OperationID,
	}, source.Key); err != nil {
		return currentMachine, err
	}
	var version RetainedVersion
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(version, RetainedVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return currentMachine, fmt.Errorf("discard version differs from failed intent")
	}
	if source.Input.Restore != nil {
		if err = d.proofs.Snapshots().RestoreRelations(ctx, tx, source); err != nil {
			return currentMachine, err
		}
	}
	return currentMachine, nil
}

func (d *VMDeletion) discardProof(ctx context.Context, tx *sql.Tx, step DiscardStep) (VMRecord, error) {
	currentMachine, err := d.discardSource(ctx, tx, step.Input)
	if err != nil {
		return currentMachine, err
	}
	input, source := step.Input, step.Input.Source
	current, err := LoadOwnership(ctx, tx, source.Candidate)
	if err != nil {
		return currentMachine, err
	}
	if err = json.Unmarshal(current.Body, &currentMachine); err != nil {
		return currentMachine, err
	}
	if !current.Owned || current.Instance != input.CurrentInstance || current.Owner != step.OperationID || !currentMachine.Deleting || !reflect.DeepEqual(currentMachine.Resource, source.Input.Resource) || currentMachine.AgentReady || currentMachine.Verified && current.State != "stopped" {
		return currentMachine, fmt.Errorf("discard lost current VM ownership")
	}
	record, err := d.ownership.ReadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
	if err != nil {
		return currentMachine, err
	}
	if record.State != "terminated" || record.OperationID != step.OperationID || record.SourceOperation != source.OperationID || record.VMID != source.Candidate || record.Instance != "" {
		return currentMachine, fmt.Errorf("discard sandbox destination changed")
	}
	operations := operation.New(tx)
	projection, err := operations.Projection(ctx, step.OperationID)
	if err != nil {
		return currentMachine, err
	}
	parentStatus, kind, tenant, hash := projection.Status, projection.ScopeKind, projection.ScopeID, projection.RequestHash
	request, parent := projection.Request, projection.Body
	var frozen struct {
		Action  string
		Request DiscardRequest
	}
	var result operation.Result
	if err = json.Unmarshal(request, &frozen); err != nil {
		return currentMachine, err
	}
	if err = json.Unmarshal(parent, &result); err != nil {
		return currentMachine, err
	}
	if operation.Terminal(parentStatus) || kind != "tenant" || tenant != input.Request.TenantID || operation.Digest(request) != hash || frozen.Action != "discard" || frozen.Request != input.Request || result.SandboxID != record.ID || result.VMID != source.Candidate {
		return currentMachine, fmt.Errorf("discard operation identity changed")
	}
	var body []byte
	for _, cleanup := range []struct {
		id        string
		operation string
		status    string
		input     any
	}{
		{"blocked:" + source.OperationID, source.OperationID, "transferred", source.Input},
		{"discard:" + step.OperationID, step.OperationID, "pending", input},
	} {
		if body, err = dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
			ID:           cleanup.id,
			OperationID:  cleanup.operation,
			ResourceKind: "vm",
			ResourceID:   source.Candidate,
			Status:       cleanup.status,
		}); err != nil {
			return currentMachine, err
		}
		if string(body) != string(operation.Encode(cleanup.input)) {
			return currentMachine, fmt.Errorf("discard cleanup responsibility changed")
		}
	}
	shape := source.Input.Resource.Config.Shape
	vmBytes := source.Input.RuntimeBytes
	generations, err := d.proofs.CleanupGenerations(ctx, tx, source, quota.Holder{
		Kind: "cleanup",
		ID:   step.OperationID + ":discard",
	})
	if err != nil {
		return currentMachine, err
	}
	if err = CheckGenerationCounts(ctx, tx, source, generations); err != nil {
		return currentMachine, err
	}
	expectedBacking := 0
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountMatchingReservation(ctx, quota.Reservation{
		Owner:     "vm:" + source.Candidate,
		Bytes:     vmBytes,
		MemoryMiB: shape.MemoryMiB + 128,
		CPUs:      int64(shape.VCPUs),
	}); err != nil || count != 1 {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	type countCheck struct {
		read func() (int64, error)
		want int64
	}
	checks := []countCheck{
		{func() (int64, error) {
			return responsibilities.CountResourceHolders(ctx, "vm", source.Candidate)
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         source.Candidate,
				HolderKind: "cleanup",
				HolderID:   step.OperationID + ":discard",
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "image",
				ID:         source.Input.Resource.Config.Image,
				HolderKind: "cleanup",
				HolderID:   step.OperationID + ":discard",
			})
		}, 1},
		{func() (int64, error) {
			return dbgen.New(tx).CountUnfinishedCleanupResource(ctx, dbgen.CountUnfinishedCleanupResourceParams{
				ResourceKind: "vm",
				ResourceID:   source.Candidate,
			})
		}, 2},
	}
	for _, cost := range []quota.Cost{
		{
			CostIdentity: quota.CostIdentity{
				TenantID:        input.Request.TenantID,
				Kind:            "pending_storage",
				Key:             source.OperationID + ":resource",
				SourceOperation: source.OperationID,
			},
			StorageMiB: shape.DiskMiB,
		},
		{
			CostIdentity: quota.CostIdentity{
				TenantID:        input.Request.TenantID,
				Kind:            source.Input.SlotBudget.Kind,
				Key:             record.ID,
				SourceOperation: source.Input.SlotOperation,
			},
			Slots: source.Input.SlotBudget.Units,
		},
	} {
		checks = append(checks,
			countCheck{func() (int64, error) {
				return responsibilities.CountMatchingCost(ctx, cost)
			}, 1},
			countCheck{func() (int64, error) {
				return responsibilities.CountTenantCostHolders(ctx, cost.TenantID, cost.Kind, cost.Key)
			}, 1},
			countCheck{func() (int64, error) {
				return responsibilities.CountCostReference(ctx, quota.CostReference{
					TenantID:   cost.TenantID,
					Kind:       cost.Kind,
					Key:        cost.Key,
					HolderKind: "cleanup",
					HolderID:   step.OperationID + ":discard",
				})
			}, 1},
		)
	}
	if count, err := dbgen.New(tx).CountVMBackings(ctx, source.Candidate); err != nil || count != int64(expectedBacking) {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	if count, err := RetainedInstanceCount(ctx, tx, source.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	if count, err := RetainedVersionCount(ctx, tx, source.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	for _, check := range checks {
		if count, err := check.read(); err != nil || count != check.want {
			return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
		}
	}
	warm, err := d.usage.VMUsers(ctx, tx, source.Candidate)
	if err != nil || warm != 0 {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	return currentMachine, nil
}
