package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// AdmitRelease revokes the logical sandbox and gives cleanup an independent
// intent. An immutable successful source witness is required; unresolved or
// failed generation attempts retain their responsibility for review.
func (d *Deletion) AdmitRelease(ctx context.Context, request ReleaseRequest) (operation.State, error) {
	if d.
		ownership ==
		nil || d.handoff ==
		nil ||
		d.
			usage == nil {
		return operation.State{}, fmt.Errorf("%w: required Deletion capability port unavailable",
			errs.ErrManualRecovery,
		)
	}

	return d.AdmitReleaseForParent(ctx, request, nil)
}

func (d *Deletion) AdmitReleaseForParent(ctx context.Context, request ReleaseRequest, parent ResourceParent) (operation.State, error) {
	if d.ownership == nil || d.handoff == nil || d.usage == nil {
		return operation.State{}, fmt.Errorf("%w: required Deletion capability port unavailable",
			errs.ErrManualRecovery,
		)
	}

	var empty operation.State
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.SandboxID == "" {
		return empty, fmt.Errorf("%w: tenant, sandbox and a key of 1..256 bytes are required", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request ReleaseRequest
	}{"release", request})
	hash := operation.Digest(frozen)
	d.storeWriteMu.Lock()
	defer d.storeWriteMu.Unlock()
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
		return empty, fmt.Errorf("%w: reuse the admitted release key", errs.ErrConflict)
	}
	source, err := LoadResourceStep(ctx, tx, record.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = d.validateStep(source); err != nil {
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
	holders, err := quota.CountResourceHolders(ctx, tx, "vm", record.VMID)
	if err != nil {
		return empty, err
	}
	warm, err := d.usage.VMUsers(ctx, tx, record.VMID)
	if err != nil {
		return empty, err
	}
	captures, err := ReleaseCaptures(ctx, tx, record.VMID, source.State.Witness)
	if err != nil {
		return empty, err
	}
	generations, err := d.proofs.CleanupGenerations(ctx, tx, source, quota.Holder{
		Kind: "active_use",
		ID:   "sandbox:" + record.ID,
	})
	if err != nil {
		return empty, err
	}
	if err = d.proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if err = CheckGenerationCounts(ctx, tx, source, generations); err != nil {
		return empty, err
	}
	if instances != int64(len(generations)) || versions != int64(len(generations)) || warm != 0 || holders != 1+int64(len(captures)) {
		return empty, fmt.Errorf("%w: cold release has additional or missing resource responsibilities", errs.ErrManualRecovery)
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: record.ID,
			VMID:      record.VMID,
		},
	}
	budget, err := d.ownership.CurrentSlotBudget(ctx, tx, record, source)
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
	if err = d.handoff.TerminateSandbox(ctx, tx, SandboxTermination{
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
		if err = quota.TransferCostReference(ctx, tx, quota.CostReference{
			TenantID:   record.TenantID,
			Kind:       charge.kind,
			Key:        charge.key,
			HolderKind: "active_use",
			HolderID:   "sandbox:" + record.ID,
		}, "cleanup", accepted.ID+":resource"); err != nil {
			return empty, err
		}
	}
	if err = quota.TransferResourceReference(ctx, tx, quota.ResourceReference{
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
		if err = quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
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
	var completed []operation.Completion
	if !operation.Terminal(parentStatus) {
		var result operation.Result
		if err = json.Unmarshal(parentBody, &result); err != nil {
			return empty, err
		}
		result.Error = "released before sandbox delivery"
		completed = append(completed, operation.Completion{
			ID:     source.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if err = d.fault("release_before_commit"); err != nil {
		return empty, err
	}
	if err = d.committer.Complete(tx, completed...); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = d.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = d.fault("release_admitted"); err != nil {
		return accepted, err
	}
	d.dispatch(accepted.ID)
	return accepted, nil
}

func (d *Deletion) releaseProof(tx *sql.Tx, step ResourceStep) (VMRecord, error) {
	queries := dbgen.New(tx)
	source, err := LoadResourceStep(d.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return VMRecord{}, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "succeeded" || source.State.Phase != "complete" || source.State.Observation == nil || source.State.DiskIdentity == nil || source.State.Witness == "" || source.State.Witness != step.ExpectedInstance || source.State.Witness != step.Input.Resource.Instance || source.Input.SourceOperation != "" || source.Input.TenantID != step.Input.TenantID || source.Input.SandboxID != step.Input.SandboxID || source.Candidate != step.Candidate {
		return VMRecord{}, fmt.Errorf("release lost its immutable source witness")
	}
	expected := source.Input
	expected.Captures = step.Input.Captures
	expected.SourceOperation = source.OperationID
	expected.DiskIdentity = source.State.DiskIdentity
	expected.RestoreMaterials = CopyRestoreMaterials(source.State.Materials)
	expected.SlotDestination = step.Input.SlotDestination
	expected.SlotBudget = step.Input.SlotBudget
	expected.SlotAssignment = step.Input.SlotAssignment
	if err = d.ownership.VerifyReleaseDestination(d.ctx, tx, step); err != nil {
		return VMRecord{}, err
	}
	if !reflect.DeepEqual(expected, step.Input) {
		return VMRecord{}, fmt.Errorf("release input differs from its frozen source")
	}
	currentMachine, instance, owner, err := LoadVM(d.ctx, tx, step)
	if err != nil {
		return currentMachine, err
	}
	if !currentMachine.Deleting || owner != step.OperationID || instance != step.ExpectedInstance {
		return currentMachine, fmt.Errorf("release lost current instance ownership")
	}
	record, err := d.ownership.ReadSandbox(d.ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return currentMachine, err
	}
	budget, budgetErr := d.ownership.CurrentSlotBudget(d.ctx, tx, record, source)
	if budgetErr != nil {
		return VMRecord{}, budgetErr
	}
	if budget != step.Input.SlotBudget || !reflect.DeepEqual(record.SlotAssignment, step.Input.SlotAssignment) {
		return VMRecord{}, fmt.Errorf("release slot assignment differs from current owner")
	}
	if record.OperationID != step.OperationID || record.SourceOperation != step.Input.SourceOperation || record.State != "terminated" || record.Instance != step.ExpectedInstance || record.VMID != step.Candidate {
		return currentMachine, fmt.Errorf("release cleanup destination changed")
	}
	operations := operation.New(tx)
	cleanup, err := dbgen.New(tx).GetPendingCleanupBody(d.ctx, dbgen.GetPendingCleanupBodyParams{
		ID:          "cleanup:" + step.OperationID,
		OperationID: step.OperationID,
	})
	if err != nil {
		return currentMachine, err
	}
	if string(cleanup) != string(operation.Encode(step.Input)) {
		return currentMachine, fmt.Errorf("cleanup frozen input changed")
	}
	projection, err := operations.Projection(d.ctx, step.OperationID)
	if err != nil {
		return currentMachine, err
	}
	request, parentBody := projection.Request, projection.Body
	requestHash, kind, tenant, status := projection.RequestHash, projection.ScopeKind, projection.ScopeID, projection.Status
	var frozen struct {
		Action  string
		Request ReleaseRequest
	}
	if err = json.Unmarshal(request, &frozen); err != nil {
		return currentMachine, err
	}
	var result operation.Result
	if err = json.Unmarshal(parentBody, &result); err != nil {
		return currentMachine, err
	}
	if operation.Digest(request) != requestHash || kind != "tenant" || tenant != step.Input.TenantID || operation.Terminal(status) || frozen.Action != "release" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != record.ID || result.SandboxID != record.ID || result.VMID != step.Candidate {
		return currentMachine, fmt.Errorf("release operation identity changed")
	}
	if err = d.validateStep(source); err != nil {
		return currentMachine, err
	}
	retained, err := queries.GetRetainedVersion(d.ctx, dbgen.GetRetainedVersionParams{
		VmID: step.Candidate,
		ID:   source.OperationID,
	})
	if err != nil {
		return currentMachine, err
	}
	var version RetainedVersion
	if err = json.Unmarshal(retained.Body, &version); err != nil {
		return currentMachine, err
	}
	if retained.ID != source.OperationID || retained.OperationID != source.OperationID || retained.StepKey != source.Key || !reflect.DeepEqual(version, RetainedVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return currentMachine, fmt.Errorf("release resource version birth proof changed")
	}
	birth, err := queries.GetRetainedInstance(d.ctx, dbgen.GetRetainedInstanceParams{
		ID:   step.ExpectedInstance,
		VmID: step.Candidate,
	})
	if err != nil {
		return currentMachine, err
	}
	var historical VMRecord
	if err = json.Unmarshal(birth.Body, &historical); err != nil {
		return currentMachine, err
	}
	if birth.OperationID != source.OperationID || !historical.Verified || !historical.AgentReady || historical.Deleting || !reflect.DeepEqual(historical.Resource, source.Input.Resource) || !reflect.DeepEqual(historical.Observation, source.State.Observation) {
		return currentMachine, fmt.Errorf("release instance birth proof changed")
	}
	if err = CheckTransferredCaptures(d.ctx, tx, step); err != nil {
		return currentMachine, err
	}
	generations, err := d.proofs.CleanupGenerations(d.ctx, tx, source, quota.Holder{
		Kind: "cleanup",
		ID:   step.OperationID + ":resource",
	})
	if err != nil {
		return currentMachine, err
	}
	if err = CheckGenerationCounts(d.ctx, tx, source, generations); err != nil {
		return currentMachine, err
	}
	shape := step.Input.Resource.Config.Shape
	vmBytes := step.Input.RuntimeBytes
	expectedBacking := 0
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}
	if count, err := quota.CountMatchingReservation(d.ctx, tx, quota.Reservation{
		Owner:     "vm:" + step.Candidate,
		Bytes:     vmBytes,
		MemoryMiB: shape.MemoryMiB + 128,
		CPUs:      int64(shape.VCPUs),
	}); err != nil || count != 1 {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	checks := []func() (int64, error){
		func() (int64, error) {
			return dbgen.New(tx).CountUnfinishedCleanupResource(d.ctx, dbgen.CountUnfinishedCleanupResourceParams{
				ResourceKind: "vm",
				ResourceID:   step.Candidate,
			})
		},
		func() (int64, error) {
			return quota.CountResourceHolders(d.ctx, tx, "vm", step.Candidate)
		},
		func() (int64, error) {
			return quota.CountResourceReference(d.ctx, tx, quota.ResourceReference{
				Kind:       "vm",
				ID:         step.Candidate,
				HolderKind: "cleanup",
				HolderID:   step.OperationID + ":resource",
			})
		},
		func() (int64, error) {
			return quota.CountResourceReference(d.ctx, tx, quota.ResourceReference{
				Kind:       "image",
				ID:         step.Input.Resource.Config.Image,
				HolderKind: "vm",
				HolderID:   step.Candidate,
			})
		},
		func() (int64, error) {
			return quota.CountMatchingCost(d.ctx, tx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        tenant,
					Kind:            "vm_disk",
					Key:             step.Input.DiskKey,
					SourceOperation: step.Input.SourceOperation,
				},
				StorageMiB: shape.DiskMiB,
			})
		},
		func() (int64, error) {
			return quota.CountMatchingCost(d.ctx, tx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        tenant,
					Kind:            step.Input.SlotBudget.Kind,
					Key:             record.ID,
					SourceOperation: step.Input.SlotOperation,
				},
				Slots: step.Input.SlotBudget.Units,
			})
		},
	}
	for _, charge := range []struct {
		kind string
		key  string
	}{
		{"vm_disk", step.Input.DiskKey},
		{step.Input.SlotBudget.Kind, record.ID},
	} {
		checks = append(checks,
			func() (int64, error) {
				return quota.CountTenantCostHolders(d.ctx, tx, tenant, charge.kind, charge.key)
			},
			func() (int64, error) {
				return quota.CountCostReference(d.ctx, tx, quota.CostReference{
					TenantID:   tenant,
					Kind:       charge.kind,
					Key:        charge.key,
					HolderKind: "cleanup",
					HolderID:   step.OperationID + ":resource",
				})
			},
		)
	}
	if count, err := queries.CountInstances(d.ctx, step.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	if count, err := queries.CountVersions(d.ctx, step.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	if count, err := dbgen.New(tx).CountVMBackings(d.ctx, step.Candidate); err != nil || count != int64(expectedBacking) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	for _, check := range checks {
		if count, err := check(); err != nil || count != 1 {
			return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
		}
	}
	warm, err := d.usage.VMUsers(d.ctx, tx, step.Candidate)
	if err != nil || warm != 0 {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	return currentMachine, nil
}
