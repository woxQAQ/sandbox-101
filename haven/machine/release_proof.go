package machine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func (service *VMDeletion) releaseProof(tx *sql.Tx, step ResourceStep) (VMRecord, error) {
	queries := dbgen.New(tx)
	source, err := LoadResourceStep(service.ctx, tx, step.Input.SourceOperation)
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
	if err = service.ownership.VerifyReleaseDestination(service.ctx, tx, step); err != nil {
		return VMRecord{}, err
	}
	if !reflect.DeepEqual(expected, step.Input) {
		return VMRecord{}, fmt.Errorf("release input differs from its frozen source")
	}
	currentMachine, instance, owner, err := LoadVM(service.ctx, tx, step)
	if err != nil {
		return currentMachine, err
	}
	if !currentMachine.Deleting || owner != step.OperationID || instance != step.ExpectedInstance {
		return currentMachine, fmt.Errorf("release lost current instance ownership")
	}
	record, err := service.ownership.ReadSandbox(service.ctx, tx, step.Input.SandboxID, step.Input.TenantID)
	if err != nil {
		return currentMachine, err
	}
	budget, budgetErr := service.ownership.CurrentSlotBudget(service.ctx, tx, record, source)
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
	cleanup, err := dbgen.New(tx).GetPendingCleanupBody(service.ctx, dbgen.GetPendingCleanupBodyParams{
		ID:          "cleanup:" + step.OperationID,
		OperationID: step.OperationID,
	})
	if err != nil {
		return currentMachine, err
	}
	if string(cleanup) != string(operation.Encode(step.Input)) {
		return currentMachine, fmt.Errorf("cleanup frozen input changed")
	}
	projection, err := operations.Projection(service.ctx, step.OperationID)
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
	if err = service.validateStep(source); err != nil {
		return currentMachine, err
	}
	retained, err := queries.GetRetainedVersion(service.ctx, dbgen.GetRetainedVersionParams{
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
	birth, err := queries.GetRetainedInstance(service.ctx, dbgen.GetRetainedInstanceParams{
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
	if err = CheckTransferredCaptures(service.ctx, tx, step); err != nil {
		return currentMachine, err
	}
	generations, err := service.proofs.CleanupGenerations(service.ctx, tx, source, quota.Holder{
		Kind: "cleanup",
		ID:   step.OperationID + ":resource",
	})
	if err != nil {
		return currentMachine, err
	}
	if err = CheckGenerationCounts(service.ctx, tx, source, generations); err != nil {
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
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountMatchingReservation(service.ctx, quota.Reservation{
		Owner:     "vm:" + step.Candidate,
		Bytes:     vmBytes,
		MemoryMiB: shape.MemoryMiB + 128,
		CPUs:      int64(shape.VCPUs),
	}); err != nil || count != 1 {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	checks := []func() (int64, error){
		func() (int64, error) {
			return dbgen.New(tx).CountUnfinishedCleanupResource(service.ctx, dbgen.CountUnfinishedCleanupResourceParams{
				ResourceKind: "vm",
				ResourceID:   step.Candidate,
			})
		},
		func() (int64, error) {
			return responsibilities.CountResourceHolders(service.ctx, "vm", step.Candidate)
		},
		func() (int64, error) {
			return responsibilities.CountResourceReference(service.ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         step.Candidate,
				HolderKind: "cleanup",
				HolderID:   step.OperationID + ":resource",
			})
		},
		func() (int64, error) {
			return responsibilities.CountResourceReference(service.ctx, quota.ResourceReference{
				Kind:       "image",
				ID:         step.Input.Resource.Config.Image,
				HolderKind: "vm",
				HolderID:   step.Candidate,
			})
		},
		func() (int64, error) {
			return responsibilities.CountMatchingCost(service.ctx, quota.Cost{
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
			return responsibilities.CountMatchingCost(service.ctx, quota.Cost{
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
				return responsibilities.CountTenantCostHolders(service.ctx, tenant, charge.kind, charge.key)
			},
			func() (int64, error) {
				return responsibilities.CountCostReference(service.ctx, quota.CostReference{
					TenantID:   tenant,
					Kind:       charge.kind,
					Key:        charge.key,
					HolderKind: "cleanup",
					HolderID:   step.OperationID + ":resource",
				})
			},
		)
	}
	if count, err := queries.CountInstances(service.ctx, step.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	if count, err := queries.CountVersions(service.ctx, step.Candidate); err != nil || count != int64(len(generations)) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	if count, err := dbgen.New(tx).CountVMBackings(service.ctx, step.Candidate); err != nil || count != int64(expectedBacking) {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	for _, check := range checks {
		if count, err := check(); err != nil || count != 1 {
			return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
		}
	}
	warm, err := service.usage.VMUsers(service.ctx, tx, step.Candidate)
	if err != nil || warm != 0 {
		return currentMachine, fmt.Errorf("release responsibility proof changed: %v", err)
	}
	return currentMachine, nil
}
