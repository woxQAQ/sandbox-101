package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func (service *VMDeletion) discardSource(ctx context.Context, tx *sql.Tx, input discardInput) (VMRecord, error) {
	var currentMachine VMRecord
	source, request := input.Source, input.Request
	if err := service.validateStep(source); err != nil {
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
		parent, err := service.ownership.ResumeDestination(service.ctx, tx, source.Input.FailureSlotDestination)
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
			Request ColdRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return currentMachine, err
		}
		if frozen.Action != "claim" || frozen.Request.TenantID != tenant || frozen.Request.TemplateID != source.Input.TemplateID {
			return currentMachine, fmt.Errorf("discard original cold request changed")
		}
	}
	var instanceBody, versionBody []byte
	if instanceBody, err = InstanceHistory(ctx, tx, request.ExpectedInstance, source.Candidate, source.OperationID); err != nil {
		return currentMachine, err
	}
	var instance VMRecord
	if err = json.Unmarshal(instanceBody, &instance); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(instance.Resource, source.Input.Resource) || instance.Verified || instance.AgentReady {
		return currentMachine, fmt.Errorf("discard instance is not the original undelivered candidate")
	}
	if versionBody, err = VersionHistory(ctx, tx, source.OperationID, source.Candidate, source.Key); err != nil {
		return currentMachine, err
	}
	var version Version
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(version, Version{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return currentMachine, fmt.Errorf("discard version differs from failed intent")
	}
	if source.Input.Restore != nil {
		if err = service.proofs.Snapshots().RestoreRelations(ctx, tx, source); err != nil {
			return currentMachine, err
		}
	}
	return currentMachine, nil
}

func (service *VMDeletion) discardProof(ctx context.Context, tx *sql.Tx, step DiscardStep) (VMRecord, error) {
	currentMachine, err := service.discardSource(ctx, tx, step.Input)
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
	record, err := service.ownership.ReadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
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
	generations, err := service.proofs.CleanupGenerations(ctx, tx, source, "cleanup", step.OperationID+":discard")
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
	warm, err := service.usage.VMUsers(ctx, tx, source.Candidate)
	if err != nil || warm != 0 {
		return currentMachine, fmt.Errorf("discard responsibility proof incomplete: %v", err)
	}
	return currentMachine, nil
}
