package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
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
	Source          resourceStep   `json:"source"`
	CurrentInstance string         `json:"current_instance"`
}

type (
	discardState struct {
		Phase string `json:"phase"`
	}
	discardStep struct {
		OperationID string
		Hash        string
		Status      string
		Input       discardInput
		State       discardState
	}
)

func loadDiscard(ctx context.Context, query queryer, operation string) (discardStep, error) {
	step := discardStep{
		OperationID: operation,
	}
	var input, body []byte
	var action, candidate, expected string
	err := query.QueryRowContext(ctx, "SELECT action,input_hash,input,candidate_resource_id,expected_instance,status,body FROM operation_steps WHERE operation_id=? AND step_key='discard'", operation).Scan(&action, &step.Hash, &input, &candidate, &expected, &step.Status, &body)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
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

func (service *Service) discardSource(ctx context.Context, tx *sql.Tx, input discardInput) (vmRecord, error) {
	var vm vmRecord
	source, request := input.Source, input.Request
	if err := service.validateStep(source); err != nil {
		return vm, err
	}
	if (source.Action != "cold_start" && source.Action != "clone" && source.Action != "restore") || source.Status != "failed" || source.State.Witness != "" || (source.Input.Restore != nil && !source.State.RestoreBlocked) || source.OperationID != request.FailedOperationID || source.Input.SandboxID != request.SandboxID || source.Input.TenantID != request.TenantID || source.Input.Resource.Instance != request.ExpectedInstance || (input.CurrentInstance != "" && !expectedCurrentInstance(source, input.CurrentInstance)) {
		return vm, fmt.Errorf("discard requires frozen failed generation intent")
	}
	historical, err := loadStep(ctx, tx, source.OperationID)
	if err != nil {
		return vm, err
	}
	if !reflect.DeepEqual(historical, source) {
		return vm, fmt.Errorf("discard failed history changed")
	}
	if source.Input.FailureSlotDestination != nil {
		parent, err := service.resumeDestination(tx, source.Input.FailureSlotDestination)
		if err != nil {
			return vm, err
		}
		link, _, err := loadResourceLink(ctx, tx, parent.OperationID, "clone")
		if err != nil {
			return vm, err
		}
		if link.ChildID != source.OperationID || parent.Input.OwnerTenantID != source.Input.TenantID || parent.Input.Sandbox.ID != source.Input.SandboxID {
			return vm, fmt.Errorf("discard resume slot source changed")
		}
	}
	var status, kind, tenant, requestHash string
	var body, originalRequest []byte
	if err = tx.QueryRowContext(ctx, "SELECT status,scope_kind,scope_id,body,request_hash,request FROM operations WHERE id=?", source.OperationID).Scan(&status, &kind, &tenant, &body, &requestHash, &originalRequest); err != nil {
		return vm, err
	}
	var result operationResult
	if err = json.Unmarshal(body, &result); err != nil {
		return vm, err
	}
	if status != "failed" || kind != "tenant" || tenant != request.TenantID || result.VMID != source.Candidate || result.SandboxID != request.SandboxID || result.ResourceReady {
		return vm, fmt.Errorf("discard public failure identity changed")
	}
	if digest(originalRequest) != requestHash {
		return vm, fmt.Errorf("discard original request checksum changed")
	}
	switch source.Action {
	case "restore":
		var frozen struct {
			Action  string
			Request RestoreRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return vm, err
		}
		if source.Input.Restore == nil || frozen.Action != "restore" || frozen.Request.TenantID != tenant || frozen.Request.SandboxID != source.Input.SandboxID || frozen.Request.SnapshotID != source.Input.Restore.Snapshot.ID || frozen.Request.ExpectedInstance != source.ExpectedInstance {
			return vm, fmt.Errorf("discard original restore request changed")
		}
	case "clone":
		var frozen struct {
			Action  string
			Request CloneRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return vm, err
		}
		if source.Input.Restore == nil || frozen.Action != "clone" || frozen.Request.TenantID != tenant || frozen.Request.SnapshotID != source.Input.Restore.Snapshot.ID {
			return vm, fmt.Errorf("discard original clone request changed")
		}
	default:
		var frozen struct {
			Action  string
			Request ColdRequest
		}
		if err = json.Unmarshal(originalRequest, &frozen); err != nil {
			return vm, err
		}
		if frozen.Action != "claim" || frozen.Request.TenantID != tenant || frozen.Request.TemplateID != source.Input.TemplateID {
			return vm, fmt.Errorf("discard original cold request changed")
		}
	}
	var instanceBody, versionBody []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM instances WHERE id=? AND vm_id=? AND operation_id=?", request.ExpectedInstance, source.Candidate, source.OperationID).Scan(&instanceBody); err != nil {
		return vm, err
	}
	var instance vmRecord
	if err = json.Unmarshal(instanceBody, &instance); err != nil {
		return vm, err
	}
	if !reflect.DeepEqual(instance.Resource, source.Input.Resource) || instance.Verified || instance.AgentReady {
		return vm, fmt.Errorf("discard instance is not the original undelivered candidate")
	}
	if err = tx.QueryRowContext(ctx, "SELECT body FROM resource_versions WHERE id=? AND vm_id=? AND operation_id=? AND step_key=?", source.OperationID, source.Candidate, source.OperationID, source.Key).Scan(&versionBody); err != nil {
		return vm, err
	}
	var version resourceVersion
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return vm, err
	}
	if !reflect.DeepEqual(version, resourceVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return vm, fmt.Errorf("discard version differs from failed intent")
	}
	if source.Input.Restore != nil {
		if err = service.restoreRelations(ctx, tx, source); err != nil {
			return vm, err
		}
	}
	return vm, nil
}

func (service *Service) discardProof(ctx context.Context, tx *sql.Tx, step discardStep) (vmRecord, error) {
	vm, err := service.discardSource(ctx, tx, step.Input)
	if err != nil {
		return vm, err
	}
	input, source := step.Input, step.Input.Source
	var body []byte
	var instance, owner, state string
	if err = tx.QueryRowContext(ctx, "SELECT current_instance,operation_id,state,body FROM vms WHERE id=?", source.Candidate).Scan(&instance, &owner, &state, &body); err != nil {
		return vm, err
	}
	if err = json.Unmarshal(body, &vm); err != nil {
		return vm, err
	}
	if instance != input.CurrentInstance || owner != step.OperationID || !vm.Deleting || !reflect.DeepEqual(vm.Resource, source.Input.Resource) || vm.AgentReady || vm.Verified && state != "stopped" {
		return vm, fmt.Errorf("discard lost current VM ownership")
	}
	sandbox, err := loadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
	if err != nil {
		return vm, err
	}
	if sandbox.State != "terminated" || sandbox.OperationID != step.OperationID || sandbox.SourceOperation != source.OperationID || sandbox.VMID != source.Candidate || sandbox.Instance != "" {
		return vm, fmt.Errorf("discard sandbox destination changed")
	}
	var parentStatus, kind, tenant, hash string
	var request, parent []byte
	if err = tx.QueryRowContext(ctx, "SELECT status,scope_kind,scope_id,request_hash,request,body FROM operations WHERE id=?", step.OperationID).Scan(&parentStatus, &kind, &tenant, &hash, &request, &parent); err != nil {
		return vm, err
	}
	var frozen struct {
		Action  string
		Request DiscardRequest
	}
	var result operationResult
	if err = json.Unmarshal(request, &frozen); err != nil {
		return vm, err
	}
	if err = json.Unmarshal(parent, &result); err != nil {
		return vm, err
	}
	if terminal(parentStatus) || kind != "tenant" || tenant != input.Request.TenantID || digest(request) != hash || frozen.Action != "discard" || frozen.Request != input.Request || result.SandboxID != sandbox.ID || result.VMID != source.Candidate {
		return vm, fmt.Errorf("discard operation identity changed")
	}
	for _, cleanup := range []struct {
		id, operation, status string
		input                 any
	}{
		{"blocked:" + source.OperationID, source.OperationID, "transferred", source.Input},
		{"discard:" + step.OperationID, step.OperationID, "pending", input},
	} {
		if err = tx.QueryRowContext(ctx, "SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='vm' AND resource_id=? AND status=?", cleanup.id, cleanup.operation, source.Candidate, cleanup.status).Scan(&body); err != nil {
			return vm, err
		}
		if string(body) != string(encode(cleanup.input)) {
			return vm, fmt.Errorf("discard cleanup responsibility changed")
		}
	}
	shape := source.Input.Resource.Config.Shape
	vmBytes := source.Input.RuntimeBytes
	generations, err := service.cleanupGenerations(ctx, tx, source, "cleanup", step.OperationID+":discard")
	if err != nil {
		return vm, err
	}
	if err = checkGenerationCounts(ctx, tx, source, generations); err != nil {
		return vm, err
	}
	expectedBacking := 0
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM backing WHERE vm_id=?", []any{
			source.Candidate,
		}, expectedBacking},
		{"SELECT COUNT(*) FROM instances WHERE vm_id=?", []any{
			source.Candidate,
		}, len(generations)},
		{"SELECT COUNT(*) FROM resource_versions WHERE vm_id=?", []any{
			source.Candidate,
		}, len(generations)},
		{"SELECT COUNT(*) FROM warm WHERE vm_id=?", []any{
			source.Candidate,
		}, 0},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=?", []any{
			source.Candidate,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			source.Candidate,
			step.OperationID + ":discard",
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='cleanup' AND holder_id=?", []any{
			source.Input.Resource.Config.Image,
			step.OperationID + ":discard",
		}, 1},
		{"SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind='vm' AND resource_id=? AND status!='done'", []any{
			source.Candidate,
		}, 2},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=? AND cpus=?", []any{
			"vm:" + source.Candidate,
			vmBytes,
			shape.MemoryMiB + 128,
			shape.VCPUs,
		}, 1},
	}
	for _, quota := range []struct {
		kind, key, operation string
		slots, storage       int64
	}{
		{"pending_storage", source.OperationID + ":resource", source.OperationID, 0, shape.DiskMiB},
		{source.Input.SlotBudget.Kind, sandbox.ID, source.Input.SlotOperation, source.Input.SlotBudget.Units, 0},
	} {
		checks = append(checks, struct {
			q    string
			args []any
			want int
		}{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=? AND sandbox_slots=? AND storage_mib=? AND source_operation_id=?", []any{
			input.Request.TenantID,
			quota.kind,
			quota.key,
			quota.slots,
			quota.storage,
			quota.operation,
		}, 1})
		checks = append(checks, struct {
			q    string
			args []any
			want int
		}{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=?", []any{
			input.Request.TenantID,
			quota.kind,
			quota.key,
		}, 1})
		checks = append(checks, struct {
			q    string
			args []any
			want int
		}{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind='cleanup' AND holder_id=?", []any{
			input.Request.TenantID,
			quota.kind,
			quota.key,
			step.OperationID + ":discard",
		}, 1})
	}
	for _, check := range checks {
		var n int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&n); err != nil || n != check.want {
			return vm, fmt.Errorf("discard responsibility proof incomplete: %v", err)
		}
	}
	return vm, nil
}
