package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
)

type CaptureDiscardRequest struct {
	TenantID          string `json:"tenant_id"`
	Key               string `json:"key"`
	SandboxID         string `json:"sandbox_id"`
	FailedOperationID string `json:"failed_operation_id"`
	ExpectedInstance  string `json:"expected_instance"`
	StopOperationID   string `json:"stop_operation_id"`
}

type (
	captureDiscardInput struct {
		Lifecycle string `json:"lifecycle,omitempty"`
		Request   CaptureDiscardRequest
		Source    captureStep
		StopHash  string
	}
	captureDiscardStep struct {
		OperationID, Hash, Status, Phase string
		Input                            captureDiscardInput
	}
)

func loadCaptureDiscard(ctx context.Context, q queryer, id string) (captureDiscardStep, error) {
	step := captureDiscardStep{
		OperationID: id,
	}
	var input, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT action,input_hash,input,candidate_resource_id,expected_instance,status,body FROM operation_steps WHERE operation_id=? AND step_key='capture_discard'", id).Scan(&action, &step.Hash, &input, &candidate, &expected, &step.Status, &body)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("capture discard input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "capture_discard" || candidate != step.Input.Source.Input.Snapshot.ID || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("capture discard frozen identity changed")
	}
	return step, nil
}

func (service *Service) captureDiscardProof(ctx context.Context, tx *sql.Tx, step captureDiscardStep, admitting bool) error {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	historical, err := loadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(historical, source) || source.OperationID != request.FailedOperationID || source.Status != "failed" || source.State.ResumeRequired || source.State.Error == "" || source.Input.Request.TenantID != request.TenantID || source.Input.Request.SandboxID != request.SandboxID || source.Input.Request.ExpectedInstance != request.ExpectedInstance || resource.State.Witness != request.ExpectedInstance {
		return fmt.Errorf("capture discard requires immutable resumed failure")
	}
	if err = service.verifyGeneration(ctx, tx, resource); err != nil {
		return err
	}
	generations, err := service.generationHistory(ctx, tx, resource)
	if err != nil {
		return err
	}
	if err = checkGenerationCounts(ctx, tx, resource, append(generations, resource)); err != nil {
		return err
	}
	stopped, err := loadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return err
	}
	if stopped.Hash != input.StopHash || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != resource.OperationID || stopped.Input.SourceHash != resource.Hash || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != request.SandboxID || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance {
		return fmt.Errorf("capture discard stop proof changed")
	}
	sandbox, err := loadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return err
	}
	allowedState := sandbox.State == "provisioning"
	if input.Lifecycle != "" {
		parent, err := loadLifecycle(ctx, tx, input.Lifecycle)
		if err != nil {
			return err
		}
		var fence string
		if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&fence); err != nil {
			return err
		}
		found := false
		for _, ref := range parent.Input.CaptureFailures {
			if ref.OperationID == source.OperationID && ref.InputHash == source.Hash {
				found = true
			}
		}
		stopLink, _, err := loadResourceLink(ctx, tx, parent.OperationID, "stop")
		if err != nil {
			return err
		}
		if !found || terminal(parent.Status) || parent.Input.Command.Action != "release" || parent.Input.OwnerTenantID != request.TenantID || parent.Input.Sandbox.ID != request.SandboxID || parent.Input.Source.OperationID != resource.OperationID || parent.Input.SourceInstance != request.ExpectedInstance || stopLink.ChildID != request.StopOperationID || fence != parent.OperationID {
			return fmt.Errorf("capture discard release authority changed")
		}
		if !admitting {
			link, _, err := loadResourceLink(ctx, tx, parent.OperationID, "failed-capture:"+source.OperationID)
			if err != nil {
				return err
			}
			if link.ChildID != step.OperationID {
				return fmt.Errorf("capture discard release child changed")
			}
		}
		allowedState = sandbox.State == parent.Input.Sandbox.State
	}
	if !reflect.DeepEqual(sandbox, source.Input.Sandbox) || sandbox.SourceOperation != resource.OperationID || sandbox.Instance != request.ExpectedInstance || !allowedState {
		return fmt.Errorf("capture discard current sandbox ownership changed")
	}
	vm, current, owner, err := service.loadVM(ctx, tx, resource)
	if err != nil {
		return err
	}
	expectedOwner := step.OperationID
	if admitting {
		expectedOwner = ""
	}
	var phase string
	if err = tx.QueryRowContext(ctx, "SELECT state FROM vms WHERE id=?", resource.Candidate).Scan(&phase); err != nil {
		return err
	}
	if owner != expectedOwner || current != request.ExpectedInstance || phase != "stopped" || vm.Deleting || !vm.Verified || vm.AgentReady || !reflect.DeepEqual(vm.Resource, resource.Input.Resource) {
		return fmt.Errorf("capture discard current stopped instance changed")
	}
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operationResult
	var original, body []byte
	var hash, status, tenant, scope string
	if err = tx.QueryRowContext(ctx, "SELECT request,request_hash,status,scope_kind,scope_id,body FROM operations WHERE id=?", source.OperationID).Scan(&original, &hash, &status, &scope, &tenant, &body); err != nil {
		return err
	}
	if err = json.Unmarshal(original, &frozen); err != nil {
		return err
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if digest(original) != hash || status != "failed" || scope != "tenant" || tenant != request.TenantID || frozen.Action != "snapshot" || frozen.Request != source.Input.Request || result.Error != source.State.Error || result.SnapshotID != source.Input.Snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandbox.ID {
		return fmt.Errorf("capture discard original terminal result changed")
	}
	snapshot := source.Input.Snapshot
	if snapshot.OperationID != source.OperationID || snapshot.TenantID != request.TenantID || snapshot.SourceVM != resource.Candidate || snapshot.SourceInstance != request.ExpectedInstance || snapshot.Manifest != nil || snapshot.Dir != filepath.Join(service.store.directory, "snapshots", snapshot.ID) {
		return fmt.Errorf("capture discard destination changed")
	}
	deleting, cleanupStatus, holderKind, holderID := 0, "blocked", "engine_dependency", source.OperationID+":capture"
	if !admitting {
		deleting = 1
		cleanupStatus = "transferred"
		holderKind = "cleanup"
		holderID = step.OperationID + ":capture_discard"
	}
	var record snapshotRecord
	var actualDeleting, published int
	if err = tx.QueryRowContext(ctx, "SELECT published,deleting,body FROM snapshots WHERE id=? AND source_vm_id=?", snapshot.ID, resource.Candidate).Scan(&published, &actualDeleting, &body); err != nil {
		return err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return err
	}
	if published != 0 || actualDeleting != deleting || !reflect.DeepEqual(record, snapshot) {
		return fmt.Errorf("capture discard unpublished catalog changed")
	}
	if err = tx.QueryRowContext(ctx, "SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='snapshot' AND resource_id=? AND status=?", "capture:"+source.OperationID, source.OperationID, snapshot.ID, cleanupStatus).Scan(&body); err != nil {
		return err
	}
	if string(body) != string(encode(source.Input)) {
		return fmt.Errorf("capture discard original cleanup changed")
	}
	shape := resource.Input.Resource.Config.Shape
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture'", []any{
			snapshot.ID,
			request.TenantID,
			source.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", []any{
			"snapshot:" + snapshot.ID,
			(2*shape.MemoryMiB + shape.DiskMiB + 224) << 20,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{
			request.TenantID,
			source.OperationID + ":capture",
			shape.DiskMiB + shape.MemoryMiB,
			source.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?", []any{
			request.TenantID,
			source.OperationID + ":capture",
		}, 1},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind=? AND holder_id=?", []any{
			request.TenantID,
			source.OperationID + ":capture",
			holderKind,
			holderID,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{
			resource.Candidate,
			source.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{
			resource.Input.Resource.Config.Image,
			source.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM source_leases WHERE snapshot_id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM checkpoints WHERE id=?", []any{
			snapshot.ID,
		}, 0},
	}
	if !admitting {
		checks = append(checks, struct {
			q    string
			args []any
			want int
		}{"SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='snapshot' AND resource_id=? AND status='pending' AND body=?", []any{
			"capture_discard:" + step.OperationID,
			step.OperationID,
			snapshot.ID,
			encode(input),
		}, 1})
		var parent struct {
			Action  string
			Request CaptureDiscardRequest
		}
		if err = tx.QueryRowContext(ctx, "SELECT request,request_hash,status,scope_kind,scope_id,body FROM operations WHERE id=?", step.OperationID).Scan(&original, &hash, &status, &scope, &tenant, &body); err != nil {
			return err
		}
		if err = json.Unmarshal(original, &parent); err != nil {
			return err
		}
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		if digest(original) != hash || terminal(status) || scope != "tenant" || tenant != request.TenantID || parent.Action != "capture_discard" || parent.Request != request || result.SnapshotID != snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandbox.ID {
			return fmt.Errorf("capture discard parent identity changed")
		}
	}
	for _, check := range checks {
		var count int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&count); err != nil || count != check.want {
			return fmt.Errorf("capture discard responsibility changed: %v", err)
		}
	}
	return nil
}
