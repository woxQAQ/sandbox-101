package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type CaptureRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type snapshotManifest struct {
	Format          int               `json:"format"`
	Backend         string            `json:"backend"`
	Architecture    string            `json:"architecture"`
	CPU             string            `json:"cpu"`
	Config          loom.Config       `json:"config"`
	MemoryMode      string            `json:"memory_mode"`
	PublicationMode string            `json:"publication_mode"`
	Files           map[string]string `json:"files"`
	Image           json.RawMessage   `json:"image"`
}

type snapshotRecord struct {
	ID             string            `json:"id"`
	TenantID       string            `json:"tenant_id"`
	OperationID    string            `json:"operation_id"`
	SourceVM       string            `json:"source_vm"`
	SourceInstance string            `json:"source_instance"`
	Dir            string            `json:"dir"`
	Manifest       *snapshotManifest `json:"manifest,omitempty"`
}

type captureInput struct {
	Request      CaptureRequest `json:"request"`
	Source       resourceStep   `json:"source"`
	Sandbox      sandboxRecord  `json:"sandbox"`
	Snapshot     snapshotRecord `json:"snapshot"`
	Backend      string         `json:"backend"`
	Architecture string         `json:"architecture"`
	CPU          string         `json:"cpu"`
}

type captureState struct {
	Phase           string                `json:"phase"`
	Boundary        *loom.CaptureBoundary `json:"boundary,omitempty"`
	ResumeRequired  bool                  `json:"resume_required"`
	Captured        bool                  `json:"captured"`
	Materialized    bool                  `json:"materialized"`
	Files           map[string]string     `json:"files,omitempty"`
	ArtifactsSynced bool                  `json:"artifacts_synced"`
	Error           string                `json:"error,omitempty"`
}

type captureStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       captureInput
	State       captureState
}

func loadCapture(ctx context.Context, query queryer, operation string) (captureStep, error) {
	step := captureStep{OperationID: operation}
	var input, body []byte
	var action, candidate, instance string
	err := query.QueryRowContext(ctx, "SELECT action,input_hash,input,candidate_resource_id,expected_instance,status,body FROM operation_steps WHERE operation_id=? AND step_key='capture'", operation).Scan(&action, &step.Hash, &input, &candidate, &instance, &step.Status, &body)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("capture input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "snapshot" || candidate != step.Input.Snapshot.ID || instance != step.Input.Request.ExpectedInstance || !loom.ValidResourceID(candidate, "snap-") {
		return step, fmt.Errorf("capture identity changed")
	}
	return step, nil
}

func (service *Service) captureProof(ctx context.Context, tx *sql.Tx, step captureStep) (vmRecord, error) {
	input := step.Input
	source := input.Source
	var empty vmRecord
	if err := service.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone") || source.Status != "succeeded" || source.State.Witness == "" || source.State.Witness != input.Request.ExpectedInstance || source.State.Witness != source.Input.Resource.Instance || source.State.Observation == nil || source.State.DiskIdentity == nil || source.Input.TenantID != input.Request.TenantID || source.Input.SandboxID != input.Request.SandboxID {
		return empty, fmt.Errorf("capture frozen source witness missing")
	}
	historical, err := loadStep(ctx, tx, source.OperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(historical, source) {
		return empty, fmt.Errorf("capture historical source changed")
	}
	snapshot := input.Snapshot
	if snapshot.OperationID != step.OperationID || snapshot.SourceVM != source.Candidate || snapshot.SourceInstance != source.State.Witness || snapshot.TenantID != input.Request.TenantID || snapshot.Dir != filepath.Join(service.store.directory, "snapshots", snapshot.ID) || snapshot.Manifest != nil {
		return empty, fmt.Errorf("capture destination changed")
	}
	sandbox, err := loadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(sandbox, input.Sandbox) || sandbox.VMID != source.Candidate || sandbox.Instance != source.State.Witness || sandbox.SourceOperation != source.OperationID || (sandbox.State != "provisioning" && sandbox.State != "running") {
		return empty, fmt.Errorf("capture sandbox ownership changed")
	}
	var vm vmRecord
	var current, owner, state string
	var body []byte
	if err = tx.QueryRowContext(ctx, "SELECT current_instance,operation_id,state,body FROM vms WHERE id=?", source.Candidate).Scan(&current, &owner, &state, &body); err != nil {
		return vm, err
	}
	if err = json.Unmarshal(body, &vm); err != nil {
		return vm, err
	}
	if current != source.State.Witness || owner != step.OperationID || state != "running" || vm.Deleting || !vm.Verified || !vm.AgentReady || !reflect.DeepEqual(vm.Resource, source.Input.Resource) {
		return vm, fmt.Errorf("capture current instance ownership changed")
	}
	var request, parent []byte
	var hash, tenant, scope, status string
	if err = tx.QueryRowContext(ctx, "SELECT request,request_hash,scope_kind,scope_id,status,body FROM operations WHERE id=?", step.OperationID).Scan(&request, &hash, &scope, &tenant, &status, &parent); err != nil {
		return vm, err
	}
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operationResult
	if err = json.Unmarshal(request, &frozen); err != nil {
		return vm, err
	}
	if err = json.Unmarshal(parent, &result); err != nil {
		return vm, err
	}
	if digest(request) != hash || scope != "tenant" || tenant != input.Request.TenantID || terminal(status) || frozen.Action != "snapshot" || frozen.Request != input.Request || result.SnapshotID != snapshot.ID || result.VMID != source.Candidate || result.SandboxID != sandbox.ID {
		return vm, fmt.Errorf("capture parent identity changed")
	}
	var record snapshotRecord
	var published, deleting int
	if err = tx.QueryRowContext(ctx, "SELECT published,deleting,body FROM snapshots WHERE id=? AND source_vm_id=?", snapshot.ID, source.Candidate).Scan(&published, &deleting, &body); err != nil {
		return vm, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return vm, err
	}
	if published != 0 || deleting != 0 || !reflect.DeepEqual(record, snapshot) {
		return vm, fmt.Errorf("capture unpublished destination changed")
	}
	var versionBody []byte
	if err = tx.QueryRowContext(ctx, "SELECT body FROM resource_versions WHERE id=? AND vm_id=? AND operation_id=? AND step_key=?", source.OperationID, source.Candidate, source.OperationID, source.Key).Scan(&versionBody); err != nil {
		return vm, err
	}
	var version resourceVersion
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return vm, err
	}
	if !reflect.DeepEqual(version, resourceVersion{Resource: source.Input.Resource, DiskIdentity: source.State.DiskIdentity}) {
		return vm, fmt.Errorf("capture source version proof changed")
	}
	image, err := loadImage(ctx, tx, source.Input.Resource.Config.Image)
	if err != nil || !reflect.DeepEqual(image, source.Input.Image) {
		return vm, fmt.Errorf("capture source image publication changed: %v", err)
	}
	shape := vm.Resource.Config.Shape
	checks := []struct {
		q    string
		args []any
	}{
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND storage_mib=? AND source_operation_id=?", []any{snapshot.TenantID, source.Candidate, shape.DiskMiB, source.OperationID}},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='sandbox' AND holder_id=?", []any{source.Candidate, sandbox.ID}},
		{"SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture'", []any{snapshot.ID, snapshot.TenantID, step.OperationID}},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", []any{"snapshot:" + snapshot.ID, (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20}},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{snapshot.TenantID, step.OperationID + ":capture", shape.MemoryMiB + shape.DiskMiB, step.OperationID}},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{snapshot.TenantID, step.OperationID + ":capture", step.OperationID + ":capture"}},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{source.Candidate, step.OperationID}},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{vm.Resource.Config.Image, step.OperationID}},
	}
	for _, check := range checks {
		var n int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&n); err != nil || n != 1 {
			return vm, fmt.Errorf("capture responsibility missing: %v", err)
		}
	}
	return vm, nil
}
