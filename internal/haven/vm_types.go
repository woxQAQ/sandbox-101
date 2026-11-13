package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

type ColdRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	TemplateID string `json:"template_id"`
}

type ReleaseRequest struct {
	TenantID  string `json:"tenant_id"`
	Key       string `json:"key"`
	SandboxID string `json:"sandbox_id"`
}

type Operation struct {
	ID     string          `json:"id"`
	Key    string          `json:"key"`
	Status string          `json:"status"`
	Result operationResult `json:"result"`
}

type operationResult struct {
	SandboxIDs    []string `json:"sandbox_ids,omitempty"`
	ImageDigest   string   `json:"image_digest,omitempty"`
	TemplateID    string   `json:"template_id,omitempty"`
	CheckpointID  string   `json:"checkpoint_id,omitempty"`
	SnapshotID    string   `json:"snapshot_id,omitempty"`
	SandboxID     string   `json:"sandbox_id"`
	VMID          string   `json:"vm_id"`
	ResourceReady bool     `json:"resource_ready,omitempty"`
	Error         string   `json:"error,omitempty"`
}

type tenantRecord struct {
	MaxSandboxes  int64 `json:"max_sandboxes"`
	MaxStorageMiB int64 `json:"max_storage_mib"`
}

type templateRecord struct {
	Version    string      `json:"version"`
	State      string      `json:"state"`
	Warmup     string      `json:"warmup,omitempty"`
	ID         string      `json:"id"`
	TenantID   string      `json:"tenant_id"`
	Config     loom.Config `json:"config"`
	SnapshotID string      `json:"snapshot_id,omitempty"`
	TargetWarm int         `json:"target_warm"`
}

// sandboxIdentity fixes business birth independently of VM generations and pool age.
type sandboxIdentity struct {
	TemplateID      string     `json:"template_id"`
	Shape           loom.Shape `json:"shape"`
	Epoch           string     `json:"creation_epoch"`
	CreatedAt       time.Time  `json:"created_at"`
	ProvisionSource string     `json:"provision_source"`
}

type sandboxRecord struct {
	Identity        sandboxIdentity `json:"identity"`
	LastError       string          `json:"last_error,omitempty"`
	SlotAssignment  *generationRef  `json:"slot_assignment,omitempty"`
	ID              string          `json:"id"`
	TenantID        string          `json:"tenant_id"`
	OperationID     string          `json:"operation_id"`
	VMID            string          `json:"vm_id"`
	Instance        string          `json:"instance,omitempty"`
	State           string          `json:"state"`
	SnapshotID      string          `json:"snapshot_id,omitempty"`
	SourceOperation string          `json:"source_operation"`
}

type vmRecord struct {
	Resource    loom.Resource     `json:"resource"`
	Observation *loom.Observation `json:"observation,omitempty"`
	Verified    bool              `json:"verified"`
	AgentReady  bool              `json:"agent_ready"`
	Deleting    bool              `json:"deleting"`
}

// slotBudget fixes the allocation's quota category and units independently of
// its physical network slot. Unclaimed warm allocations consume zero units.
type slotBudget struct {
	Kind  string `json:"kind"`
	Units int64  `json:"units"`
}

type provisionTarget struct {
	Activation    *claimAdmission
	Identity      *sandboxIdentity
	SlotHolder    string
	Budget        *slotBudget
	NewSandboxID  string
	Sandbox       sandboxRecord
	SlotOperation string
	Parent        resourceParent
}

type resourceVersion struct {
	Resource     loom.Resource      `json:"resource"`
	DiskIdentity *loom.FileIdentity `json:"disk_identity,omitempty"`
}

// generationRef names an immutable completed resource step without copying ancestry.
type generationRef struct {
	OperationID string `json:"operation_id"`
	InputHash   string `json:"input_hash"`
}

type quotaDestination struct {
	OperationID string `json:"operation_id"`
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	SnapshotID  string `json:"snapshot_id"`
}

type stepInput struct {
	SlotAssignment         *generationRef         `json:"slot_assignment,omitempty"`
	SlotBudget             slotBudget             `json:"slot_budget"`
	FailureSlotDestination *quotaDestination      `json:"failure_slot_destination,omitempty"`
	SlotDestination        *quotaDestination      `json:"slot_destination,omitempty"`
	StopOperation          string                 `json:"stop_operation,omitempty"`
	Ancestors              []generationRef        `json:"ancestors,omitempty"`
	DiskKey                string                 `json:"disk_key"`
	SlotOperation          string                 `json:"slot_operation"`
	RuntimeBytes           int64                  `json:"runtime_bytes"`
	BackingHolder          string                 `json:"backing_holder,omitempty"`
	RestoreMaterials       *loom.RestoreMaterials `json:"restore_materials,omitempty"`
	Restore                *restoreSource         `json:"restore,omitempty"`
	Captures               []captureStep          `json:"captures,omitempty"`
	DiskIdentity           *loom.FileIdentity     `json:"disk_identity,omitempty"`
	Resource               loom.Resource          `json:"resource"`
	Image                  host.ImageManifest     `json:"image"`
	TemplateID             string                 `json:"template_id"`
	TenantID               string                 `json:"tenant_id"`
	SandboxID              string                 `json:"sandbox_id"`
	SourceOperation        string                 `json:"source_operation,omitempty"`
}

type stepState struct {
	Materials      *loom.RestoreMaterials `json:"restore_materials,omitempty"`
	BackingReady   bool                   `json:"backing_ready,omitempty"`
	RestoreBlocked bool                   `json:"restore_blocked,omitempty"`
	DiskIdentity   *loom.FileIdentity     `json:"disk_identity,omitempty"`
	Observation    *loom.Observation      `json:"observation,omitempty"`
	Phase          string                 `json:"phase"`
	Witness        string                 `json:"result_instance,omitempty"`
	Error          string                 `json:"error,omitempty"`
}

type resourceStep struct {
	OperationID      string
	Key              string
	Action           string
	Status           string
	Hash             string
	Candidate        string
	ExpectedInstance string
	Input            stepInput
	State            stepState
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadStep(ctx context.Context, query queryer, operation string) (resourceStep, error) {
	var step resourceStep
	var input, body []byte
	err := query.QueryRowContext(ctx, `SELECT operation_id,step_key,action,status,input_hash,input,candidate_resource_id,expected_instance,body FROM operation_steps WHERE operation_id=? AND step_key='resource'`, operation).Scan(&step.OperationID, &step.Key, &step.Action, &step.Status, &step.Hash, &input, &step.Candidate, &step.ExpectedInstance, &body)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("resource step input checksum mismatch")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if step.Input.Resource.VMID != step.Candidate || step.Input.TenantID == "" || step.Input.SandboxID == "" || step.Input.Resource.Instance == "" {
		return step, fmt.Errorf("resource step candidate proof missing")
	}
	return step, nil
}

func loadSandbox(ctx context.Context, query queryer, id, tenant string) (sandboxRecord, error) {
	var sandbox sandboxRecord
	var body []byte
	var state string
	if err := query.QueryRowContext(ctx, "SELECT state,body FROM sandboxes WHERE id=? AND tenant=?", id, tenant).Scan(&state, &body); err != nil {
		return sandbox, err
	}
	if err := json.Unmarshal(body, &sandbox); err != nil {
		return sandbox, err
	}
	if sandbox.ID != id || sandbox.TenantID != tenant || sandbox.State != state {
		return sandbox, fmt.Errorf("sandbox ownership metadata inconsistent")
	}
	return sandbox, nil
}

func terminal(status string) bool { return status == "succeeded" || status == "failed" }
