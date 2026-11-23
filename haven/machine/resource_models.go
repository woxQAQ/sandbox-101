package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// QuotaDestination freezes the parent workflow that receives a resource slot.
type QuotaDestination struct {
	OperationID string `json:"operation_id"`
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	SnapshotID  string `json:"snapshot_id"`
}

// ResourceInput is the cross-domain intent; it is not a mutable VM record.
type ResourceInput struct {
	SlotAssignment         *ownership.GenerationRef  `json:"slot_assignment,omitempty"`
	SlotBudget             quota.SlotBudget          `json:"slot_budget"`
	FailureSlotDestination *QuotaDestination         `json:"failure_slot_destination,omitempty"`
	SlotDestination        *QuotaDestination         `json:"slot_destination,omitempty"`
	StopOperation          string                    `json:"stop_operation,omitempty"`
	Ancestors              []ownership.GenerationRef `json:"ancestors,omitempty"`
	DiskKey                string                    `json:"disk_key"`
	SlotOperation          string                    `json:"slot_operation"`
	RuntimeBytes           int64                     `json:"runtime_bytes"`
	BackingHolder          string                    `json:"backing_holder,omitempty"`
	RestoreMaterials       *loom.RestoreMaterials    `json:"restore_materials,omitempty"`
	Restore                *SnapshotSource           `json:"restore,omitempty"`
	Captures               []CaptureStep             `json:"captures,omitempty"`
	DiskIdentity           *loom.FileIdentity        `json:"disk_identity,omitempty"`
	Resource               loom.Resource             `json:"resource"`
	Image                  host.ImageManifest        `json:"image"`
	TemplateID             string                    `json:"template_id"`
	TenantID               string                    `json:"tenant_id"`
	SandboxID              string                    `json:"sandbox_id"`
	SourceOperation        string                    `json:"source_operation,omitempty"`
}

type ResourceState struct {
	Materials      *loom.RestoreMaterials `json:"restore_materials,omitempty"`
	BackingReady   bool                   `json:"backing_ready,omitempty"`
	RestoreBlocked bool                   `json:"restore_blocked,omitempty"`
	DiskIdentity   *loom.FileIdentity     `json:"disk_identity,omitempty"`
	Observation    *loom.Observation      `json:"observation,omitempty"`
	Phase          string                 `json:"phase"`
	Witness        string                 `json:"result_instance,omitempty"`
	Error          string                 `json:"error,omitempty"`
}

type ResourceStep struct {
	OperationID      string
	Key              string
	Action           string
	Hash             string
	Status           string
	Candidate        string
	ExpectedInstance string
	Input            ResourceInput
	State            ResourceState
}

func LoadResourceStep(ctx context.Context, query store.DBTX, id string) (ResourceStep, error) {
	var step ResourceStep
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		step, err = readResourceStep(ctx, tx, id)
		return err
	})
	return step, err
}

func readResourceStep(ctx context.Context, tx *sql.Tx, id string) (ResourceStep, error) {
	q := dbgen.New(tx)
	row, err := q.GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "resource",
	})
	if err != nil {
		row = dbgen.GetStepRow{}
	}
	step := ResourceStep{
		OperationID:      row.OperationID,
		Key:              row.StepKey,
		Action:           row.Action,
		Status:           row.Status,
		Hash:             row.InputHash,
		Candidate:        row.CandidateResourceID,
		ExpectedInstance: row.ExpectedInstance,
	}
	input := row.Input
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("resource step input checksum mismatch")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	progress, err := q.GetResourceProgress(ctx, id)
	if err != nil {
		return step, fmt.Errorf("resource progress missing for %s: %v", id, err)
	}
	snapshot := ""
	if step.Input.Restore != nil {
		snapshot = step.Input.Restore.Snapshot.ID
	}
	if progress.RestoreSnapshotID.String != snapshot || progress.BackingHolder.String != step.Input.BackingHolder {
		return step, fmt.Errorf("resource backing provenance changed")
	}
	step.State = ResourceState{
		Phase:          row.Phase.String,
		BackingReady:   progress.BackingReady,
		RestoreBlocked: progress.RestoreBlocked,
		Witness:        progress.ResultInstance,
		Error:          progress.Error,
	}
	if progress.DiskDevice.Valid {
		step.State.DiskIdentity = &loom.FileIdentity{
			Device: uint64(progress.DiskDevice.Int64),
			Inode:  uint64(progress.DiskInode.Int64),
			Mode:   uint32(progress.DiskMode.Int64),
		}
	}
	if progress.MaterialsHash.Valid {
		step.State.Materials, err = readMaterials(ctx, q, id, step.Input.Resource)
		if err != nil {
			return step, fmt.Errorf("resource restore proof unavailable: %v", err)
		}
		if operation.Digest(operation.Encode(step.State.Materials)) != progress.MaterialsHash.String {
			return step, fmt.Errorf("resource restore proof changed")
		}
	}
	step.State.Observation = launchObservation(progress)
	if step.Input.Resource.VMID != step.Candidate || step.Input.TenantID == "" || step.Input.SandboxID == "" || step.Input.Resource.Instance == "" {
		return step, fmt.Errorf("resource step candidate proof missing")
	}
	return step, nil
}

func transitionResource(ctx context.Context, tx *sql.Tx, step ResourceStep, next ResourceState, status string) error {
	current, err := readResourceStep(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("resource step %s changed before transition", step.OperationID)
	}
	device, inode, mode := diskColumns(next.DiskIdentity)
	params := dbgen.SetResourceProgressParams{
		OperationID:    step.OperationID,
		BackingReady:   next.BackingReady,
		RestoreBlocked: next.RestoreBlocked,
		DiskDevice:     device,
		DiskInode:      inode,
		DiskMode:       mode,
		ResultInstance: next.Witness,
		Error:          next.Error,
	}
	q := dbgen.New(tx)
	if !reflect.DeepEqual(step.State.Materials, next.Materials) {
		if err = replaceMaterials(ctx, q, step, next.Materials); err != nil {
			return err
		}
	}
	if next.Materials != nil {
		params.MaterialsHash = sql.NullString{
			String: operation.Digest(operation.Encode(next.Materials)),
			Valid:  true,
		}
	}
	if o := next.Observation; o != nil {
		params.ObservationPresent = true
		params.Alive = o.Alive
		params.Paused = o.Paused
		params.Booted = o.Booted
		params.Configured = o.Configured
		params.Pid = int64(o.PID)
		params.StartTime = int64(o.StartTime)
		params.Network = o.Network
	}
	if err = store.RequireOne(q.SetResourceProgress(ctx, params)); err != nil {
		return err
	}
	return store.RequireOne(q.TransitionResource(ctx, dbgen.TransitionResourceParams{
		OperationID:    step.OperationID,
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		NextStatus:     status,
		PreviousPhase: sql.NullString{
			String: step.State.Phase,
			Valid:  true,
		},
		NextPhase: sql.NullString{
			String: next.Phase,
			Valid:  true,
		},
	}))
}

func SlotDestinationHolder(destination *QuotaDestination) string {
	switch destination.Kind {
	case "sleep":
		return "sleep:" + destination.ID
	case "claim":
		return "claim:" + destination.OperationID
	default:
		return ""
	}
}

type FreshRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	TemplateID string `json:"template_id"`
}

type ReleaseRequest struct {
	TenantID  string `json:"tenant_id"`
	Key       string `json:"key"`
	SandboxID string `json:"sandbox_id"`
}

type CloneRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}

type RestoreRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	SnapshotID       string `json:"snapshot_id"`
	ExpectedInstance string `json:"expected_instance"`
	StopOperationID  string `json:"stop_operation_id"`
}

type provisionRequest interface {
	FreshRequest | CloneRequest | RestoreRequest
	// operationRequest fixes the resource action and parameters used for replay.
	operationRequest() operation.Request
}

// FreezeProvisionRequest binds the original resource parameters and optional tenant command.
func FreezeProvisionRequest[T provisionRequest](request T, claim *operation.Intent) operation.Request {
	r := request.operationRequest()
	r.Command = claim
	return r
}

func (r FreshRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "claim",
		Resource: &operation.ResourceRequest{
			TenantID:   r.TenantID,
			Key:        r.Key,
			TemplateID: r.TemplateID,
		},
	}
}

func (r CloneRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "clone",
		Resource: &operation.ResourceRequest{
			TenantID:   r.TenantID,
			Key:        r.Key,
			SnapshotID: r.SnapshotID,
		},
	}
}

func (r RestoreRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "restore",
		Resource: &operation.ResourceRequest{
			TenantID:         r.TenantID,
			Key:              r.Key,
			SandboxID:        r.SandboxID,
			SnapshotID:       r.SnapshotID,
			ExpectedInstance: r.ExpectedInstance,
			StopOperationID:  r.StopOperationID,
		},
	}
}

func (r ReleaseRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "release",
		Resource: &operation.ResourceRequest{
			TenantID:  r.TenantID,
			Key:       r.Key,
			SandboxID: r.SandboxID,
		},
	}
}

func (r StopRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "stop",
		Resource: &operation.ResourceRequest{
			TenantID:         r.TenantID,
			Key:              r.Key,
			SandboxID:        r.SandboxID,
			ExpectedInstance: r.ExpectedInstance,
		},
	}
}

func (r CaptureRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "snapshot",
		Resource: &operation.ResourceRequest{
			TenantID:         r.TenantID,
			Key:              r.Key,
			SandboxID:        r.SandboxID,
			ExpectedInstance: r.ExpectedInstance,
		},
	}
}

func (r CaptureCleanupRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "capture_cleanup",
		Resource: &operation.ResourceRequest{
			TenantID:          r.TenantID,
			Key:               r.Key,
			SourceOperationID: r.CaptureOperationID,
			StopOperationID:   r.StopOperationID,
		},
	}
}

func (r CaptureDiscardRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "capture_discard",
		Resource: &operation.ResourceRequest{
			TenantID:          r.TenantID,
			Key:               r.Key,
			SandboxID:         r.SandboxID,
			SourceOperationID: r.FailedOperationID,
			ExpectedInstance:  r.ExpectedInstance,
			StopOperationID:   r.StopOperationID,
		},
	}
}

func (r DiscardRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "discard",
		Resource: &operation.ResourceRequest{
			TenantID:          r.TenantID,
			Key:               r.Key,
			SandboxID:         r.SandboxID,
			ExpectedInstance:  r.ExpectedInstance,
			SourceOperationID: r.FailedOperationID,
		},
	}
}

func (r SnapshotDeleteRequest) operationRequest() operation.Request {
	return operation.Request{
		Action: "snapshot_delete",
		Resource: &operation.ResourceRequest{
			TenantID:   r.TenantID,
			Key:        r.Key,
			SnapshotID: r.SnapshotID,
		},
	}
}

// ResourceAdmission carries one child's frozen identity through the parent
// transaction. Its fields are required facts, not optional execution modes.
type ResourceAdmission struct {
	Child     operation.State
	Kind      string
	Candidate string
	Request   operation.Request
}

// ResourceParent joins a control workflow to a resource admission transaction.
// Implementations verify their frozen intent; Loom never receives this port.
type ResourceParent interface {
	// AdmitResource records the parent handoff in the resource admission transaction.
	AdmitResource(ctx context.Context, tx *sql.Tx, admission ResourceAdmission) error
}

func AdmitResourceParent(ctx context.Context, tx *sql.Tx, parent ResourceParent, request ResourceAdmission) error {
	if parent == nil {
		return nil
	}
	return parent.AdmitResource(ctx, tx, request)
}
