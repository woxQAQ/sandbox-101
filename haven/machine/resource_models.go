package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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
	row, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
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
	input, body := row.Input, row.Body
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
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

// A request type fixes the action used by every writer and reader.
type provisionAction interface {
	provisionAction() string
}

// The action and field order are part of the hashed operation request. Claim
// is present only for admissions that also freeze a sanitized sandbox command.
type provisionRequest[T provisionAction] struct {
	Action  string
	Request T
	Claim   *operation.Command `json:"Claim,omitempty"`
}

func (FreshRequest) provisionAction() string   { return "claim" }
func (CloneRequest) provisionAction() string   { return "clone" }
func (RestoreRequest) provisionAction() string { return "restore" }

func FreezeProvisionRequest[T provisionAction](request T, claim *operation.Command) []byte {
	return operation.Encode(provisionRequest[T]{Action: request.provisionAction(), Request: request, Claim: claim})
}

func DecodeProvisionRequest[T provisionAction](body []byte) (T, error) {
	var frozen provisionRequest[T]
	if err := json.Unmarshal(body, &frozen); err != nil {
		return frozen.Request, err
	}
	if frozen.Action != frozen.Request.provisionAction() {
		return frozen.Request, fmt.Errorf("unexpected provision request action %q", frozen.Action)
	}
	return frozen.Request, nil
}

// ResourceAdmission carries one child's frozen identity through the parent
// transaction. Its fields are required facts, not optional execution modes.
type ResourceAdmission struct {
	Child       operation.Record
	Kind        string
	Candidate   string
	RequestHash string
}

// ResourceParent joins a control workflow to a resource admission transaction.
// Implementations verify their frozen intent; Loom never receives this port.
type ResourceParent interface {
	AdmitResource(context.Context, *sql.Tx, ResourceAdmission) error
}

func AdmitResourceParent(ctx context.Context, tx *sql.Tx, parent ResourceParent, request ResourceAdmission) error {
	if parent == nil {
		return nil
	}
	return parent.AdmitResource(ctx, tx, request)
}
