package supply

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

type SupplyInput struct {
	RequesterRoot   bool              `json:"requester_root,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	RequestTenantID string            `json:"request_tenant_id"`
	Command         operation.Command `json:"command"`
	Template        TemplateRecord    `json:"template"`
	Previous        *TemplateRecord   `json:"previous,omitempty"`
	Source          *machine.Source   `json:"source,omitempty"`
	SandboxID       string            `json:"sandbox_id"`
}

type SupplyState struct {
	FinishedAt time.Time `json:"finished_at"`
	Phase      string    `json:"phase"`
	SnapshotID string    `json:"snapshot_id,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type SupplyStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       SupplyInput
	State       SupplyState
}

type SupplyCleanupRequest struct {
	OperationID string `json:"operation_id"`
}

type SupplyCleanupInput struct {
	RequestTenantID string                    `json:"request_tenant_id"`
	Request         SupplyCleanupRequest      `json:"request"`
	Original        SupplyStep                `json:"original"`
	Source          machine.ResourceStep      `json:"source"`
	Pool            *PoolReservation          `json:"pool,omitempty"`
	Retired         *ownership.GenerationRef  `json:"retired,omitempty"`
	CaptureFailures []ownership.GenerationRef `json:"capture_failures,omitempty"`
}

type SupplyCleanupStep struct {
	OperationID string
	Hash        string
	Status      string
	Phase       string
	Input       SupplyCleanupInput
}

func LoadSupply(ctx context.Context, database store.DBTX, id string) (SupplyStep, error) {
	step := SupplyStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{OperationID: id, StepKey: "supply"})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	candidate, expected, action := record.CandidateResourceID, record.ExpectedInstance, record.Action
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("supply input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != step.Input.Command.Action || candidate != step.Input.Template.ID || expected != "" || step.Input.Template.TenantID == "" {
		return step, fmt.Errorf("supply frozen identity changed")
	}
	return step, nil
}

func LoadSupplyCleanup(ctx context.Context, database store.DBTX, id string) (SupplyCleanupStep, error) {
	step := SupplyCleanupStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{OperationID: id, StepKey: "supply_cleanup"})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("supply cleanup input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "supply_cleanup" || candidate != step.Input.Source.Candidate || expected != step.Input.Source.Input.Resource.Instance || step.Input.Request.OperationID != step.Input.Original.OperationID {
		return step, fmt.Errorf("supply cleanup frozen identity changed")
	}
	return step, nil
}
