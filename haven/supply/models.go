package supply

import (
	"context"
	"database/sql"
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
	RequesterRoot   bool                    `json:"requester_root,omitempty"`
	CreatedAt       time.Time               `json:"created_at"`
	RequestTenantID string                  `json:"request_tenant_id"`
	Command         operation.Intent        `json:"command"`
	Template        TemplateRecord          `json:"template"`
	Previous        *TemplateRecord         `json:"previous,omitempty"`
	Source          *machine.SnapshotSource `json:"source,omitempty"`
	SandboxID       string                  `json:"sandbox_id"`
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

func supplyTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}

func insertSupply(ctx context.Context, tx *sql.Tx, id, action, candidate, status string, input []byte, state SupplyState) error {
	return store.RequireOne(dbgen.New(tx).InsertSupply(ctx, dbgen.InsertSupplyParams{
		OperationID:         id,
		Action:              action,
		CandidateResourceID: candidate,
		Input:               input,
		InputHash:           operation.Digest(input),
		Status:              status,
		Phase: sql.NullString{
			String: state.Phase,
			Valid:  true,
		},
		CompletedAt: supplyTime(state.FinishedAt),
		StepError:   state.Error,
	}))
}

func transitionSupply(ctx context.Context, q store.DBTX, step SupplyStep, next SupplyState, status string) error {
	return store.RequireOne(dbgen.New(q).TransitionSupply(ctx, dbgen.TransitionSupplyParams{
		OperationID:    step.OperationID,
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase: sql.NullString{
			String: step.State.Phase,
			Valid:  true,
		},
		PreviousCompletion: supplyTime(step.State.FinishedAt),
		PreviousSnapshot:   step.State.SnapshotID,
		PreviousError:      step.State.Error,
		NextPhase: sql.NullString{
			String: next.Phase,
			Valid:  true,
		},
		NextStatus:       status,
		CompletedAt:      supplyTime(next.FinishedAt),
		ResultSnapshotID: next.SnapshotID,
		StepError:        next.Error,
	}))
}

func LoadSupply(ctx context.Context, database store.DBTX, id string) (SupplyStep, error) {
	step := SupplyStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "supply",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input := record.Input
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
	step.State = SupplyState{
		Phase:      record.Phase.String,
		SnapshotID: record.ResultSnapshotID,
		Error:      record.StepError,
	}
	if record.CompletedAt != "" {
		if step.State.FinishedAt, err = time.Parse(time.RFC3339Nano, record.CompletedAt); err != nil {
			return step, err
		}
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
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "supply_cleanup",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload := record.Input
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
	step.Phase = record.Phase.String
	if action != "supply_cleanup" || candidate != step.Input.Source.Candidate || expected != step.Input.Source.Input.Resource.Instance || step.Input.Request.OperationID != step.Input.Original.OperationID {
		return step, fmt.Errorf("supply cleanup frozen identity changed")
	}
	return step, nil
}
