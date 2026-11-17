package sandbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type LifecycleInput struct {
	CaptureFailures []ownership.GenerationRef `json:"capture_failures,omitempty"`
	PauseOperation  string                    `json:"pause_operation,omitempty"`
	PriorFence      string                    `json:"prior_fence,omitempty"`
	Rollback        *RollbackInput            `json:"rollback,omitempty"`
	SourceInstance  string                    `json:"source_instance"`
	CheckpointID    string                    `json:"checkpoint_id,omitempty"`
	RequestTenantID string                    `json:"request_tenant_id"`
	OwnerTenantID   string                    `json:"owner_tenant_id"`
	Command         operation.Command         `json:"command"`
	Sandbox         ownership.Sandbox         `json:"sandbox"`
	Settings        Settings                  `json:"settings"`
	Source          ownership.GenerationRef   `json:"source"`
}

type RollbackInput struct {
	RawCaptures []ownership.GenerationRef `json:"raw_captures,omitempty"`
	Checkpoint  Checkpoint                `json:"checkpoint"`
	Source      machine.SnapshotSource    `json:"source"`
}

type LifecycleState struct {
	Phase      string `json:"phase"`
	SnapshotID string `json:"snapshot_id,omitempty"`
}

type LifecycleStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       LifecycleInput
	State       LifecycleState
}

func LoadLifecycle(ctx context.Context, database store.DBTX, id string) (LifecycleStep, error) {
	step := LifecycleStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "lifecycle",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, instance := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("lifecycle input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != step.Input.Command.Action || candidate != step.Input.Sandbox.ID || instance != step.Input.Sandbox.Instance {
		return step, fmt.Errorf("lifecycle frozen identity changed")
	}
	return step, nil
}
