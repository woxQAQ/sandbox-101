package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
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
	Source          ResourceStep   `json:"source"`
	CurrentInstance string         `json:"current_instance"`
}

type discardState struct {
	Phase string `json:"phase"`
}

type DiscardStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       discardInput
	State       discardState
}

func LoadDiscard(ctx context.Context, query store.DBTX, operationID string) (DiscardStep, error) {
	step := DiscardStep{
		OperationID: operationID,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: operationID,
		StepKey:     "discard",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
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
