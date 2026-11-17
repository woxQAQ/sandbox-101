package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type StopRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type StopInput struct {
	Lifecycle       string `json:"lifecycle,omitempty"`
	Request         StopRequest
	SourceOperation string
	SourceHash      string
}

type StopStep struct {
	OperationID string
	Hash        string
	Status      string
	Phase       string
	Input       StopInput
}

func LoadStop(ctx context.Context, database store.DBTX, id string) (StopStep, error) {
	step := StopStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "stop",
	})
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
		return step, fmt.Errorf("stop input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "stop" || candidate != step.Input.Request.SandboxID || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("stop frozen identity changed")
	}
	return step, nil
}
