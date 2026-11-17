package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type DirectRetirementInput struct {
	Source         ResourceStep        `json:"source"`
	ActivationHash string              `json:"activation_hash"`
	ActivationKey  string              `json:"activation_key"`
	Policy         *loom.NetworkPolicy `json:"policy"`
}

type DirectRetirementState struct {
	Phase     string `json:"phase"`
	RetiredBy string `json:"retired_by,omitempty"`
}

type DirectRetirementStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       DirectRetirementInput
	State       DirectRetirementState
}

func LoadDirectRetirement(ctx context.Context, query store.DBTX, id string) (DirectRetirementStep, error) {
	step := DirectRetirementStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{OperationID: id, StepKey: "direct_retirement"})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(record.Input) != step.Hash {
		return step, fmt.Errorf("direct retirement intent changed")
	}
	if err = json.Unmarshal(record.Input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(record.Body, &step.State); err != nil {
		return step, err
	}
	if record.Action != "direct_retirement" || record.CandidateResourceID != step.Input.Source.Candidate || record.ExpectedInstance != step.Input.Source.State.Witness || step.Input.Policy == nil {
		return step, fmt.Errorf("direct retirement source identity changed")
	}
	return step, nil
}
