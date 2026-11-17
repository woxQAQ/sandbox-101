package sandbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type ActivationInput struct {
	PreviousSettings  *Settings `json:"previous_settings,omitempty"`
	Attempt           string    `json:"attempt,omitempty"`
	AllowFallback     bool      `json:"allow_fallback,omitempty"`
	RequestTenantID   string    `json:"request_tenant_id"`
	TenantID          string    `json:"tenant_id"`
	SandboxID         string    `json:"sandbox_id"`
	ResourceOperation string    `json:"resource_operation"`
	ResourceHash      string    `json:"resource_hash"`
	ExpectedInstance  string    `json:"expected_instance"`
	Settings          Settings  `json:"settings"`
	ResetIdentity     bool      `json:"reset_identity"`
}

type ActivationStep struct {
	OperationID string
	Key         string
	Hash        string
	Status      string
	Phase       string
	Input       ActivationInput
}

func ActivationKey(attempt string) string {
	if attempt == "" {
		return "activation"
	}
	return "activation:" + attempt
}

func LoadActivation(ctx context.Context, query store.DBTX, id string) (ActivationStep, error) {
	step := ActivationStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetLatestActivationStep(ctx, id)
	step.Key, step.Hash, step.Status = record.StepKey, record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(record.Input) != step.Hash {
		return step, fmt.Errorf("activation input checksum changed")
	}
	if err = json.Unmarshal(record.Input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(record.Body, &step.Phase); err != nil {
		return step, err
	}
	if step.Key != ActivationKey(step.Input.Attempt) {
		return step, fmt.Errorf("activation attempt key changed")
	}
	if record.Action != "activate" || record.ExpectedInstance != step.Input.ExpectedInstance || record.CandidateResourceID == "" {
		return step, fmt.Errorf("activation frozen identity changed")
	}
	return step, nil
}
