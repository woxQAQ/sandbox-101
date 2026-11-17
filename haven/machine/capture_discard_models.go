package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type CaptureDiscardRequest struct {
	TenantID          string `json:"tenant_id"`
	Key               string `json:"key"`
	SandboxID         string `json:"sandbox_id"`
	FailedOperationID string `json:"failed_operation_id"`
	ExpectedInstance  string `json:"expected_instance"`
	StopOperationID   string `json:"stop_operation_id"`
}

type (
	CaptureDiscardInput struct {
		Lifecycle string `json:"lifecycle,omitempty"`
		Request   CaptureDiscardRequest
		Source    CaptureStep
		StopHash  string
	}
	CaptureDiscardStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       CaptureDiscardInput
	}
)

func LoadCaptureDiscard(ctx context.Context, q store.DBTX, id string) (CaptureDiscardStep, error) {
	step := CaptureDiscardStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{OperationID: id, StepKey: "capture_discard"})
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
		return step, fmt.Errorf("capture discard input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "capture_discard" || candidate != step.Input.Source.Input.Snapshot.ID || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("capture discard frozen identity changed")
	}
	return step, nil
}
