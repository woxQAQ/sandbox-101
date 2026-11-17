package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type (
	CaptureCleanupRequest struct {
		TenantID           string `json:"tenant_id"`
		Key                string `json:"key"`
		CaptureOperationID string `json:"capture_operation_id"`
		StopOperationID    string `json:"stop_operation_id"`
	}
	captureCleanupInput struct {
		Request CaptureCleanupRequest
		Capture CaptureStep
		Stop    StopStep
	}
	CaptureCleanupStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       captureCleanupInput
	}
)

func LoadCaptureCleanup(ctx context.Context, q store.DBTX, id string) (CaptureCleanupStep, error) {
	step := CaptureCleanupStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "capture_cleanup",
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
		return step, fmt.Errorf("raw capture cleanup checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "capture_cleanup" || candidate != step.Input.Capture.Input.Source.Candidate || expected != step.Input.Capture.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("raw capture cleanup identity changed")
	}
	return step, nil
}
