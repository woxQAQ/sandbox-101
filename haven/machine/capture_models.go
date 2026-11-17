package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

type CaptureRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type CaptureInput struct {
	Request      CaptureRequest    `json:"request"`
	Source       ResourceStep      `json:"source"`
	Sandbox      ownership.Sandbox `json:"sandbox"`
	Snapshot     SnapshotRecord    `json:"snapshot"`
	Backend      string            `json:"backend"`
	Architecture string            `json:"architecture"`
	CPU          string            `json:"cpu"`
}

type CaptureState struct {
	Phase           string                `json:"phase"`
	Boundary        *loom.CaptureBoundary `json:"boundary,omitempty"`
	ResumeRequired  bool                  `json:"resume_required"`
	Captured        bool                  `json:"captured"`
	Materialized    bool                  `json:"materialized"`
	Files           map[string]string     `json:"files,omitempty"`
	ArtifactsSynced bool                  `json:"artifacts_synced"`
	Error           string                `json:"error,omitempty"`
}

type CaptureStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       CaptureInput
	State       CaptureState
}

func LoadCapture(ctx context.Context, query store.DBTX, operationID string) (CaptureStep, error) {
	step := CaptureStep{
		OperationID: operationID,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: operationID,
		StepKey:     "capture",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	action, candidate, instance := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("capture input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "snapshot" || candidate != step.Input.Snapshot.ID || instance != step.Input.Request.ExpectedInstance || !loom.ValidResourceID(candidate, "snap-") {
		return step, fmt.Errorf("capture identity changed")
	}
	return step, nil
}
