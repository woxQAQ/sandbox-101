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
	"github.com/woxQAQ/haven/supply"
)

// A fork group owns the whole requested batch before any capture or allocation.
// Source contains either a completed snapshot or the exact generation to capture.
type ForkInput struct {
	RequestTenantID string                `json:"request_tenant_id"`
	OwnerTenantID   string                `json:"owner_tenant_id"`
	Command         operation.Command     `json:"command"`
	Template        supply.TemplateRecord `json:"template"`
	Settings        Settings              `json:"settings"`
	Targets         []ownership.Sandbox   `json:"targets"`
	SourceSandbox   *ownership.Sandbox    `json:"source_sandbox,omitempty"`
	Source          *machine.ResourceStep `json:"source,omitempty"`
	Snapshot        *machine.Source       `json:"snapshot,omitempty"`
	Checkpoint      *Checkpoint           `json:"checkpoint,omitempty"`
}

type ForkState struct {
	Phase    string          `json:"phase"`
	Snapshot *machine.Source `json:"snapshot,omitempty"`
	Children []string        `json:"children,omitempty"`
	Error    string          `json:"error,omitempty"`
}

type ForkStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       ForkInput
	State       ForkState
}

func LoadFork(ctx context.Context, query store.DBTX, id string) (ForkStep, error) {
	step := ForkStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{OperationID: id, StepKey: "fork_group"})
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
		return step, fmt.Errorf("fork group input changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "fork" || candidate != id || expected != "" || step.Input.Command.Action != "fork" || len(step.Input.Targets) < 1 || len(step.Input.Targets) > 32 || len(step.Input.Targets) != step.Input.Command.Count || (step.Input.Source == nil) == (step.Input.Snapshot == nil) {
		return step, fmt.Errorf("fork group source or batch identity changed")
	}
	ids := map[string]bool{}
	for _, target := range step.Input.Targets {
		if target.TenantID != step.Input.OwnerTenantID || target.OperationID != id || target.State != "provisioning" || target.VMID != "" || target.SourceOperation != "" || target.Identity.TemplateID != step.Input.Template.ID || target.Identity.Shape != step.Input.Template.Config.Shape || target.Identity.Epoch == "" || ids[target.ID] {
			return step, fmt.Errorf("fork placement identity changed")
		}
		ids[target.ID] = true
	}
	if len(step.State.Children) != 0 && len(step.State.Children) != len(step.Input.Targets) {
		return step, fmt.Errorf("fork child batch is incomplete")
	}
	switch step.State.Phase {
	case "admitted", "captured", "allocated", "compensating", "complete", "failed":
	default:
		return step, fmt.Errorf("fork group phase is unknown")
	}
	return step, nil
}
