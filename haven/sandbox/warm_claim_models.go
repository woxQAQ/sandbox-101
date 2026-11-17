package sandbox

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

type WarmClaimInput struct {
	RequestTenantID string                 `json:"request_tenant_id"`
	Command         operation.Command      `json:"command"`
	PoolID          string                 `json:"pool_id"`
	Pool            supply.PoolReservation `json:"pool"`
	Source          machine.ResourceStep   `json:"source"`
	Settings        Settings               `json:"settings"`
	Restore         machine.SnapshotSource `json:"restore"`
}

type WarmClaimState struct {
	Phase string `json:"phase"`
}

type WarmClaimStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       WarmClaimInput
	State       WarmClaimState
}

type WarmClaimAdmission struct {
	ParentID string
	Role     string
	Input    WarmClaimInput
}

func LoadWarmClaim(ctx context.Context, query store.DBTX, id string) (WarmClaimStep, error) {
	step := WarmClaimStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "warm_claim",
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
		return step, fmt.Errorf("warm claim checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	switch step.State.Phase {
	case "selected", "warm_failed", "warm_retired", "complete", "failed", "superseded":
	default:
		return step, fmt.Errorf("warm claim phase is unsupported")
	}
	if action != "claim" || candidate != step.Input.Pool.SandboxID || expected != step.Input.Pool.Instance || step.Input.Pool.Source.OperationID != step.Input.Source.OperationID || step.Input.Pool.Source.InputHash != step.Input.Source.Hash {
		return step, fmt.Errorf("warm claim frozen resource identity changed")
	}
	return step, nil
}

func CurrentSlotBudget(ctx context.Context, query store.DBTX, record ownership.Sandbox, source machine.ResourceStep) (quota.SlotBudget, error) {
	if record.SlotAssignment == nil {
		if source.Input.SlotAssignment != nil {
			return quota.SlotBudget{}, fmt.Errorf("%w: current slot assignment proof missing", errs.ErrManualRecovery)
		}
		return source.Input.SlotBudget, nil
	}
	assignment := record.SlotAssignment
	claim, err := LoadWarmClaim(ctx, query, assignment.OperationID)
	if err != nil {
		return quota.SlotBudget{}, err
	}
	original := claim.Input.Source
	if claim.Hash != assignment.InputHash || claim.Input.Pool.SandboxID != record.ID || claim.Input.Pool.Template.TenantID != record.TenantID || original.OperationID != source.Input.SlotOperation || original.Hash != claim.Input.Pool.Source.InputHash || original.State.Witness == "" || original.State.Witness != claim.Input.Pool.Instance || original.Status != "succeeded" || original.Input.SlotBudget != (quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) || (source.Input.SlotAssignment != nil && !reflect.DeepEqual(source.Input.SlotAssignment, assignment)) {
		return quota.SlotBudget{}, fmt.Errorf("%w: slot charge differs from frozen warm selection", errs.ErrManualRecovery)
	}
	historical, err := machine.LoadResourceStep(ctx, query, original.OperationID)
	if err != nil {
		return quota.SlotBudget{}, err
	}
	if !reflect.DeepEqual(historical, original) {
		return quota.SlotBudget{}, fmt.Errorf("%w: warm slot birth history changed", errs.ErrManualRecovery)
	}
	return quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 1,
	}, nil
}
