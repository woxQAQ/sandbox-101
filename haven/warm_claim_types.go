package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
)

type warmClaimInput struct {
	RequestTenantID string          `json:"request_tenant_id"`
	Command         Command         `json:"command"`
	PoolID          string          `json:"pool_id"`
	Pool            poolReservation `json:"pool"`
	Source          resourceStep    `json:"source"`
	Settings        sandboxSettings `json:"settings"`
	Restore         restoreSource   `json:"restore"`
}
type (
	warmClaimState struct {
		Phase string `json:"phase"`
	}
	warmClaimStep struct {
		OperationID, Hash, Status string
		Input                     warmClaimInput
		State                     warmClaimState
	}
	warmClaimAdmission struct {
		ParentID, Role string
		Input          warmClaimInput
	}
)

func loadWarmClaim(ctx context.Context, q queryer, id string) (warmClaimStep, error) {
	step := warmClaimStep{
		OperationID: id,
	}
	var input, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='warm_claim'", id).Scan(&step.Hash, &input, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
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

func (service *Service) currentSlotBudget(ctx context.Context, q queryer, sandbox sandboxRecord, source resourceStep) (slotBudget, error) {
	if sandbox.SlotAssignment == nil {
		if source.Input.SlotAssignment != nil {
			return slotBudget{}, fmt.Errorf("%w: current slot assignment proof missing", ErrManualRecovery)
		}
		return source.Input.SlotBudget, nil
	}
	assignment := sandbox.SlotAssignment
	claim, err := loadWarmClaim(ctx, q, assignment.OperationID)
	if err != nil {
		return slotBudget{}, err
	}
	original := claim.Input.Source
	if claim.Hash != assignment.InputHash || claim.Input.Pool.SandboxID != sandbox.ID || claim.Input.Pool.Template.TenantID != sandbox.TenantID || original.OperationID != source.Input.SlotOperation || original.Hash != claim.Input.Pool.Source.InputHash || original.State.Witness == "" || original.State.Witness != claim.Input.Pool.Instance || original.Status != "succeeded" || original.Input.SlotBudget != (slotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) || (source.Input.SlotAssignment != nil && !reflect.DeepEqual(source.Input.SlotAssignment, assignment)) {
		return slotBudget{}, fmt.Errorf("%w: slot charge differs from frozen warm selection", ErrManualRecovery)
	}
	historical, err := loadStep(ctx, q, original.OperationID)
	if err != nil {
		return slotBudget{}, err
	}
	if !reflect.DeepEqual(historical, original) {
		return slotBudget{}, fmt.Errorf("%w: warm slot birth history changed", ErrManualRecovery)
	}
	return slotBudget{
		Kind:  "sandbox_slot",
		Units: 1,
	}, nil
}

func (parent *warmClaimAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	return service.admitWarmResource(ctx, tx, parent, child, kind, candidate, hash)
}
