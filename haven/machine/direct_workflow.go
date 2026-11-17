package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func ArmDirectRetirement(ctx context.Context, tx *sql.Tx, operationID string, input DirectRetirementInput) error {
	if input.Policy == nil {
		return nil
	}
	source := input.Source
	payload := operation.Encode(input)
	if existing, err := LoadDirectRetirement(ctx, tx, operationID); err == nil {
		if existing.Hash != operation.Digest(payload) || existing.State.Phase != "armed" || operation.Terminal(existing.Status) {
			return fmt.Errorf("direct retirement original arm changed")
		}
		return nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if err := store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         operationID,
		StepKey:             "direct_retirement",
		Action:              "direct_retirement",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Body: operation.Encode(DirectRetirementState{
			Phase: "armed",
		}),
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).InsertArmedCleanup(ctx, dbgen.InsertArmedCleanupParams{
		ID:           "direct_policy:" + operationID,
		OperationID:  operationID,
		ResourceKind: "direct_policy",
		ResourceID:   source.Candidate,
		Body:         payload,
	}))
}

// DirectPolicyReplacement ties retirement of the prior activation to its exact
// instance and the activation taking responsibility in the same transaction.
type DirectPolicyReplacement struct {
	PriorOperationID string
	OperationID      string
	VMID             string
	Instance         string
}

func CompleteDirectReplacement(ctx context.Context, tx *sql.Tx, replacement DirectPolicyReplacement) error {
	oldOperation, newOperation := replacement.PriorOperationID, replacement.OperationID
	vm, instance := replacement.VMID, replacement.Instance
	old, err := LoadDirectRetirement(ctx, tx, oldOperation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if operation.Terminal(old.Status) {
		return nil
	}
	if old.Input.Source.Candidate != vm || old.Input.Source.State.Witness != instance || old.State.Phase != "armed" {
		return fmt.Errorf("direct policy replacement has unresolved prior retirement")
	}
	state := DirectRetirementState{
		Phase:     "replaced",
		RetiredBy: newOperation,
	}

	if err = store.RequireOne(dbgen.New(tx).CompletePendingStep(ctx, dbgen.CompletePendingStepParams{
		OperationID:  old.OperationID,
		StepKey:      "direct_retirement",
		InputHash:    old.Hash,
		PreviousBody: operation.Encode(old.State),
		NextBody:     operation.Encode(state),
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).TransitionDirectCleanup(ctx, dbgen.TransitionDirectCleanupParams{
		ID:             "direct_policy:" + oldOperation,
		OperationID:    oldOperation,
		PreviousStatus: "armed",
		NextStatus:     "done",
	}))
}

func (service *DirectPolicies) retireDirect(ctx context.Context, operationID string) error {
	step, err := LoadDirectRetirement(ctx, service.store.Database, operationID)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		return nil
	}
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err == nil {
		current, lookup := LoadDirectRetirement(ctx, tx, operationID)
		err = lookup
		if err == nil && !reflect.DeepEqual(current, step) {
			err = fmt.Errorf("%w: direct retirement changed", errs.ErrConflict)
		}
		source := step.Input.Source
		if err == nil {
			err = service.proofs.VerifyGeneration(ctx, tx, source)
		}
		if err == nil {
			currentMachine, instance, owner, lookup := LoadVM(ctx, tx, source)
			err = lookup
			if err == nil && (instance != source.State.Witness || owner != "" || !currentMachine.Verified) {
				err = fmt.Errorf("%w: direct retirement current instance is unverified or busy", errs.ErrConflict)
			}
		}
		if err == nil {
			var policy NetworkPolicyRecord
			policy, err = LoadPolicyWitness(ctx, tx, source.Candidate)
			if err == nil && (policy.OperationID != operationID || policy.Instance != source.State.Witness || string(policy.Body) != string(operation.Encode(step.Input.Policy))) {
				err = fmt.Errorf("%w: direct retirement policy ownership changed", errs.ErrConflict)
			}
		}
		if err == nil && step.State.Phase == "armed" {
			next := DirectRetirementState{
				Phase: "pending",
			}

			err = store.RequireOne(dbgen.New(tx).BeginPendingStep(ctx, dbgen.BeginPendingStepParams{
				OperationID:  operationID,
				StepKey:      "direct_retirement",
				InputHash:    step.Hash,
				PreviousBody: operation.Encode(step.State),
				NextBody:     operation.Encode(next),
			}))

			if err == nil {
				err = store.RequireOne(dbgen.New(tx).TransitionDirectCleanup(ctx, dbgen.TransitionDirectCleanupParams{
					ID:             "direct_policy:" + operationID,
					OperationID:    operationID,
					PreviousStatus: "armed",
					NextStatus:     "pending",
				}))
			}
			if err == nil {
				step.State = next
				step.Status = "running"
			}
		}
		if err == nil {
			err = tx.Commit()
		} else {
			err = errors.Join(err, tx.Rollback())
		}
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	if err = service.store.Checkpoint(); err != nil {
		return err
	}
	resource := step.Input.Source.Input.Resource
	if err = service.backend.Network(ctx, resource, false); err != nil {
		return err
	}
	backend, ok := service.backend.(loom.NetworkPolicyBackend)
	if !ok {
		return fmt.Errorf("%w: direct policy effects unavailable", errs.ErrIncompatible)
	}
	if err = backend.InstallNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	if err = backend.VerifyNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	observation, err := service.backend.Inspect(ctx, resource)
	if err != nil {
		return err
	}
	if observation.Network {
		return fmt.Errorf("%w: direct retirement network-off unconfirmed", errs.ErrUnknown)
	}
	if err = service.fault("direct_retirement_effect"); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err = service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadDirectRetirement(ctx, tx, operationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("direct retirement completion changed")
	}
	if err = deletePolicyWitness(ctx, tx, NetworkPolicyRecord{
		VMID:        resource.VMID,
		Instance:    resource.Instance,
		OperationID: operationID,
		Body:        operation.Encode(step.Input.Policy),
	}); err != nil {
		return err
	}

	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(ctx, dbgen.CompletePendingCleanupKindParams{
		ID:           "direct_policy:" + operationID,
		OperationID:  operationID,
		ResourceKind: "direct_policy",
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(ctx, dbgen.CompleteRunningStepParams{
		OperationID:  operationID,
		StepKey:      "direct_retirement",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.State),
		NewBody: operation.Encode(DirectRetirementState{
			Phase: "complete",
		}),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
