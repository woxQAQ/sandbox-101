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

	if err := operation.InsertPhaseStep(ctx, tx, operation.PhaseIntent{
		OperationID:         operationID,
		StepKey:             "direct_retirement",
		Action:              "direct_retirement",
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Phase:               "armed",
	}); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).InsertArmedCleanup(ctx, dbgen.InsertArmedCleanupParams{
		ID:           "direct_policy:" + operationID,
		OperationID:  operationID,
		ResourceKind: "direct_policy",
		ResourceID:   source.Candidate,
		StepKey:      "direct_retirement",
		WitnessHash:  operation.Digest(payload),
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

	if err = transitionDirect(ctx, tx, old, state, "succeeded"); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).TransitionDirectCleanup(ctx, dbgen.TransitionDirectCleanupParams{
		ID:             "direct_policy:" + oldOperation,
		OperationID:    oldOperation,
		PreviousStatus: "armed",
		NextStatus:     "done",
	}))
}

func (dp *DirectPolicies) retireDirect(ctx context.Context, operationID string) error {
	step, err := LoadDirectRetirement(ctx, dp.store.Database, operationID)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		return nil
	}
	dp.storeWriteMu.Lock()
	tx, err := dp.store.Database.BeginTx(ctx, nil)
	if err == nil {
		current, lookup := LoadDirectRetirement(ctx, tx, operationID)
		err = lookup
		if err == nil && !reflect.DeepEqual(current, step) {
			err = fmt.Errorf("%w: direct retirement changed", errs.ErrConflict)
		}
		source := step.Input.Source
		if err == nil {
			err = dp.proofs.VerifyGeneration(ctx, tx, source)
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
			if err == nil && (policy.OperationID != operationID || policy.Instance != source.State.Witness || policy.PolicyHash != InstalledPolicyHash(step.Input.Policy)) {
				err = fmt.Errorf("%w: direct retirement policy ownership changed", errs.ErrConflict)
			}
		}
		if err == nil && step.State.Phase == "armed" {
			next := DirectRetirementState{
				Phase: "pending",
			}

			err = transitionDirect(ctx, tx, step, next, "running")

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
	dp.storeWriteMu.Unlock()
	if err != nil {
		return err
	}
	if err = dp.store.Checkpoint(); err != nil {
		return err
	}
	resource := step.Input.Source.Input.Resource
	if err = dp.backend.SetNetworkEnabled(ctx, resource, false); err != nil {
		return err
	}
	backend, ok := dp.backend.(loom.NetworkPolicyBackend)
	if !ok {
		return fmt.Errorf("%w: direct policy effects unavailable", errs.ErrIncompatible)
	}
	if err = backend.InstallNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	if err = backend.VerifyNetworkPolicy(ctx, resource, nil); err != nil {
		return err
	}
	observation, err := dp.backend.Inspect(ctx, resource)
	if err != nil {
		return err
	}
	if observation.Network {
		return fmt.Errorf("%w: direct retirement network-off unconfirmed", errs.ErrUnknown)
	}
	if err = dp.fault("direct_retirement_effect"); err != nil {
		return err
	}
	dp.storeWriteMu.Lock()
	defer dp.storeWriteMu.Unlock()
	tx, err = dp.store.Database.BeginTx(ctx, nil)
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
		PolicyHash:  InstalledPolicyHash(step.Input.Policy),
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
	if err = transitionDirect(ctx, tx, step, DirectRetirementState{
		Phase: "complete",
	}, "succeeded"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return dp.store.Checkpoint()
}

func retireDirectGenerations(ctx context.Context, tx *sql.Tx, generations []ResourceStep, retirement string) error {
	if len(generations) == 0 {
		return fmt.Errorf("direct retirement lacks known generations")
	}

	ids, err := dbgen.New(tx).ListLiveDirectRetirements(ctx, generations[0].Candidate)
	if err != nil {
		return err
	}
	for _, id := range ids {
		step, err := LoadDirectRetirement(ctx, tx, id)
		if err != nil {
			return err
		}
		known := false
		for _, generation := range generations {
			if generation.OperationID == step.Input.Source.OperationID && generation.Hash == step.Input.Source.Hash && generation.State.Witness == step.Input.Source.State.Witness {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("direct retirement source generation lacks reclamation proof")
		}
		if err = transitionDirect(ctx, tx, step, DirectRetirementState{
			Phase:     "reclaimed",
			RetiredBy: retirement,
		}, "succeeded"); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).CompleteLiveDirectCleanup(ctx, dbgen.CompleteLiveDirectCleanupParams{
			ID:          "direct_policy:" + id,
			OperationID: id,
		})); err != nil {
			return err
		}
	}
	return nil
}

func (proofs ResourceProofs) RetireVMPolicy(ctx context.Context, tx *sql.Tx, generations []ResourceStep, retirement string) error {
	if len(generations) == 0 {
		return fmt.Errorf("policy retirement lacks resource generations")
	}
	if err := retireDirectGenerations(ctx, tx, generations, retirement); err != nil {
		return err
	}
	vmID := generations[0].Candidate
	instance, operationID, err := policyOwner(ctx, tx, vmID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	activation, err := proofs.Activation(ctx, tx, operationID)
	if err != nil {
		return err
	}
	known := false
	for _, generation := range generations {
		if generation.Candidate == vmID && generation.Input.Resource.Instance == instance && activation.ExpectedInstance == instance && activation.ResourceOperation == generation.OperationID && activation.ResourceHash == generation.Hash {
			known = true
		}
	}
	if !known {
		return fmt.Errorf("%w: network policy retirement lacks exact historical instance", errs.ErrManualRecovery)
	}
	return store.RequireOne(dbgen.New(tx).DeleteNetworkPolicy(ctx, dbgen.DeleteNetworkPolicyParams{
		VmID:        vmID,
		Instance:    instance,
		OperationID: operationID,
	}))
}
