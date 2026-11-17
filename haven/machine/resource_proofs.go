package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/quota"
)

// ResourceProofs verifies retained generations against their immutable birth
// records. Current VM ownership never supplies a missing historical witness.
type ResourceProofs struct {
	snapshots   SnapshotProofs
	activations ActivationProofReader
}

func NewResourceProofs(snapshots SnapshotProofs, activations ActivationProofReader) ResourceProofs {
	return ResourceProofs{
		snapshots:   snapshots,
		activations: activations,
	}
}

// Snapshots returns the same read-only history verifier used for generation proofs.
func (proofs ResourceProofs) Snapshots() SnapshotProofs {
	return proofs.snapshots
}

func (proofs ResourceProofs) GenerationHistory(ctx context.Context, tx *sql.Tx, step ResourceStep) ([]ResourceStep, error) {
	return proofs.GenerationHistoryAt(ctx, tx, step, "active_use", "sandbox:"+step.Input.SandboxID)
}

func (proofs ResourceProofs) GenerationHistoryAt(ctx context.Context, tx *sql.Tx, step ResourceStep, holderKind, holderID string) ([]ResourceStep, error) {
	seen := map[string]bool{
		step.OperationID: true,
	}
	var history []ResourceStep
	for index, reference := range step.Input.Ancestors {
		if seen[reference.OperationID] {
			return nil, fmt.Errorf("cyclic or duplicate generation history")
		}
		seen[reference.OperationID] = true
		prior, err := LoadResourceStep(ctx, tx, reference.OperationID)
		if err != nil {
			return nil, err
		}
		if !slices.Equal(prior.Input.Ancestors, step.Input.Ancestors[:index]) {
			return nil, fmt.Errorf("generation history prefix changed")
		}
		if prior.Hash != reference.InputHash || prior.Status != "succeeded" || prior.State.Phase != "complete" || prior.State.Witness == "" || prior.State.Witness != prior.Input.Resource.Instance || prior.Candidate != step.Candidate || prior.Input.SandboxID != step.Input.SandboxID || prior.Input.TenantID != step.Input.TenantID || prior.Input.Resource.Config.Image != step.Input.Resource.Config.Image || !reflect.DeepEqual(prior.Input.Resource.Config.Shape, step.Input.Resource.Config.Shape) || prior.Input.Resource.NetworkToken != step.Input.Resource.NetworkToken || prior.Input.Resource.Slot != step.Input.Resource.Slot || prior.Input.RuntimeBytes != step.Input.RuntimeBytes || prior.Input.SlotOperation != step.Input.SlotOperation {
			return nil, fmt.Errorf("retained generation lineage changed")
		}
		if err = proofs.VerifyGenerationAt(ctx, tx, prior, holderKind, holderID); err != nil {
			return nil, err
		}
		history = append(history, prior)
	}
	return history, nil
}

func (proofs ResourceProofs) VerifyGeneration(ctx context.Context, tx *sql.Tx, source ResourceStep) error {
	return proofs.VerifyGenerationAt(ctx, tx, source, "active_use", "sandbox:"+source.Input.SandboxID)
}

func (proofs ResourceProofs) VerifyGenerationAt(ctx context.Context, tx *sql.Tx, source ResourceStep, holderKind, holderID string) error {
	if err := proofs.snapshots.ValidateResource(source); err != nil {
		return err
	}
	if source.Status != "succeeded" || source.State.Witness != source.Input.Resource.Instance || source.State.Observation == nil || source.State.DiskIdentity == nil {
		return fmt.Errorf("completed generation lacks historical witness")
	}

	body, err := VersionHistory(ctx, tx, source.OperationID, source.Candidate, source.Key)
	if err != nil {
		return err
	}
	var version Version
	if err := json.Unmarshal(body, &version); err != nil {
		return err
	}
	if !reflect.DeepEqual(version, Version{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return fmt.Errorf("retained version identity changed")
	}
	if body, err = InstanceHistory(ctx, tx, source.State.Witness, source.Candidate, source.OperationID); err != nil {
		return err
	}
	var instance VMRecord
	if err := json.Unmarshal(body, &instance); err != nil {
		return err
	}
	if !instance.Verified || !instance.AgentReady || !reflect.DeepEqual(instance.Resource, source.Input.Resource) || !reflect.DeepEqual(instance.Observation, source.State.Observation) {
		return fmt.Errorf("retained instance birth proof changed")
	}
	count, err := quota.New(tx).CountStorageCostReference(ctx, quota.CostIdentity{
		TenantID:        source.Input.TenantID,
		Kind:            "vm_disk",
		Key:             source.Input.DiskKey,
		SourceOperation: source.OperationID,
	}, source.Input.Resource.Config.Shape.DiskMiB, holderKind, holderID)
	if err != nil || count != 1 {
		return fmt.Errorf("retained generation disk responsibility missing: %v", err)
	}
	if source.Input.Restore != nil {
		return proofs.snapshots.RestoreRelations(ctx, tx, source)
	}
	return nil
}

func (proofs ResourceProofs) CleanupGenerations(ctx context.Context, tx *sql.Tx, source ResourceStep, holderKind, holderID string) ([]ResourceStep, error) {
	history, err := proofs.GenerationHistoryAt(ctx, tx, source, holderKind, holderID)
	if err != nil {
		return nil, err
	}
	return append(history, source), nil
}
