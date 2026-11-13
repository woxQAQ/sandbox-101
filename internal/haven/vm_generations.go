package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
)

// generationHistory verifies every retained version against immutable step history.
// The mutable VM row identifies the current candidate; it never fills a witness.
func expectedCurrentInstance(step resourceStep, current string) bool {
	if current == step.Input.Resource.Instance {
		return true
	}
	if step.Action != "restore" || current != step.ExpectedInstance {
		return false
	}
	switch step.State.Phase {
	case "admitted", "restore_prepare_intent", "restore_candidate_prepared", "restore_candidate_copied", "restore_candidate_renamed", "restore_candidate_synced", "restore_materials_complete", "restore_resources_ready":
		return true
	default:
		return false
	}
}

func (service *Service) generationHistory(ctx context.Context, tx *sql.Tx, step resourceStep) ([]resourceStep, error) {
	return service.generationHistoryAt(ctx, tx, step, "active_use", "sandbox:"+step.Input.SandboxID)
}

func (service *Service) generationHistoryAt(ctx context.Context, tx *sql.Tx, step resourceStep, holderKind, holderID string) ([]resourceStep, error) {
	seen := map[string]bool{step.OperationID: true}
	var history []resourceStep
	for index, reference := range step.Input.Ancestors {
		if seen[reference.OperationID] {
			return nil, fmt.Errorf("cyclic or duplicate generation history")
		}
		seen[reference.OperationID] = true
		prior, err := loadStep(ctx, tx, reference.OperationID)
		if err != nil {
			return nil, err
		}
		if !slices.Equal(prior.Input.Ancestors, step.Input.Ancestors[:index]) {
			return nil, fmt.Errorf("generation history prefix changed")
		}
		if prior.Hash != reference.InputHash || prior.Status != "succeeded" || prior.State.Phase != "complete" || prior.State.Witness == "" || prior.State.Witness != prior.Input.Resource.Instance || prior.Candidate != step.Candidate || prior.Input.SandboxID != step.Input.SandboxID || prior.Input.TenantID != step.Input.TenantID || prior.Input.Resource.Config.Image != step.Input.Resource.Config.Image || !reflect.DeepEqual(prior.Input.Resource.Config.Shape, step.Input.Resource.Config.Shape) || prior.Input.Resource.NetworkToken != step.Input.Resource.NetworkToken || prior.Input.Resource.Slot != step.Input.Resource.Slot || prior.Input.RuntimeBytes != step.Input.RuntimeBytes || prior.Input.SlotOperation != step.Input.SlotOperation {
			return nil, fmt.Errorf("retained generation lineage changed")
		}
		if err = service.verifyGenerationAt(ctx, tx, prior, holderKind, holderID); err != nil {
			return nil, err
		}
		history = append(history, prior)
	}
	return history, nil
}

func (service *Service) verifyGeneration(ctx context.Context, tx *sql.Tx, source resourceStep) error {
	return service.verifyGenerationAt(ctx, tx, source, "active_use", "sandbox:"+source.Input.SandboxID)
}

func (service *Service) verifyGenerationAt(ctx context.Context, tx *sql.Tx, source resourceStep, holderKind, holderID string) error {
	if err := service.validateStep(source); err != nil {
		return err
	}
	if source.Status != "succeeded" || source.State.Witness != source.Input.Resource.Instance || source.State.Observation == nil || source.State.DiskIdentity == nil {
		return fmt.Errorf("completed generation lacks historical witness")
	}
	var body []byte
	if err := tx.QueryRowContext(ctx, "SELECT body FROM resource_versions WHERE id=? AND vm_id=? AND operation_id=? AND step_key=?", source.OperationID, source.Candidate, source.OperationID, source.Key).Scan(&body); err != nil {
		return err
	}
	var version resourceVersion
	if err := json.Unmarshal(body, &version); err != nil {
		return err
	}
	if !reflect.DeepEqual(version, resourceVersion{Resource: source.Input.Resource, DiskIdentity: source.State.DiskIdentity}) {
		return fmt.Errorf("retained version identity changed")
	}
	if err := tx.QueryRowContext(ctx, "SELECT body FROM instances WHERE id=? AND vm_id=? AND operation_id=?", source.State.Witness, source.Candidate, source.OperationID).Scan(&body); err != nil {
		return err
	}
	var instance vmRecord
	if err := json.Unmarshal(body, &instance); err != nil {
		return err
	}
	if !instance.Verified || !instance.AgentReady || !reflect.DeepEqual(instance.Resource, source.Input.Resource) || !reflect.DeepEqual(instance.Observation, source.State.Observation) {
		return fmt.Errorf("retained instance birth proof changed")
	}
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM quota_resources q JOIN quota_refs r USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.resource_key=? AND q.storage_mib=? AND q.source_operation_id=? AND r.holder_kind=? AND r.holder_id=?", source.Input.TenantID, source.Input.DiskKey, source.Input.Resource.Config.Shape.DiskMiB, source.OperationID, holderKind, holderID).Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("retained generation disk responsibility missing: %v", err)
	}
	if source.Input.Restore != nil {
		return service.restoreRelations(ctx, tx, source)
	}
	return nil
}
