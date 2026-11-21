package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
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
	return proofs.GenerationHistoryAt(ctx, tx, step, quota.Holder{
		Kind: "active_use",
		ID:   "sandbox:" + step.Input.SandboxID,
	})
}

func (proofs ResourceProofs) GenerationHistoryAt(ctx context.Context, tx *sql.Tx, step ResourceStep, holder quota.Holder) ([]ResourceStep, error) {
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
		if err = proofs.VerifyGenerationAt(ctx, tx, prior, holder); err != nil {
			return nil, err
		}
		history = append(history, prior)
	}
	return history, nil
}

func (proofs ResourceProofs) VerifyGeneration(ctx context.Context, tx *sql.Tx, source ResourceStep) error {
	return proofs.VerifyGenerationAt(ctx, tx, source, quota.Holder{
		Kind: "active_use",
		ID:   "sandbox:" + source.Input.SandboxID,
	})
}

func (proofs ResourceProofs) VerifyGenerationAt(ctx context.Context, tx *sql.Tx, source ResourceStep, holder quota.Holder) error {
	if err := proofs.snapshots.ValidateResource(source); err != nil {
		return err
	}
	if source.Status != "succeeded" || source.State.Witness != source.Input.Resource.Instance || source.State.Observation == nil || source.State.DiskIdentity == nil {
		return fmt.Errorf("completed generation lacks historical witness")
	}

	body, err := VersionHistory(ctx, tx, GenerationIdentity{
		VMID:        source.Candidate,
		OperationID: source.OperationID,
	}, source.Key)
	if err != nil {
		return err
	}
	var version RetainedVersion
	if err := json.Unmarshal(body, &version); err != nil {
		return err
	}
	if !reflect.DeepEqual(version, RetainedVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return fmt.Errorf("retained version identity changed")
	}
	if body, err = InstanceHistory(ctx, tx, GenerationIdentity{
		VMID:        source.Candidate,
		OperationID: source.OperationID,
	}, source.State.Witness); err != nil {
		return err
	}
	var instance VMRecord
	if err := json.Unmarshal(body, &instance); err != nil {
		return err
	}
	if !instance.Verified || !instance.AgentReady || !reflect.DeepEqual(instance.Resource, source.Input.Resource) || !reflect.DeepEqual(instance.Observation, source.State.Observation) {
		return fmt.Errorf("retained instance birth proof changed")
	}
	count, err := quota.CountStorageCostReference(ctx, tx, quota.CostIdentity{
		TenantID:        source.Input.TenantID,
		Kind:            "vm_disk",
		Key:             source.Input.DiskKey,
		SourceOperation: source.OperationID,
	}, source.Input.Resource.Config.Shape.DiskMiB, holder)
	if err != nil || count != 1 {
		return fmt.Errorf("retained generation disk responsibility missing: %v", err)
	}
	if source.Input.Restore != nil {
		return proofs.snapshots.RestoreRelations(ctx, tx, source)
	}
	return nil
}

func (proofs ResourceProofs) CleanupGenerations(ctx context.Context, tx *sql.Tx, source ResourceStep, holder quota.Holder) ([]ResourceStep, error) {
	history, err := proofs.GenerationHistoryAt(ctx, tx, source, holder)
	if err != nil {
		return nil, err
	}
	return append(history, source), nil
}

func ValidateResourceStep(stateDirectory string, step ResourceStep) error {
	r := step.Input.Resource
	if err := host.CheckConfig(&r.Config); err != nil {
		return err
	}
	source := step.OperationID
	if step.Action == "release" {
		source = step.Input.SourceOperation
	}
	if (step.Action != "cold_start" && step.Action != "clone" && step.Action != "restore" && step.Action != "release") || !loom.ValidResourceID(source, "op-") || !loom.ValidResourceID(r.VMID, "vm-") || !loom.ValidResourceID(r.Instance, "run-") || !loom.ValidResourceID(r.NetworkToken, "net-") || r.Slot < 1 || r.Slot >= 32768 {
		return fmt.Errorf("unsupported or incomplete resource step identity")
	}
	if ((step.Action == "cold_start" || step.Action == "clone") && step.ExpectedInstance != "") || (step.Action == "release" && step.ExpectedInstance != r.Instance) || (!operation.Terminal(step.Status) && step.State.Witness != "") {
		return fmt.Errorf("step instance condition or historical witness is inconsistent")
	}
	if step.Action == "restore" && (step.ExpectedInstance == "" || step.ExpectedInstance == r.Instance || len(step.Input.Ancestors) == 0 || !loom.ValidResourceID(step.Input.StopOperation, "op-")) {
		return fmt.Errorf("restore predecessor/stop proof missing")
	}
	if (step.Input.SlotBudget.Kind != "sandbox_slot" && step.Input.SlotBudget.Kind != "build_slot") || step.Input.SlotBudget.Units < 0 || step.Input.SlotBudget.Units > 1 {
		return fmt.Errorf("resource slot budget is incomplete")
	}
	if step.Input.DiskKey == "" || !loom.ValidResourceID(step.Input.SlotOperation, "op-") || step.Input.RuntimeBytes <= 0 {
		return fmt.Errorf("resource responsibility identity missing")
	}
	if step.Action == "cold_start" {
		unprepared := step.State.Phase == "admitted" || step.State.Phase == "prepare_intent"
		if unprepared != (step.State.DiskIdentity == nil) {
			return fmt.Errorf("cold disk completion proof missing or out of phase")
		}
	}
	if step.Action == "release" && step.Input.DiskIdentity == nil {
		return fmt.Errorf("release disk witness missing")
	}
	directory := filepath.Join(stateDirectory, "vms", r.VMID)
	disk := filepath.Join(directory, "disks", source+".raw")
	restoreDir := ""
	if step.Input.Restore != nil {
		if step.Action == "cold_start" {
			return fmt.Errorf("cold step contains restore source")
		}
		disk = filepath.Join(directory, "versions", source, "disk.raw")
		restoreDir = filepath.Join(directory, "versions", source, "restore")
	} else if step.Action == "clone" || step.Action == "restore" {
		return fmt.Errorf("clone source proof missing")
	}
	if r.Dir != directory || r.Disk != disk || r.RunDir != filepath.Join(directory, "runs", r.Instance) || r.RestoreDir != restoreDir || r.ImageDir != filepath.Join(stateDirectory, "images", r.Config.Image[7:]) || step.Input.Image.Digest != r.Config.Image {
		return fmt.Errorf("resource step paths or image proof mismatch")
	}
	return nil
}

type InstanceInspector interface {
	// Inspect observes the identified instance and reports uncertainty as an error.
	Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error)
}

func InspectStoppedGenerations(ctx context.Context, inspector InstanceInspector, generations []ResourceStep) error {
	for _, generation := range generations {
		observation, err := inspector.Inspect(ctx, generation.Input.Resource)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return fmt.Errorf("%w: known instance host removal unconfirmed", errs.ErrUnknown)
		}
	}
	return nil
}

// LoadReadyInstance verifies the provision witness and current VM ownership for data admission.
func LoadReadyInstance(ctx context.Context, db store.DBTX, sourceOperation, instance string) (ResourceStep, error) {
	source, err := LoadResourceStep(ctx, db, sourceOperation)
	if err != nil {
		return source, err
	}
	vm, current, owner, err := LoadVM(ctx, db, source)
	if err != nil {
		return source, err
	}
	if instance == "" || source.Status != "succeeded" || source.State.Witness != instance || current != instance || owner != "" || !vm.Verified || !vm.AgentReady || vm.Deleting {
		return source, fmt.Errorf("%w: sandbox instance is not verified ready", errs.ErrConflict)
	}
	return source, nil
}

func LoadVM(ctx context.Context, database store.DBTX, step ResourceStep) (VMRecord, string, string, error) {
	current, err := LoadOwnership(ctx, database, step.Candidate)
	var record VMRecord
	if err != nil {
		return record, "", "", err
	}
	if err = json.Unmarshal(current.Body, &record); err != nil {
		return record, "", "", err
	}
	if !reflect.DeepEqual(record.Resource, step.Input.Resource) {
		return record, "", "", fmt.Errorf("VM candidate resource changed")
	}
	return record, current.Instance, current.Owner, nil
}

func ExpectedCurrentInstance(step ResourceStep, current string) bool {
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

func CopyRestoreMaterials(proof *loom.RestoreMaterials) *loom.RestoreMaterials {
	if proof == nil {
		return nil
	}
	var copy loom.RestoreMaterials
	if err := json.Unmarshal(operation.Encode(proof), &copy); err != nil {
		panic(err)
	}
	return &copy
}
