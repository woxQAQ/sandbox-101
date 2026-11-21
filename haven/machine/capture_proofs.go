package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// CaptureCleanupTarget identifies the exact source instance and the retained
// cleanup state expected at this transaction boundary.
type CaptureCleanupTarget struct {
	VMID     string
	Instance string
	Status   string
}

// CaptureCleanupProof checks historical publication separately from the current
// cleanup destination. Publication never by itself retires raw VMM output.
func CaptureCleanupProof(ctx context.Context, tx *sql.Tx, step CaptureStep, target CaptureCleanupTarget) error {
	vm, instance, status := target.VMID, target.Instance, target.Status
	current, err := LoadCapture(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) || step.Status != "succeeded" || step.State.Phase != "complete" || step.State.ResumeRequired || !step.State.Materialized || !step.State.ArtifactsSynced || len(step.State.Files) != 4 || step.State.Boundary == nil || step.Input.Source.Candidate != vm || step.Input.Request.ExpectedInstance != instance {
		return fmt.Errorf("raw capture lacks frozen publication proof")
	}
	r := step.Input.Source.Input.Resource
	if step.State.Boundary.Path != filepath.Join(r.Dir, "capture", step.OperationID) {
		return fmt.Errorf("raw capture boundary differs from historical source")
	}
	body, err := dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "capture",
		ResourceID:   vm,
		Status:       status,
	})
	if err != nil {
		return err
	}
	if string(body) != string(operation.Encode(step.Input)) {
		return fmt.Errorf("raw capture cleanup input changed")
	}
	count, err := quota.CountResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "image",
		ID:         r.Config.Image,
		HolderKind: "capture",
		HolderID:   step.OperationID,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("raw capture image dependency missing: %v", err)
	}
	return nil
}

func ReleaseCaptures(ctx context.Context, tx *sql.Tx, vm, instance string) ([]CaptureStep, error) {
	ids, err := quota.ResourceHolderIDs(ctx, tx, "vm", vm, "capture")
	if err != nil {
		return nil, err
	}
	var captures []CaptureStep
	for _, id := range ids {
		step, err := LoadCapture(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = CaptureCleanupProof(ctx, tx, step, CaptureCleanupTarget{
			VMID:     vm,
			Instance: instance,
			Status:   "pending",
		}); err != nil {
			return nil, err
		}
		captures = append(captures, step)
	}
	return captures, nil
}

func CheckTransferredCaptures(ctx context.Context, tx *sql.Tx, step ResourceStep) error {
	seen := map[string]bool{}
	for _, capture := range step.Input.Captures {
		if seen[capture.OperationID] {
			return fmt.Errorf("duplicate capture cleanup destination")
		}
		seen[capture.OperationID] = true
		if capture.Input.Request.TenantID != step.Input.TenantID || capture.Input.Request.SandboxID != step.Input.SandboxID {
			return fmt.Errorf("capture cleanup tenant or sandbox changed")
		}
		if err := CaptureCleanupProof(ctx, tx, capture, CaptureCleanupTarget{
			VMID:     step.Candidate,
			Instance: step.ExpectedInstance,
			Status:   "transferred",
		}); err != nil {
			return err
		}
	}
	count, err := dbgen.New(tx).CleanupResourceCount(ctx, dbgen.CleanupResourceCountParams{
		ResourceKind: "capture",
		ResourceID:   step.Candidate,
		Status:       "transferred",
	})

	if err != nil || count != int64(len(seen)) {
		return fmt.Errorf("capture cleanup handoff differs from frozen release: %v", err)
	}
	return nil
}

func CaptureBoundaries(step ResourceStep) []loom.CaptureBoundary {
	var boundaries []loom.CaptureBoundary
	for _, capture := range step.Input.Captures {
		if capture.State.Boundary != nil {
			boundaries = append(boundaries, *capture.State.Boundary)
		}
	}
	return boundaries
}

func (c *Captures) captureProof(ctx context.Context, tx *sql.Tx, step CaptureStep) (VMRecord, error) {
	input := step.Input
	source := input.Source
	var empty VMRecord
	if err := c.validateStep(source); err != nil {
		return empty, err
	}
	if (source.Action != "cold_start" && source.Action != "clone") || source.Status != "succeeded" || source.State.Witness == "" || source.State.Witness != input.Request.ExpectedInstance || source.State.Witness != source.Input.Resource.Instance || source.State.Observation == nil || source.State.DiskIdentity == nil || source.Input.TenantID != input.Request.TenantID || source.Input.SandboxID != input.Request.SandboxID {
		return empty, fmt.Errorf("capture frozen source witness missing")
	}
	historical, err := LoadResourceStep(ctx, tx, source.OperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(historical, source) {
		return empty, fmt.Errorf("capture historical source changed")
	}
	publication := input.Snapshot
	if publication.OperationID != step.OperationID || publication.SourceVM != source.Candidate || publication.SourceInstance != source.State.Witness || publication.TenantID != input.Request.TenantID || publication.Dir != filepath.Join(c.store.Directory, "snapshots", publication.ID) || publication.Manifest != nil {
		return empty, fmt.Errorf("capture destination changed")
	}
	sandboxRecord, err := c.ownership.ReadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(sandboxRecord, input.Sandbox) || sandboxRecord.VMID != source.Candidate || sandboxRecord.Instance != source.State.Witness || sandboxRecord.SourceOperation != source.OperationID || (sandboxRecord.State != "provisioning" && sandboxRecord.State != "running") {
		return empty, fmt.Errorf("capture sandbox ownership changed")
	}
	var currentMachine VMRecord
	current, err := LoadOwnership(ctx, tx, source.Candidate)
	if err != nil {
		return currentMachine, err
	}
	if err = json.Unmarshal(current.Body, &currentMachine); err != nil {
		return currentMachine, err
	}
	if !current.Owned || current.Instance != source.State.Witness || current.Owner != step.OperationID || current.State != "running" || currentMachine.Deleting || !currentMachine.Verified || !currentMachine.AgentReady || !reflect.DeepEqual(currentMachine.Resource, source.Input.Resource) {
		return currentMachine, fmt.Errorf("capture current instance ownership changed")
	}
	projection, err := operation.New(tx).Projection(ctx, step.OperationID)
	if err != nil {
		return currentMachine, err
	}
	request, parent := projection.Request, projection.Body
	hash, tenant, scope, status := projection.RequestHash, projection.ScopeID, projection.ScopeKind, projection.Status
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operation.Result
	if err = json.Unmarshal(request, &frozen); err != nil {
		return currentMachine, err
	}
	if err = json.Unmarshal(parent, &result); err != nil {
		return currentMachine, err
	}
	if operation.Digest(request) != hash || scope != "tenant" || tenant != input.Request.TenantID || operation.Terminal(status) || frozen.Action != "snapshot" || frozen.Request != input.Request || result.SnapshotID != publication.ID || result.VMID != source.Candidate || result.SandboxID != sandboxRecord.ID {
		return currentMachine, fmt.Errorf("capture parent identity changed")
	}
	var record SnapshotRecord
	stored, err := dbgen.New(tx).GetCapture(ctx, dbgen.GetCaptureParams{
		ID:         publication.ID,
		SourceVmID: source.Candidate,
	})
	if err != nil {
		return currentMachine, err
	}
	if err = json.Unmarshal(stored.Body, &record); err != nil {
		return currentMachine, err
	}
	if stored.Published != 0 || stored.Deleting != 0 || !reflect.DeepEqual(record, publication) {
		return currentMachine, fmt.Errorf("capture unpublished destination changed")
	}
	versionBody, err := VersionHistory(ctx, tx, GenerationIdentity{
		VMID:        source.Candidate,
		OperationID: source.OperationID,
	}, source.Key)
	if err != nil {
		return currentMachine, err
	}
	var version RetainedVersion
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(version, RetainedVersion{
		Resource:     source.Input.Resource,
		DiskIdentity: source.State.DiskIdentity,
	}) {
		return currentMachine, fmt.Errorf("capture source version proof changed")
	}
	manifest, err := image.LoadManifest(ctx, tx, source.Input.Resource.Config.Image)
	if err != nil || !reflect.DeepEqual(manifest, source.Input.Image) {
		return currentMachine, fmt.Errorf("capture source image publication changed: %v", err)
	}
	shape := currentMachine.Resource.Config.Shape
	if count, err := quota.CountMatchingReservation(ctx, tx, quota.Reservation{
		Owner: "snapshot:" + publication.ID,
		Bytes: (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return currentMachine, fmt.Errorf("capture responsibility missing: %v", err)
	}
	checks := []func() (int64, error){
		func() (int64, error) {
			return quota.CountStorageCost(ctx, tx, quota.CostIdentity{
				TenantID:        publication.TenantID,
				Kind:            "vm_disk",
				Key:             source.Candidate,
				SourceOperation: source.OperationID,
			}, shape.DiskMiB)
		},
		func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "vm",
				ID:         source.Candidate,
				HolderKind: "sandbox",
				HolderID:   sandboxRecord.ID,
			})
		},
		func() (int64, error) {
			return quota.CountMatchingCost(ctx, tx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        publication.TenantID,
					Kind:            "pending_storage",
					Key:             step.OperationID + ":capture",
					SourceOperation: step.OperationID,
				},
				StorageMiB: shape.MemoryMiB + shape.DiskMiB,
			})
		},
		func() (int64, error) {
			return quota.CountCostReference(ctx, tx, quota.CostReference{
				TenantID:   publication.TenantID,
				Kind:       "pending_storage",
				Key:        step.OperationID + ":capture",
				HolderKind: "engine_dependency",
				HolderID:   step.OperationID + ":capture",
			})
		},
		func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "vm",
				ID:         source.Candidate,
				HolderKind: "capture",
				HolderID:   step.OperationID,
			})
		},
		func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "image",
				ID:         currentMachine.Resource.Config.Image,
				HolderKind: "capture",
				HolderID:   step.OperationID,
			})
		},
	}
	for _, check := range checks {
		if count, err := check(); err != nil || count != 1 {
			return currentMachine, fmt.Errorf("capture responsibility missing: %v", err)
		}
	}
	owners, err := dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        publication.ID,
		TenantID:          publication.TenantID,
		SourceOperationID: step.OperationID,
	})

	if err != nil || owners != 1 {
		return currentMachine, fmt.Errorf("capture responsibility missing: %v", err)
	}
	return currentMachine, nil
}
