package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func (service *Captures) captureProof(ctx context.Context, tx *sql.Tx, step CaptureStep) (VMRecord, error) {
	input := step.Input
	source := input.Source
	var empty VMRecord
	if err := service.validateStep(source); err != nil {
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
	if publication.OperationID != step.OperationID || publication.SourceVM != source.Candidate || publication.SourceInstance != source.State.Witness || publication.TenantID != input.Request.TenantID || publication.Dir != filepath.Join(service.store.Directory, "snapshots", publication.ID) || publication.Manifest != nil {
		return empty, fmt.Errorf("capture destination changed")
	}
	sandboxRecord, err := service.ownership.ReadSandbox(ctx, tx, input.Request.SandboxID, input.Request.TenantID)
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
	versionBody, err := VersionHistory(ctx, tx, source.OperationID, source.Candidate, source.Key)
	if err != nil {
		return currentMachine, err
	}
	var version Version
	if err = json.Unmarshal(versionBody, &version); err != nil {
		return currentMachine, err
	}
	if !reflect.DeepEqual(version, Version{
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
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountMatchingReservation(ctx, quota.Reservation{
		Owner: "snapshot:" + publication.ID,
		Bytes: (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return currentMachine, fmt.Errorf("capture responsibility missing: %v", err)
	}
	checks := []func() (int64, error){
		func() (int64, error) {
			return responsibilities.CountStorageCost(ctx, quota.CostIdentity{
				TenantID:        publication.TenantID,
				Kind:            "vm_disk",
				Key:             source.Candidate,
				SourceOperation: source.OperationID,
			}, shape.DiskMiB)
		},
		func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         source.Candidate,
				HolderKind: "sandbox",
				HolderID:   sandboxRecord.ID,
			})
		},
		func() (int64, error) {
			return responsibilities.CountMatchingCost(ctx, quota.Cost{
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
			return responsibilities.CountCostReference(ctx, quota.CostReference{
				TenantID:   publication.TenantID,
				Kind:       "pending_storage",
				Key:        step.OperationID + ":capture",
				HolderKind: "engine_dependency",
				HolderID:   step.OperationID + ":capture",
			})
		},
		func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         source.Candidate,
				HolderKind: "capture",
				HolderID:   step.OperationID,
			})
		},
		func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
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
