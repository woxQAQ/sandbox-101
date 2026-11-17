package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

func (service *CaptureDiscard) captureDiscardProof(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep, admitting bool) error {
	var record SnapshotRecord
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	historical, err := LoadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(historical, source) || source.OperationID != request.FailedOperationID || source.Status != "failed" || source.State.ResumeRequired || source.State.Error == "" || source.Input.Request.TenantID != request.TenantID || source.Input.Request.SandboxID != request.SandboxID || source.Input.Request.ExpectedInstance != request.ExpectedInstance || resource.State.Witness != request.ExpectedInstance {
		return fmt.Errorf("capture discard requires immutable resumed failure")
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, resource); err != nil {
		return err
	}
	generations, err := service.proofs.GenerationHistory(ctx, tx, resource)
	if err != nil {
		return err
	}
	if err = CheckGenerationCounts(ctx, tx, resource, append(generations, resource)); err != nil {
		return err
	}
	stopped, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return err
	}
	if stopped.Hash != input.StopHash || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != resource.OperationID || stopped.Input.SourceHash != resource.Hash || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != request.SandboxID || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance {
		return fmt.Errorf("capture discard stop proof changed")
	}
	sandboxRecord, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return err
	}
	allowedState := sandboxRecord.State == "provisioning"
	if input.Lifecycle != "" {
		allowedState, err = service.ownership.VerifyCaptureDiscardParent(ctx, tx, step, sandboxRecord, admitting)
		if err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(sandboxRecord, source.Input.Sandbox) || sandboxRecord.SourceOperation != resource.OperationID || sandboxRecord.Instance != request.ExpectedInstance || !allowedState {
		return fmt.Errorf("capture discard current sandbox ownership changed")
	}
	currentMachine, current, owner, err := LoadVM(ctx, tx, resource)
	if err != nil {
		return err
	}
	expectedOwner := step.OperationID
	if admitting {
		expectedOwner = ""
	}
	ownership, err := LoadOwnership(ctx, tx, resource.Candidate)
	if err != nil {
		return err
	}
	if owner != expectedOwner || current != request.ExpectedInstance || ownership.State != "stopped" || currentMachine.Deleting || !currentMachine.Verified || currentMachine.AgentReady || !reflect.DeepEqual(currentMachine.Resource, resource.Input.Resource) {
		return fmt.Errorf("capture discard current stopped instance changed")
	}
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operation.Result
	projection, err := operation.New(tx).Projection(ctx, source.OperationID)
	if err != nil {
		return err
	}
	original, body := projection.Request, projection.Body
	hash, status, tenant, scope := projection.RequestHash, projection.Status, projection.ScopeID, projection.ScopeKind
	if err = json.Unmarshal(original, &frozen); err != nil {
		return err
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if operation.Digest(original) != hash || status != "failed" || scope != "tenant" || tenant != request.TenantID || frozen.Action != "snapshot" || frozen.Request != source.Input.Request || result.Error != source.State.Error || result.SnapshotID != source.Input.Snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandboxRecord.ID {
		return fmt.Errorf("capture discard original terminal result changed")
	}
	snapshot := source.Input.Snapshot
	if snapshot.OperationID != source.OperationID || snapshot.TenantID != request.TenantID || snapshot.SourceVM != resource.Candidate || snapshot.SourceInstance != request.ExpectedInstance || snapshot.Manifest != nil || snapshot.Dir != filepath.Join(service.store.Directory, "snapshots", snapshot.ID) {
		return fmt.Errorf("capture discard destination changed")
	}
	deleting, cleanupStatus, holderKind, holderID := 0, "blocked", "engine_dependency", source.OperationID+":capture"
	if !admitting {
		deleting = 1
		cleanupStatus = "transferred"
		holderKind = "cleanup"
		holderID = step.OperationID + ":capture_discard"
	}
	publication, err := dbgen.New(tx).GetCapture(ctx, dbgen.GetCaptureParams{
		ID:         snapshot.ID,
		SourceVmID: resource.Candidate,
	})
	if err != nil {
		return err
	}
	if err = json.Unmarshal(publication.Body, &record); err != nil {
		return err
	}
	if publication.Published != 0 || publication.Deleting != int64(deleting) || !reflect.DeepEqual(record, snapshot) {
		return fmt.Errorf("capture discard unpublished catalog changed")
	}
	body, err = dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + source.OperationID,
		OperationID:  source.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   snapshot.ID,
		Status:       cleanupStatus,
	})
	if err != nil {
		return err
	}
	if string(body) != string(operation.Encode(source.Input)) {
		return fmt.Errorf("capture discard original cleanup changed")
	}
	shape := resource.Input.Resource.Config.Shape
	if count, err := quota.New(tx).CountMatchingReservation(ctx, quota.Reservation{
		Owner: "snapshot:" + snapshot.ID,
		Bytes: (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	responsibilities := quota.New(tx)
	checks := []struct {
		count func() (int64, error)
		want  int64
	}{
		{func() (int64, error) {
			return responsibilities.CountMatchingCost(ctx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        request.TenantID,
					Kind:            "pending_storage",
					Key:             source.OperationID + ":capture",
					SourceOperation: source.OperationID,
				},
				StorageMiB: shape.DiskMiB + shape.MemoryMiB,
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountTenantCostHolders(ctx, request.TenantID, "pending_storage", source.OperationID+":capture")
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountCostReference(ctx, quota.CostReference{
				TenantID:   request.TenantID,
				Kind:       "pending_storage",
				Key:        source.OperationID + ":capture",
				HolderKind: holderKind,
				HolderID:   holderID,
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         resource.Candidate,
				HolderKind: "capture",
				HolderID:   source.OperationID,
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "image",
				ID:         resource.Input.Resource.Config.Image,
				HolderKind: "capture",
				HolderID:   source.OperationID,
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountResourceHolders(ctx, "snapshot", snapshot.ID)
		}, 0},
	}
	if !admitting {
		checks = append(checks, struct {
			count func() (int64, error)
			want  int64
		}{func() (int64, error) {
			return dbgen.New(tx).MatchingCleanupCount(ctx, dbgen.MatchingCleanupCountParams{
				ID:           "capture_discard:" + step.OperationID,
				OperationID:  step.OperationID,
				ResourceKind: "snapshot",
				ResourceID:   snapshot.ID,
				Status:       "pending",
				Body:         operation.Encode(input),
			})
		}, 1})
		var parent struct {
			Action  string
			Request CaptureDiscardRequest
		}
		projection, err = operation.New(tx).Projection(ctx, step.OperationID)
		if err != nil {
			return err
		}
		original, body = projection.Request, projection.Body
		hash, status, tenant, scope = projection.RequestHash, projection.Status, projection.ScopeID, projection.ScopeKind
		if err = json.Unmarshal(original, &parent); err != nil {
			return err
		}
		if err = json.Unmarshal(body, &result); err != nil {
			return err
		}
		if operation.Digest(original) != hash || operation.Terminal(status) || scope != "tenant" || tenant != request.TenantID || parent.Action != "capture_discard" || parent.Request != request || result.SnapshotID != snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandboxRecord.ID {
			return fmt.Errorf("capture discard parent identity changed")
		}
	}
	for _, check := range checks {
		if count, err := check.count(); err != nil || count != check.want {
			return fmt.Errorf("capture discard responsibility changed: %v", err)
		}
	}
	owners, err := dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        snapshot.ID,
		TenantID:          request.TenantID,
		SourceOperationID: source.OperationID,
	})

	if err != nil || owners != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	checkpoints, err := dbgen.New(tx).CountCheckpoint(ctx, snapshot.ID)

	if err != nil || checkpoints != 0 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	leases, err := SnapshotReaders(ctx, tx, snapshot.ID)
	if err != nil || leases != 0 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	return nil
}
