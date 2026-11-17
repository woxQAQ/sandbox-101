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
	"github.com/woxQAQ/haven/store"
)

type SnapshotDeleteRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}
type (
	snapshotDeleteInput struct {
		Request SnapshotDeleteRequest
		Source  SnapshotSource
	}
	SnapshotDeleteStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       snapshotDeleteInput
	}
)

func LoadSnapshotDelete(ctx context.Context, q store.DBTX, id string) (SnapshotDeleteStep, error) {
	step := SnapshotDeleteStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "snapshot_delete",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("snapshot delete input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "snapshot_delete" || expected != "" || candidate != step.Input.Request.SnapshotID || candidate != step.Input.Source.Snapshot.ID {
		return step, fmt.Errorf("snapshot delete frozen identity changed")
	}
	return step, nil
}

func (service *SnapshotDeletion) snapshotDeleteProof(ctx context.Context, tx *sql.Tx, step SnapshotDeleteStep, admitting bool) ([]ResourceStep, error) {
	var record SnapshotRecord
	var checkpoint SnapshotRecord
	source := step.Input.Source
	snapshot := source.Snapshot
	request := step.Input.Request
	if request.TenantID != snapshot.TenantID || request.SnapshotID != snapshot.ID {
		return nil, fmt.Errorf("snapshot delete owner changed")
	}
	capture, err := LoadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return nil, err
	}
	if err = service.proofs.ValidateSnapshotPublication(source, request.TenantID, capture); err != nil {
		return nil, err
	}
	deleting, holderKind, holderID, cleanupStatus := 1, "cleanup", step.OperationID+":snapshot_delete", "pending"
	if admitting {
		deleting = 0
		holderKind = "active_use"
		holderID = "checkpoint:" + snapshot.ID
	}
	var body []byte
	if body, err = dbgen.New(tx).GetPublishedBody(ctx, dbgen.GetPublishedBodyParams{
		ID:         snapshot.ID,
		SourceVmID: snapshot.SourceVM,
		Deleting:   int64(deleting),
	}); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(record, snapshot) {
		return nil, fmt.Errorf("snapshot delete catalog changed")
	}
	if body, err = dbgen.New(tx).GetCheckpointBody(ctx, dbgen.GetCheckpointBodyParams{
		ID:     snapshot.ID,
		Tenant: snapshot.TenantID,
	}); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &checkpoint); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(checkpoint, snapshot) {
		return nil, fmt.Errorf("snapshot delete checkpoint changed")
	}
	operations := operation.New(tx)
	if body, err = dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + capture.OperationID,
		OperationID:  capture.OperationID,
		ResourceKind: "capture",
		ResourceID:   snapshot.SourceVM,
		Status:       "done",
	}); err != nil {
		return nil, fmt.Errorf("snapshot raw capture cleanup not retired: %w", err)
	}
	if string(body) != string(operation.Encode(capture.Input)) {
		return nil, fmt.Errorf("snapshot raw cleanup history changed")
	}
	backings, err := service.proofs.Backings(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	physicalHolderKind, physicalHolderID := "cleanup", step.OperationID
	if admitting {
		physicalHolderKind = "checkpoint"
		physicalHolderID = snapshot.ID
	}
	if count, err := quota.New(tx).CountMatchingReservation(ctx, quota.Reservation{
		Owner: "snapshot:" + snapshot.ID,
		Bytes: (2*snapshot.Manifest.Config.Shape.MemoryMiB + snapshot.Manifest.Config.Shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if !admitting {
		var original struct {
			Action  string
			Request SnapshotDeleteRequest
		}
		var result operation.Result
		parent, err := operations.Projection(ctx, step.OperationID)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(parent.Request, &original); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(parent.Body, &result); err != nil {
			return nil, err
		}
		if operation.Digest(parent.Request) != parent.RequestHash || operation.Terminal(parent.Status) || parent.ScopeKind != "tenant" || parent.ScopeID != request.TenantID || original.Action != "snapshot_delete" || original.Request != request || result.SnapshotID != snapshot.ID {
			return nil, fmt.Errorf("snapshot delete parent identity changed")
		}
	}
	responsibilities := quota.New(tx)
	if count, err := responsibilities.CountCost(ctx, "snapshot", snapshot.ID); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountMatchingCost(ctx, quota.Cost{
		CostIdentity: quota.CostIdentity{
			TenantID:        snapshot.TenantID,
			Kind:            "snapshot",
			Key:             snapshot.ID,
			SourceOperation: capture.OperationID,
		},
		StorageMiB: snapshot.Manifest.Config.Shape.DiskMiB + snapshot.Manifest.Config.Shape.MemoryMiB,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountCostHolders(ctx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountCostReference(ctx, quota.CostReference{
		TenantID:   snapshot.TenantID,
		Kind:       "snapshot",
		Key:        snapshot.ID,
		HolderKind: holderKind,
		HolderID:   holderID,
	}); err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	if count, err := responsibilities.CountResourceHolders(ctx, "snapshot", snapshot.ID); err != nil || count != int64(len(backings)+1) {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	for _, reference := range []quota.ResourceReference{
		{
			Kind:       "snapshot",
			ID:         snapshot.ID,
			HolderKind: physicalHolderKind,
			HolderID:   physicalHolderID,
		},
		{
			Kind:       "image",
			ID:         snapshot.Manifest.Config.Image,
			HolderKind: "snapshot",
			HolderID:   snapshot.ID,
		},
	} {
		if count, err := responsibilities.CountResourceReference(ctx, reference); err != nil || count != 1 {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	if !admitting {
		if count, err := dbgen.New(tx).MatchingCleanupCount(ctx, dbgen.MatchingCleanupCountParams{
			ID:           "snapshot_delete:" + step.OperationID,
			OperationID:  step.OperationID,
			ResourceKind: "snapshot",
			ResourceID:   snapshot.ID,
			Status:       cleanupStatus,
			Body:         operation.Encode(step.Input),
		}); err != nil || count != 1 {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	count, err := service.usage.TemplateSnapshotUsers(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = service.usage.PoolSnapshotUsers(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        snapshot.ID,
		TenantID:          snapshot.TenantID,
		SourceOperationID: capture.OperationID,
	})

	if err != nil || count != 1 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	count, err = SnapshotReaders(ctx, tx, snapshot.ID)
	if err != nil || count != 0 {
		return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
	}
	return backings, nil
}

func snapshotRawPath(source SnapshotSource) string {
	return filepath.Join(filepath.Dir(filepath.Dir(source.Snapshot.Dir)), "vms", source.Snapshot.SourceVM, "capture", source.CaptureOperation)
}
