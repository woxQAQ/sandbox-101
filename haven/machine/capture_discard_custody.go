package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
)

// captureDiscardCustody is the exact responsibility expected at one durable
// boundary. These facts are matched directly against storage, without selecting
// execution paths or inferring a holder from current catalog state.
type captureDiscardCustody struct {
	Deleting      int64
	CleanupStatus string
	Holder        quota.Holder
}

func verifyCaptureDiscardCustody(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep, expected captureDiscardCustody) error {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	snapshot := source.Input.Snapshot
	publication, err := dbgen.New(tx).GetCapture(ctx, dbgen.GetCaptureParams{
		ID:         snapshot.ID,
		SourceVmID: resource.Candidate,
	})
	if err != nil {
		return err
	}
	var record SnapshotRecord
	if err = json.Unmarshal(publication.Body, &record); err != nil {
		return err
	}
	if publication.Published != 0 || publication.Deleting != expected.Deleting || !reflect.DeepEqual(record, snapshot) {
		return fmt.Errorf("capture discard unpublished catalog changed")
	}
	body, err := dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + source.OperationID,
		OperationID:  source.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   snapshot.ID,
		Status:       expected.CleanupStatus,
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
				HolderKind: expected.Holder.Kind,
				HolderID:   expected.Holder.ID,
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
