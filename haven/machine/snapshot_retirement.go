package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// RetireDeletedSnapshot runs only after a durable catalog deletion or a backing
// retirement. Retained fees need an independent physical receipt, never an absent
// catalog alone. Legacy private fixtures without that receipt remain charged.
func RetireDeletedSnapshot(ctx context.Context, tx *sql.Tx, source SnapshotSource, history SnapshotProofs) error {
	operations := operation.New(tx)
	cleanup, err := dbgen.New(tx).GetRetainedCleanup(ctx, dbgen.GetRetainedCleanupParams{
		ResourceKind: "snapshot",
		ResourceID:   source.Snapshot.ID,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var input snapshotDeleteInput
	if err = json.Unmarshal(cleanup.Body, &input); err != nil {
		return err
	}
	if !reflect.DeepEqual(input.Source, source) {
		return fmt.Errorf("snapshot retained fee provenance changed")
	}
	step, err := LoadSnapshotDelete(ctx, tx, cleanup.OperationID)
	if err != nil {
		return err
	}
	if step.Status != "succeeded" || step.Phase != "complete" || !reflect.DeepEqual(step.Input, input) {
		return fmt.Errorf("snapshot fee retirement lacks completed deletion")
	}
	publication := source.Snapshot
	parent, err := operations.Projection(ctx, cleanup.OperationID)
	if err != nil {
		return err
	}
	var request struct {
		Action  string
		Request SnapshotDeleteRequest
	}
	var result operation.Result
	if err = json.Unmarshal(parent.Request, &request); err != nil {
		return err
	}
	if err = json.Unmarshal(parent.Body, &result); err != nil {
		return err
	}
	if operation.Digest(parent.Request) != parent.RequestHash || parent.Status != "succeeded" || parent.ScopeKind != "tenant" || parent.ScopeID != publication.TenantID || request.Action != "snapshot_delete" || request.Request != input.Request || result.SnapshotID != publication.ID {
		return fmt.Errorf("snapshot retained deletion parent changed")
	}
	refs, err := quota.CountResourceHolders(ctx, tx, "snapshot", publication.ID)
	if err != nil {
		return err
	}
	fees, err := quota.CountCostHolders(ctx, tx, "snapshot", publication.ID)
	if err != nil {
		return err
	}
	if refs != 0 || fees != 0 {
		return nil
	}
	capture, err := LoadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return err
	}
	if err = history.ValidateSnapshotPublication(source, publication.TenantID, capture); err != nil {
		return err
	}
	for _, path := range []string{
		publication.Dir,
		snapshotRawPath(source),
	} {
		if _, err = os.Lstat(path); !os.IsNotExist(err) {
			return fmt.Errorf("snapshot fee retirement physical absence unconfirmed: %v", err)
		}
	}
	body, err := dbgen.New(tx).GetDeletionReceipt(ctx, dbgen.GetDeletionReceiptParams{
		Path:        publication.Dir,
		OperationID: cleanup.OperationID,
		StepKey:     "snapshot_delete",
	})
	if err != nil {
		return err
	}
	var intent loom.DeletionIntent
	if err = json.Unmarshal(body, &intent); err != nil {
		return err
	}
	if err = loom.VerifySnapshotDeletion(intent, publication.Dir, source.Publication); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(intent); err != nil {
		return err
	}
	count, err := dbgen.New(tx).CountCheckpoint(ctx, publication.ID)

	if err != nil || count != 0 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = quota.CountReservation(ctx, tx, "snapshot:"+publication.ID)
	if err != nil || count != 0 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = dbgen.New(tx).CountCompletedCleanup(ctx, dbgen.CountCompletedCleanupParams{
		ID:          "capture:" + source.CaptureOperation,
		OperationID: source.CaptureOperation,
		Body:        operation.Encode(capture.Input),
	})

	if err != nil || count != 1 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = quota.CountCost(ctx, tx, "snapshot", publication.ID)
	if err != nil || count != 1 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	cost := quota.CostIdentity{
		TenantID:        publication.TenantID,
		Kind:            "snapshot",
		Key:             publication.ID,
		SourceOperation: source.CaptureOperation,
	}
	count, err = quota.CountMatchingCost(ctx, tx, quota.Cost{
		CostIdentity: cost,
		StorageMiB:   publication.Manifest.Config.Shape.DiskMiB + publication.Manifest.Config.Shape.MemoryMiB,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = dbgen.New(tx).CountSnapshot(ctx, publication.ID)

	if err != nil || count != 0 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = SnapshotReaders(ctx, tx, publication.ID)
	if err != nil || count != 0 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	count, err = dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        publication.ID,
		TenantID:          publication.TenantID,
		SourceOperationID: source.CaptureOperation,
	})

	if err != nil || count != 1 {
		return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
	}
	if err = quota.RemoveResourceReference(ctx, tx, quota.ResourceReference{
		Kind:       "image",
		ID:         publication.Manifest.Config.Image,
		HolderKind: "snapshot",
		HolderID:   publication.ID,
	}); err != nil {
		return err
	}
	if err = quota.RetireCost(ctx, tx, cost); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).RetireDeletionReceipt(ctx, dbgen.RetireDeletionReceiptParams{
		Path:        publication.Dir,
		OperationID: cleanup.OperationID,
		StepKey:     "snapshot_delete",
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).CompleteRetainedCleanup(ctx, dbgen.CompleteRetainedCleanupParams{
		ID:          "snapshot_delete:" + cleanup.OperationID,
		OperationID: cleanup.OperationID,
	}))
}
