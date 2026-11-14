package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

// retireDeletedSnapshot runs only after a durable catalog deletion or a backing
// retirement. Retained fees need an independent physical receipt, never an absent
// catalog alone. Legacy private fixtures without that receipt remain charged.
func (service *Service) retireDeletedSnapshot(ctx context.Context, tx *sql.Tx, source restoreSource) error {
	var operation string
	var body []byte
	err := tx.QueryRowContext(ctx, "SELECT operation_id,body FROM cleanup_intents WHERE resource_kind='snapshot' AND resource_id=? AND status='retained'", source.Snapshot.ID).Scan(&operation, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var input snapshotDeleteInput
	if err = json.Unmarshal(body, &input); err != nil {
		return err
	}
	if !reflect.DeepEqual(input.Source, source) {
		return fmt.Errorf("snapshot retained fee provenance changed")
	}
	step, err := loadSnapshotDelete(ctx, tx, operation)
	if err != nil {
		return err
	}
	if step.Status != "succeeded" || step.Phase != "complete" || !reflect.DeepEqual(step.Input, input) {
		return fmt.Errorf("snapshot fee retirement lacks completed deletion")
	}
	snapshot := source.Snapshot
	var request, parentBody []byte
	var parentHash, parentStatus, parentScope, parentTenant string
	if err = tx.QueryRowContext(ctx, "SELECT request,request_hash,status,scope_kind,scope_id,body FROM operations WHERE id=?", operation).Scan(&request, &parentHash, &parentStatus, &parentScope, &parentTenant, &parentBody); err != nil {
		return err
	}
	var parent struct {
		Action  string
		Request SnapshotDeleteRequest
	}
	var result operationResult
	if err = json.Unmarshal(request, &parent); err != nil {
		return err
	}
	if err = json.Unmarshal(parentBody, &result); err != nil {
		return err
	}
	if digest(request) != parentHash || parentStatus != "succeeded" || parentScope != "tenant" || parentTenant != snapshot.TenantID || parent.Action != "snapshot_delete" || parent.Request != input.Request || result.SnapshotID != snapshot.ID {
		return fmt.Errorf("snapshot retained deletion parent changed")
	}
	var refs, fees int
	if err = tx.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=?),(SELECT COUNT(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=?)", snapshot.ID, snapshot.ID).Scan(&refs, &fees); err != nil {
		return err
	}
	if refs != 0 || fees != 0 {
		return nil
	}
	capture, err := loadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return err
	}
	if err = service.validateSnapshotPublication(source, snapshot.TenantID, capture); err != nil {
		return err
	}
	for _, path := range []string{
		snapshot.Dir,
		snapshotRawPath(source),
	} {
		if _, err = os.Lstat(path); !os.IsNotExist(err) {
			return fmt.Errorf("snapshot fee retirement physical absence unconfirmed: %v", err)
		}
	}
	var intent loom.DeletionIntent
	if err = tx.QueryRowContext(ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='snapshot_delete'", snapshot.Dir, operation).Scan(&body); err != nil {
		return err
	}
	if err = json.Unmarshal(body, &intent); err != nil {
		return err
	}
	if err = loom.VerifySnapshotDeletion(intent, snapshot.Dir, source.Publication); err != nil {
		return err
	}
	var identities []loom.FileIdentity
	for _, identity := range intent.Nodes {
		identities = append(identities, identity)
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM snapshots WHERE id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM checkpoints WHERE id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM source_leases WHERE snapshot_id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM reservations WHERE owner=?", []any{
			"snapshot:" + snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture'", []any{
			snapshot.ID,
			snapshot.TenantID,
			source.CaptureOperation,
		}, 1},
		{"SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND status='done' AND body=?", []any{
			"capture:" + source.CaptureOperation,
			source.CaptureOperation,
			encode(capture.Input),
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE kind='snapshot' AND resource_key=?", []any{
			snapshot.ID,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND storage_mib=? AND sandbox_slots=0 AND source_operation_id=?", []any{
			snapshot.TenantID,
			snapshot.ID,
			snapshot.Manifest.Config.Shape.DiskMiB + snapshot.Manifest.Config.Shape.MemoryMiB,
			source.CaptureOperation,
		}, 1},
	}
	for _, check := range checks {
		var count int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&count); err != nil || count != check.want {
			return fmt.Errorf("snapshot retained retirement proof changed: %v", err)
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='snapshot' AND holder_id=?", []any{
			snapshot.Manifest.Config.Image,
			snapshot.ID,
		}},
		{"DELETE FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND source_operation_id=?", []any{
			snapshot.TenantID,
			snapshot.ID,
			source.CaptureOperation,
		}},
		{"DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='snapshot_delete'", []any{
			snapshot.Dir,
			operation,
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='retained'", []any{
			"snapshot_delete:" + operation,
			operation,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}
