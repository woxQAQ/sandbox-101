package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
)

type SnapshotDeleteRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}
type (
	snapshotDeleteInput struct {
		Request SnapshotDeleteRequest
		Source  restoreSource
	}
	snapshotDeleteStep struct {
		OperationID, Hash, Status, Phase string
		Input                            snapshotDeleteInput
	}
)

func loadSnapshotDelete(ctx context.Context, q queryer, id string) (snapshotDeleteStep, error) {
	step := snapshotDeleteStep{
		OperationID: id,
	}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT action,input_hash,input,candidate_resource_id,expected_instance,status,body FROM operation_steps WHERE operation_id=? AND step_key='snapshot_delete'", id).Scan(&action, &step.Hash, &payload, &candidate, &expected, &step.Status, &body)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
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

func (service *Service) snapshotPublication(ctx context.Context, q queryer, tenant, id string) (restoreSource, error) {
	var source restoreSource
	var body []byte
	var captureID, kind string
	var published, deleting int
	err := q.QueryRowContext(ctx, "SELECT s.body,s.published,s.deleting,o.source_operation_id,o.source_kind FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?", id, tenant).Scan(&body, &published, &deleting, &captureID, &kind)
	if err != nil {
		return source, err
	}
	if err = json.Unmarshal(body, &source.Snapshot); err != nil {
		return source, err
	}
	capture, err := loadCapture(ctx, q, captureID)
	if err != nil {
		return source, err
	}
	if published != 1 || deleting != 0 || kind != "capture" || capture.State.Boundary == nil || source.Snapshot.ID != id {
		return source, fmt.Errorf("snapshot delete requires unmarked published capture")
	}
	source.CaptureOperation = captureID
	source.CaptureHash = capture.Hash
	source.Publication = capture.State.Boundary.Publication
	return source, service.validateSnapshotPublication(source, tenant, capture)
}

// snapshotBackings enumerates explicit retained users. No name or current VM
// query substitutes for their immutable material/lease handoff proof.
func (service *Service) snapshotBackings(ctx context.Context, tx *sql.Tx, source restoreSource) ([]resourceStep, error) {
	rows, err := tx.QueryContext(ctx, "SELECT holder_id FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='backing' ORDER BY holder_id", source.Snapshot.ID)
	if err != nil {
		return nil, err
	}
	var holders []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		holders = append(holders, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	var result []resourceStep
	for _, holder := range holders {
		var id string
		err = tx.QueryRowContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='resource' AND action IN ('clone','restore') AND json_extract(input,'$.backing_holder')=? AND json_extract(input,'$.restore.snapshot.id')=?", holder, source.Snapshot.ID).Scan(&id)
		if err != nil {
			return nil, err
		}
		step, err := loadStep(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = service.validateStep(step); err != nil {
			return nil, err
		}
		if step.Input.Restore == nil || !reflect.DeepEqual(*step.Input.Restore, source) || step.Input.BackingHolder != holder || !step.State.BackingReady {
			return nil, fmt.Errorf("snapshot retained backing history changed")
		}
		if err = service.restoreRelations(ctx, tx, step); err != nil {
			return nil, err
		}
		result = append(result, step)
	}
	return result, nil
}

func (service *Service) snapshotDeleteProof(ctx context.Context, tx *sql.Tx, step snapshotDeleteStep, admitting bool) ([]resourceStep, error) {
	source := step.Input.Source
	snapshot := source.Snapshot
	request := step.Input.Request
	if request.TenantID != snapshot.TenantID || request.SnapshotID != snapshot.ID {
		return nil, fmt.Errorf("snapshot delete owner changed")
	}
	capture, err := loadCapture(ctx, tx, source.CaptureOperation)
	if err != nil {
		return nil, err
	}
	if err = service.validateSnapshotPublication(source, request.TenantID, capture); err != nil {
		return nil, err
	}
	deleting, holderKind, holderID, cleanupStatus := 1, "cleanup", step.OperationID+":snapshot_delete", "pending"
	if admitting {
		deleting = 0
		holderKind = "active_use"
		holderID = "checkpoint:" + snapshot.ID
	}
	var body []byte
	var record snapshotRecord
	var checkpoint snapshotRecord
	if err = tx.QueryRowContext(ctx, "SELECT body FROM snapshots WHERE id=? AND published=1 AND deleting=? AND source_vm_id=?", snapshot.ID, deleting, snapshot.SourceVM).Scan(&body); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(record, snapshot) {
		return nil, fmt.Errorf("snapshot delete catalog changed")
	}
	if err = tx.QueryRowContext(ctx, "SELECT body FROM checkpoints WHERE id=? AND tenant=?", snapshot.ID, snapshot.TenantID).Scan(&body); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(body, &checkpoint); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(checkpoint, snapshot) {
		return nil, fmt.Errorf("snapshot delete checkpoint changed")
	}
	if err = tx.QueryRowContext(ctx, "SELECT body FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='capture' AND resource_id=? AND status='done'", "capture:"+capture.OperationID, capture.OperationID, snapshot.SourceVM).Scan(&body); err != nil {
		return nil, fmt.Errorf("snapshot raw capture cleanup not retired: %w", err)
	}
	if string(body) != string(encode(capture.Input)) {
		return nil, fmt.Errorf("snapshot raw cleanup history changed")
	}
	backings, err := service.snapshotBackings(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	physicalHolderKind, physicalHolderID := "cleanup", step.OperationID
	if admitting {
		physicalHolderKind = "checkpoint"
		physicalHolderID = snapshot.ID
	}
	checks := []struct {
		q    string
		args []any
		want int
	}{
		{"SELECT COUNT(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind='capture'", []any{
			snapshot.ID,
			snapshot.TenantID,
			capture.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE kind='snapshot' AND resource_key=?", []any{
			snapshot.ID,
		}, 1},
		{"SELECT COUNT(*) FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND sandbox_slots=0 AND storage_mib=? AND source_operation_id=?", []any{
			snapshot.TenantID,
			snapshot.ID,
			snapshot.Manifest.Config.Shape.DiskMiB + snapshot.Manifest.Config.Shape.MemoryMiB,
			capture.OperationID,
		}, 1},
		{"SELECT COUNT(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=?", []any{
			snapshot.ID,
		}, len(backings) + 1},
		{"SELECT COUNT(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind=? AND holder_id=?", []any{
			snapshot.TenantID,
			snapshot.ID,
			holderKind,
			holderID,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=?", []any{
			snapshot.ID,
		}, len(backings) + 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind=? AND holder_id=?", []any{
			snapshot.ID,
			physicalHolderKind,
			physicalHolderID,
		}, 1},
		{"SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='snapshot' AND holder_id=?", []any{
			snapshot.Manifest.Config.Image,
			snapshot.ID,
		}, 1},
		{"SELECT COUNT(*) FROM source_leases WHERE snapshot_id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM warm WHERE snapshot_id=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM templates WHERE json_extract(body,'$.snapshot_id')=?", []any{
			snapshot.ID,
		}, 0},
		{"SELECT COUNT(*) FROM reservations WHERE owner=? AND bytes=? AND memory=0 AND cpus=0", []any{
			"snapshot:" + snapshot.ID,
			(2*snapshot.Manifest.Config.Shape.MemoryMiB + snapshot.Manifest.Config.Shape.DiskMiB + 224) << 20,
		}, 1},
	}
	if !admitting {
		checks = append(checks, struct {
			q    string
			args []any
			want int
		}{"SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='snapshot' AND resource_id=? AND status=? AND body=?", []any{
			"snapshot_delete:" + step.OperationID,
			step.OperationID,
			snapshot.ID,
			cleanupStatus,
			encode(step.Input),
		}, 1})
		var original struct {
			Action  string
			Request SnapshotDeleteRequest
		}
		var result operationResult
		var frozen []byte
		var hash, status, scope, tenant string
		if err = tx.QueryRowContext(ctx, "SELECT request,request_hash,status,scope_kind,scope_id,body FROM operations WHERE id=?", step.OperationID).Scan(&frozen, &hash, &status, &scope, &tenant, &body); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(frozen, &original); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		if digest(frozen) != hash || terminal(status) || scope != "tenant" || tenant != request.TenantID || original.Action != "snapshot_delete" || original.Request != request || result.SnapshotID != snapshot.ID {
			return nil, fmt.Errorf("snapshot delete parent identity changed")
		}
	}
	for _, check := range checks {
		var count int
		if err = tx.QueryRowContext(ctx, check.q, check.args...).Scan(&count); err != nil || count != check.want {
			return nil, fmt.Errorf("snapshot deletion responsibility changed: %v", err)
		}
	}
	return backings, nil
}

func snapshotRawPath(source restoreSource) string {
	return filepath.Join(filepath.Dir(filepath.Dir(source.Snapshot.Dir)), "vms", source.Snapshot.SourceVM, "capture", source.CaptureOperation)
}
