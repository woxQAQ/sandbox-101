package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

func (service *Service) internalSnapshot(ctx context.Context, source restoreSource) (bool, bool, error) {
	rows, err := service.store.db.QueryContext(ctx, "SELECT operation_id,input_hash,input,status FROM operation_steps WHERE action='resource_link' AND step_key='link:capture' AND json_extract(input,'$.child_id')=?", source.CaptureOperation)
	if err != nil {
		return false, false, err
	}
	type linkEntry struct {
		id, hash, status string
		input            []byte
	}
	var entries []linkEntry
	for rows.Next() {
		var entry linkEntry
		if err = rows.Scan(&entry.id, &entry.hash, &entry.input, &entry.status); err != nil {
			break
		}
		entries = append(entries, entry)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return false, false, err
	}
	if len(entries) == 0 {
		return false, false, nil
	}
	if len(entries) != 1 {
		return false, false, fmt.Errorf("snapshot has ambiguous control publication")
	}
	entry := entries[0]
	if digest(entry.input) != entry.hash {
		return false, false, fmt.Errorf("snapshot control link checksum changed")
	}
	var link resourceLink
	if err = json.Unmarshal(entry.input, &link); err != nil {
		return false, false, err
	}
	if link.Kind != "capture" || link.Candidate != source.Snapshot.ID {
		return false, false, fmt.Errorf("snapshot control link identity changed")
	}
	if lifecycle, err := loadLifecycle(ctx, service.store.db, entry.id); err == nil {
		if lifecycle.Input.OwnerTenantID != source.Snapshot.TenantID || (lifecycle.Input.Command.Action != "pause" && lifecycle.Input.Command.Action != "checkpoint") {
			return false, false, fmt.Errorf("snapshot lifecycle publication scope changed")
		}
		return true, terminal(lifecycle.Status) && entry.status == "succeeded", nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, false, err
	}
	supply, err := loadSupply(ctx, service.store.db, entry.id)
	if err != nil {
		return false, false, err
	}
	if supply.Input.Command.Action != "template_build" || supply.Input.Template.TenantID != source.Snapshot.TenantID {
		return false, false, fmt.Errorf("snapshot supply publication scope changed")
	}
	return true, terminal(supply.Status) && entry.status == "succeeded", nil
}

func (service *Service) snapshotGCCandidate(ctx context.Context, snapshot snapshotRecord) (bool, string, error) {
	source, err := service.snapshotPublication(ctx, service.store.db, snapshot.TenantID, snapshot.ID)
	if err != nil {
		return false, "", err
	}
	internal, complete, err := service.internalSnapshot(ctx, source)
	if err != nil {
		return false, "", err
	}
	if !internal {
		return false, "", nil
	}
	if !complete {
		return false, "owning control workflow remains unresolved", nil
	}
	var readers int
	if err = service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_leases WHERE snapshot_id=?", snapshot.ID).Scan(&readers); err != nil {
		return false, "", err
	}
	if readers != 0 {
		return false, "source lease retained", nil
	}
	if err = service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND NOT (holder_kind='backing' OR (holder_kind='checkpoint' AND holder_id=?))", snapshot.ID, snapshot.ID).Scan(&readers); err != nil {
		return false, "", err
	}
	if readers != 0 {
		return false, "active or unresolved snapshot use retained", nil
	}
	var raw string
	if err = service.store.db.QueryRowContext(ctx, "SELECT status FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='capture'", "capture:"+source.CaptureOperation, source.CaptureOperation).Scan(&raw); err != nil {
		return false, "", err
	}
	if raw != "done" {
		return false, "raw capture cleanup not retired", nil
	}
	return true, "", nil
}
