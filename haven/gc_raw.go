package haven

import (
	"context"
	"database/sql"
	"errors"
)

func (service *Service) rawGCCandidates(ctx context.Context) ([]GCItem, []GCItem, error) {
	rows, err := service.store.db.QueryContext(ctx, "SELECT operation_id FROM cleanup_intents WHERE resource_kind='capture' AND status='pending' ORDER BY operation_id")
	if err != nil {
		return nil, nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return nil, nil, err
	}
	var candidates, deferred []GCItem
	for _, id := range ids {
		capture, err := loadCapture(ctx, service.store.db, id)
		if err != nil {
			return nil, nil, err
		}
		if capture.Status != "succeeded" {
			continue
		}
		source := capture.Input.Source
		var state, instance string
		var owner sql.NullString
		err = service.store.db.QueryRowContext(ctx, "SELECT state,current_instance,operation_id FROM vms WHERE id=?", source.Candidate).Scan(&state, &instance, &owner)
		if errors.Is(err, sql.ErrNoRows) {
			deferred = append(deferred, GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "source lacks independent successful reclamation/stop proof",
			})
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if state != "stopped" || owner.Valid || instance != source.State.Witness {
			deferred = append(deferred, GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "source remains active or owned",
			})
			continue
		}
		stopRows, err := service.store.db.QueryContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='stop' AND status='succeeded' AND candidate_resource_id=? AND expected_instance=? ORDER BY rowid DESC", source.Input.SandboxID, source.State.Witness)
		if err != nil {
			return nil, nil, err
		}
		var stops []string
		for stopRows.Next() {
			var stop string
			if err = stopRows.Scan(&stop); err != nil {
				break
			}
			stops = append(stops, stop)
		}
		if err = errors.Join(err, stopRows.Err(), stopRows.Close()); err != nil {
			return nil, nil, err
		}
		selected := ""
		for _, stop := range stops {
			proof, err := loadStop(ctx, service.store.db, stop)
			if err != nil {
				return nil, nil, err
			}
			if proof.Input.SourceOperation == source.OperationID && proof.Input.SourceHash == source.Hash && proof.Phase == "complete" {
				selected = stop
				break
			}
		}
		if selected == "" {
			deferred = append(deferred, GCItem{
				Kind:   "raw_capture",
				ID:     id,
				Reason: "original stopped generation witness missing",
			})
			continue
		}
		candidates = append(candidates, GCItem{
			Kind:        "raw_capture",
			ID:          id,
			TenantID:    capture.Input.Request.TenantID,
			Proof:       capture.Hash,
			OperationID: selected,
		})
	}
	return candidates, deferred, nil
}
