package haven

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
)

func (service *Service) runSupplyCleanup(step supplyCleanupStep) error {
	if terminal(step.Status) {
		return nil
	}
	if step.Input.Retired == nil {
		source := step.Input.Source
		if len(step.Input.CaptureFailures) > 0 {
			if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "stop"); errors.Is(lookup, sql.ErrNoRows) {
				parent := &supplyCleanupAdmission{Parent: step, Role: "stop"}
				if _, err := service.admitStop(service.ctx, StopRequest{TenantID: source.Input.TenantID, Key: step.OperationID + ":stop", SandboxID: source.Input.SandboxID, ExpectedInstance: source.State.Witness}, parent); err != nil {
					return err
				}
			} else if lookup != nil {
				return lookup
			}
			stopped, err := service.runResourceLink(step.OperationID, "stop")
			if err != nil {
				return err
			}
			for _, reference := range step.Input.CaptureFailures {
				role := "capture:" + reference.OperationID
				if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
					parent := &supplyCleanupAdmission{Parent: step, Role: role}
					if _, err = service.admitCaptureDiscard(service.ctx, CaptureDiscardRequest{TenantID: source.Input.TenantID, Key: step.OperationID + ":" + role, SandboxID: source.Input.SandboxID, FailedOperationID: reference.OperationID, ExpectedInstance: source.State.Witness, StopOperationID: stopped.ChildID}, parent); err != nil {
						return err
					}
				} else if lookup != nil {
					return lookup
				}
				if _, err = service.runResourceLink(step.OperationID, role); err != nil {
					return err
				}
			}
		}
		if _, _, lookup := loadResourceLink(service.ctx, service.store.db, step.OperationID, "resource"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &supplyCleanupAdmission{Parent: step, Role: "resource"}
			if source.Status == "failed" {
				if _, err := service.admitDiscard(service.ctx, DiscardRequest{TenantID: source.Input.TenantID, Key: step.OperationID + ":resource", SandboxID: source.Input.SandboxID, FailedOperationID: source.OperationID, ExpectedInstance: source.Input.Resource.Instance}, parent); err != nil {
					return err
				}
			} else {
				if _, err := service.admitRelease(service.ctx, ReleaseRequest{TenantID: source.Input.TenantID, Key: step.OperationID + ":resource", SandboxID: source.Input.SandboxID}, parent); err != nil {
					return err
				}
			}
		} else if lookup != nil {
			return lookup
		}
		if _, err := service.runResourceLink(step.OperationID, "resource"); err != nil {
			return err
		}
	}
	return service.completeSupplyCleanup(step)
}

func (service *Service) completeSupplyCleanup(step supplyCleanupStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadSupplyCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply cleanup current intent changed")
	}
	original, err := loadSupply(service.ctx, tx, step.Input.Original.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(original, step.Input.Original) {
		return fmt.Errorf("supply cleanup original terminal changed")
	}
	source := step.Input.Source
	if step.Input.Retired != nil {
		retired, err := loadStep(service.ctx, tx, step.Input.Retired.OperationID)
		if err != nil {
			return err
		}
		if retired.Hash != step.Input.Retired.InputHash || retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID {
			return fmt.Errorf("supply cleanup original retirement proof changed")
		}
	} else {
		link, status, err := loadResourceLink(service.ctx, tx, step.OperationID, "resource")
		if err != nil {
			return err
		}
		if status != "succeeded" {
			return ErrInterrupted
		}
		switch link.Kind {
		case "release":
			retired, err := loadStep(service.ctx, tx, link.ChildID)
			if err != nil {
				return err
			}
			if retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID || retired.Candidate != source.Candidate {
				return fmt.Errorf("supply cleanup release receipt changed")
			}
		case "discard":
			retired, err := loadDiscard(service.ctx, tx, link.ChildID)
			if err != nil {
				return err
			}
			if retired.Status != "succeeded" || retired.State.Phase != "complete" || !reflect.DeepEqual(retired.Input.Source, source) {
				return fmt.Errorf("supply cleanup failed-candidate receipt changed")
			}
		default:
			return fmt.Errorf("supply cleanup resource link kind unsupported")
		}
	}
	var count int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM vms WHERE id=?", source.Candidate).Scan(&count); err != nil || count != 0 {
		return fmt.Errorf("supply cleanup VM responsibility remains: %v", err)
	}
	if step.Input.Pool != nil {
		pool, status, err := loadPoolReservation(service.ctx, tx, original.OperationID)
		if err != nil {
			return err
		}
		if status != "cleanup" || !reflect.DeepEqual(pool, *step.Input.Pool) {
			return fmt.Errorf("supply cleanup pool slot changed")
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM pool_slots WHERE operation_id=? AND status='cleanup' AND body=?", original.OperationID, encode(pool))); err != nil {
			return err
		}
	}
	if original.Status == "failed" {
		if err = requireWrite(tx.ExecContext(service.ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", source.Input.Resource.Config.Image, original.OperationID+":supply")); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='supply' AND status='pending'", "supply:"+original.OperationID, original.OperationID)); err != nil {
			return err
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{source.Input.Resource.Config.Image, step.OperationID + ":supply_cleanup"}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='supply_cleanup' AND status='pending'", []any{"supply_cleanup:" + step.OperationID, step.OperationID}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='supply_cleanup' AND input_hash=? AND body=? AND status IN ('pending','running')", []any{encode("complete"), step.OperationID, step.Hash, encode(step.Phase)}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{step.OperationID}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("supply_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
