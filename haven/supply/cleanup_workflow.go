package supply

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *SupplyCleanup) runSupplyCleanup(step SupplyCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if step.Input.Retired == nil {
		source := step.Input.Source
		if len(step.Input.CaptureFailures) > 0 {
			if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "stop"); errors.Is(lookup, sql.ErrNoRows) {
				parent := &SupplyCleanupAdmission{
					Parent: step,
					Role:   "stop",
				}
				if _, err := service.stops.AdmitForParent(service.ctx, machine.StopRequest{
					TenantID:         source.Input.TenantID,
					Key:              step.OperationID + ":stop",
					SandboxID:        source.Input.SandboxID,
					ExpectedInstance: source.State.Witness,
				}, parent); err != nil {
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
				if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, role); errors.Is(lookup, sql.ErrNoRows) {
					parent := &SupplyCleanupAdmission{
						Parent: step,
						Role:   role,
					}
					if _, err = service.captures.AdmitForParent(service.ctx, machine.CaptureDiscardRequest{
						TenantID:          source.Input.TenantID,
						Key:               step.OperationID + ":" + role,
						SandboxID:         source.Input.SandboxID,
						FailedOperationID: reference.OperationID,
						ExpectedInstance:  source.State.Witness,
						StopOperationID:   stopped.ChildID,
					}, parent); err != nil {
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
		if _, _, lookup := machine.LoadResourceLink(service.ctx, service.store.Database, step.OperationID, "resource"); errors.Is(lookup, sql.ErrNoRows) {
			parent := &SupplyCleanupAdmission{
				Parent: step,
				Role:   "resource",
			}
			if source.Status == "failed" {
				if _, err := service.deletion.AdmitDiscardForParent(service.ctx, machine.DiscardRequest{
					TenantID:          source.Input.TenantID,
					Key:               step.OperationID + ":resource",
					SandboxID:         source.Input.SandboxID,
					FailedOperationID: source.OperationID,
					ExpectedInstance:  source.Input.Resource.Instance,
				}, parent); err != nil {
					return err
				}
			} else {
				if _, err := service.deletion.AdmitReleaseForParent(service.ctx, machine.ReleaseRequest{
					TenantID:  source.Input.TenantID,
					Key:       step.OperationID + ":resource",
					SandboxID: source.Input.SandboxID,
				}, parent); err != nil {
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

func (service *SupplyCleanup) completeSupplyCleanup(step SupplyCleanupStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadSupplyCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("supply cleanup current intent changed")
	}
	original, err := LoadSupply(service.ctx, tx, step.Input.Original.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(original, step.Input.Original) {
		return fmt.Errorf("supply cleanup original terminal changed")
	}
	source := step.Input.Source
	if step.Input.Retired != nil {
		retired, err := machine.LoadResourceStep(service.ctx, tx, step.Input.Retired.OperationID)
		if err != nil {
			return err
		}
		if retired.Hash != step.Input.Retired.InputHash || retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID {
			return fmt.Errorf("supply cleanup original retirement proof changed")
		}
	} else {
		link, status, err := machine.LoadResourceLink(service.ctx, tx, step.OperationID, "resource")
		if err != nil {
			return err
		}
		if status != "succeeded" {
			return errs.ErrInterrupted
		}
		switch link.Kind {
		case "release":
			retired, err := machine.LoadResourceStep(service.ctx, tx, link.ChildID)
			if err != nil {
				return err
			}
			if retired.Status != "succeeded" || retired.State.Phase != "complete" || retired.Input.SourceOperation != source.OperationID || retired.Candidate != source.Candidate {
				return fmt.Errorf("supply cleanup release receipt changed")
			}
		case "discard":
			retired, err := machine.LoadDiscard(service.ctx, tx, link.ChildID)
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
	count, err := machine.CurrentVMCount(service.ctx, tx, source.Candidate)
	if err != nil || count != 0 {
		return fmt.Errorf("supply cleanup VM responsibility remains: %v", err)
	}
	if step.Input.Pool != nil {
		reservation, status, err := LoadPoolReservation(service.ctx, tx, original.OperationID)
		if err != nil {
			return err
		}
		if status != "cleanup" || !reflect.DeepEqual(reservation, *step.Input.Pool) {
			return fmt.Errorf("supply cleanup pool slot changed")
		}
		if err = store.RequireOne(dbgen.New(tx).DeleteReservation(service.ctx, dbgen.DeleteReservationParams{
			OperationID: original.OperationID,
			Status:      "cleanup",
			Body:        operation.Encode(reservation),
		})); err != nil {
			return err
		}
	}
	if original.Status == "failed" {
		if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         source.Input.Resource.Config.Image,
			HolderKind: "step",
			HolderID:   original.OperationID + ":supply",
		}); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(service.ctx, dbgen.CompletePendingCleanupKindParams{
			ID:           "supply:" + original.OperationID,
			OperationID:  original.OperationID,
			ResourceKind: "supply",
		})); err != nil {
			return err
		}
	}
	if err = quota.New(tx).RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply_cleanup",
	}); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanupKind(service.ctx, dbgen.CompletePendingCleanupKindParams{
		ID:           "supply_cleanup:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "supply_cleanup",
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(service.ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("supply_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
