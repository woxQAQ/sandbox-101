package supply

import (
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
)

func (service *SupplyCleanup) completeResourceLink(parent, role string, link machine.ResourceLink) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	changed, err := link.Complete(service.ctx, tx, parent, role)
	if err != nil || !changed {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (service *SupplyCleanup) resumeChild(link machine.ResourceLink) error {
	var handled bool
	var err error
	switch link.Kind {
	case "stop":
		handled, err = service.stops.Resume(link.ChildID)
	case "capture_discard":
		handled, err = service.captures.Resume(link.ChildID)
	case "release":
		handled, err = service.deletion.ResumeRelease(link.ChildID)
	case "discard":
		handled, err = service.deletion.ResumeDiscard(link.ChildID)
	default:
		return fmt.Errorf("supply cleanup child kind unsupported")
	}
	if err != nil {
		return err
	}
	if !handled {
		return fmt.Errorf("supply cleanup child lacks matching immutable step")
	}
	return nil
}

func (service *SupplyCleanup) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	link, status, err := machine.LoadResourceLink(service.ctx, service.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status == "failed" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	if status == "succeeded" {
		return link, nil
	}
	runErr := service.resumeChild(link)
	if errors.Is(runErr, errs.ErrInterrupted) {
		return link, runErr
	}
	if err = service.completeResourceLink(parent, role, link); err != nil {
		return link, errors.Join(err, runErr)
	}
	_, status, err = machine.LoadResourceLink(service.ctx, service.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status != "succeeded" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	return link, nil
}
