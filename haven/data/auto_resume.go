package data

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

// ensureDataResume serializes only selection of a durable attempt. It does not
// hold the sandbox lifecycle lock while a worker must acquire that same lock.
func (service *Data) ensureDataResume(ctx context.Context, principal authorization.Principal, id string) error {
	guard := service.guards.AutoResume(id)
	guard.Lock()
	defer guard.Unlock()
	view, err := service.catalog.Get(ctx, principal, id)
	if err != nil {
		return err
	}
	switch view.State {
	case "paused":
		if err = service.catalog.Unfenced(ctx, id); err != nil {
			return err
		}
		settings, err := sandbox.LoadSettings(ctx, service.store.Database, id)
		if err != nil {
			return err
		}
		accepted, err := service.lifecycle.AdmitResume(ctx, authorization.Principal{
			TenantID: view.TenantID,
		}, "auto-resume:"+id+":"+view.SnapshotID+":"+settings.Generation, operation.Command{
			Action:    "resume",
			SandboxID: id,
		})
		if err != nil {
			return err
		}
		_, err = service.operations.Wait(ctx, authorization.Principal{
			TenantID: view.TenantID,
		}, accepted.ID)
		return err
	case "resuming":
		lifecycle, err := sandbox.LoadLifecycle(ctx, service.store.Database, view.OperationID)
		if err != nil {
			return err
		}
		if lifecycle.Input.OwnerTenantID != view.TenantID || lifecycle.Input.Sandbox.ID != id || lifecycle.Input.Command.Action != "resume" {
			return fmt.Errorf("%w: automatic resume ownership changed", errs.ErrConflict)
		}
		_, err = service.operations.Wait(ctx, authorization.Principal{
			TenantID: lifecycle.Input.RequestTenantID,
		}, lifecycle.OperationID)
		return err
	default:
		return nil
	}
}
