package dataplane

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

// ensureResume serializes only selection of a durable attempt. It does not
// hold the sandbox lifecycle lock while a worker must acquire that same lock.
func (p *Plane) ensureResume(ctx context.Context, principal authorization.Principal, id string) error {
	unlock := p.wakeupLocks.Lock(id)
	defer unlock()
	view, err := p.catalog.Get(ctx, principal, id)
	if err != nil {
		return err
	}
	switch view.State {
	case "paused":
		if err = p.catalog.Unfenced(ctx, id); err != nil {
			return err
		}
		settings, err := sandbox.LoadSettings(ctx, p.store.Database, id)
		if err != nil {
			return err
		}
		accepted, err := p.lifecycle.AdmitResume(ctx, authorization.Principal{
			TenantID: view.TenantID,
		}, "auto-resume:"+id+":"+view.SnapshotID+":"+settings.Generation, operation.Intent{
			Action:    "resume",
			SandboxID: id,
		})
		if err != nil {
			return err
		}
		_, err = p.operations.Wait(ctx, authorization.Principal{
			TenantID: view.TenantID,
		}, accepted.ID)
		return err
	case "resuming":
		lifecycle, err := sandbox.LoadLifecycleStep(ctx, p.store.Database, view.OperationID)
		if err != nil {
			return err
		}
		if lifecycle.Input.OwnerTenantID != view.TenantID || lifecycle.Input.Sandbox.ID != id || lifecycle.Input.Command.Action != "resume" {
			return fmt.Errorf("%w: automatic resume ownership changed", errs.ErrConflict)
		}
		_, err = p.operations.Wait(ctx, authorization.Principal{
			TenantID: lifecycle.Input.RequestTenantID,
		}, lifecycle.OperationID)
		return err
	default:
		return nil
	}
}
