package haven

import (
	"context"
	"fmt"
)

// ensureDataResume serializes only selection of a durable attempt. It does not
// hold the sandbox lifecycle lock while a worker must acquire that same lock.
func (service *Service) ensureDataResume(ctx context.Context, principal Principal, id string) error {
	guard := service.vmLock("auto-resume:" + id)
	guard.Lock()
	defer guard.Unlock()
	sandbox, err := service.Get(ctx, principal, id)
	if err != nil {
		return err
	}
	switch sandbox.State {
	case "paused":
		if err = service.dataFence(ctx, id); err != nil {
			return err
		}
		settings, err := loadSettings(ctx, service.store.db, id)
		if err != nil {
			return err
		}
		operation, err := service.AdmitResume(ctx, Principal{
			TenantID: sandbox.TenantID,
		}, "auto-resume:"+id+":"+sandbox.SnapshotID+":"+settings.Generation, Command{
			Action:    "resume",
			SandboxID: id,
		})
		if err != nil {
			return err
		}
		_, err = service.Wait(ctx, Principal{
			TenantID: sandbox.TenantID,
		}, operation.ID)
		return err
	case "resuming":
		lifecycle, err := loadLifecycle(ctx, service.store.db, sandbox.OperationID)
		if err != nil {
			return err
		}
		if lifecycle.Input.OwnerTenantID != sandbox.TenantID || lifecycle.Input.Sandbox.ID != id || lifecycle.Input.Command.Action != "resume" {
			return fmt.Errorf("%w: automatic resume ownership changed", ErrConflict)
		}
		_, err = service.Wait(ctx, Principal{
			TenantID: lifecycle.Input.RequestTenantID,
		}, lifecycle.OperationID)
		return err
	default:
		return nil
	}
}
