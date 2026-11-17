package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
)

// ReconcileBindings keeps an already published none policy free of residual
// proxies and rebinds a restart without renewing its saved lease.
func (service *Data) ReconcileBindings() error {
	if service.store.Reviewed {
		return nil
	}

	subjects, err := sandbox.BoundSubjects(service.ctx, service.store.Database)
	if err != nil {
		return err
	}
	for _, subject := range subjects {
		id, tenant := subject.ID, subject.TenantID
		guard := service.guards.Configuration(id)
		if !guard.TryLock() {
			continue
		}
		settings, lookup := sandbox.LoadSettings(service.ctx, service.store.Database, id)
		if lookup == nil {
			_, fenceErr := sandbox.LifecycleFence(service.ctx, service.store.Database, id)
			if fenceErr == nil {
				guard.Unlock()
				continue
			}
			if !errors.Is(fenceErr, sql.ErrNoRows) {
				lookup = fenceErr
			}
		}
		if lookup == nil && (settings.Policy.Mode == "none" || (len(settings.Secrets) > 0 && settings.ActiveEpoch != service.epoch) || !time.Now().Before(settings.Deadline)) {
			lookup = service.activations.RevokeSandbox(id)
		}
		rebind := lookup == nil && settings.ActiveEpoch != service.epoch && len(settings.Secrets) == 0 && time.Now().Before(settings.Deadline) && service.opts.Guest != nil
		guard.Unlock()
		if rebind {
			bindingCtx, cancel := context.WithTimeout(service.ctx, 10*time.Second)
			_, lookup = service.waitRestartBinding(bindingCtx, sandbox.View{
				ID:       id,
				TenantID: tenant,
			}, settings)
			cancel()
		}
		if lookup != nil {
			service.access.RecordBindingError(id, lookup)
		}
	}
	return nil
}

func (service *Data) waitRestartBinding(ctx context.Context, view sandbox.View, settings sandbox.Settings) (operation.Record, error) {
	env := map[string]operation.EnvVar{}
	for name, value := range settings.Env {
		env[name] = operation.EnvVar{
			Value: value,
		}
	}
	accepted, err := service.sandboxes.AdmitConfigure(ctx, authorization.Principal{
		TenantID: view.TenantID,
	}, "rebind:"+service.epoch+":"+view.ID+":"+settings.Generation, operation.Command{
		Action:             "configure",
		SandboxID:          view.ID,
		ObservedGeneration: settings.Generation,
		Env:                env,
		Policy:             settings.Policy,
	})
	if err != nil {
		return accepted, err
	}
	return service.operations.Wait(ctx, authorization.Principal{
		TenantID: view.TenantID,
	}, accepted.ID)
}
