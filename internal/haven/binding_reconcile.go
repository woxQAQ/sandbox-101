package haven

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// reconcileBindings keeps an already published none policy free of residual
// proxies and rebinds a restart without renewing its saved lease.
func (service *Service) reconcileBindings() error {
	rows, err := service.store.db.QueryContext(service.ctx, "SELECT s.id,s.tenant FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state='running' ORDER BY s.id")
	if err != nil {
		return err
	}
	var subjects [][2]string
	for rows.Next() {
		var subject [2]string
		if err = rows.Scan(&subject[0], &subject[1]); err != nil {
			break
		}
		subjects = append(subjects, subject)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, subject := range subjects {
		id, tenant := subject[0], subject[1]
		guard := service.vmLock("configuration:" + id)
		if !guard.TryLock() {
			continue
		}
		settings, lookup := loadSettings(service.ctx, service.store.db, id)
		if lookup == nil {
			var fence string
			fenceErr := service.store.db.QueryRowContext(service.ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", id).Scan(&fence)
			if fenceErr == nil {
				guard.Unlock()
				continue
			}
			if !errors.Is(fenceErr, sql.ErrNoRows) {
				lookup = fenceErr
			}
		}
		if lookup == nil && (settings.Policy.Mode == "none" || (len(settings.Secrets) > 0 && settings.ActiveEpoch != service.epoch) || !time.Now().Before(settings.Deadline)) {
			lookup = service.revokeSandbox(id)
		}
		rebind := lookup == nil && settings.ActiveEpoch != service.epoch && len(settings.Secrets) == 0 && time.Now().Before(settings.Deadline) && service.opts.Guest != nil
		guard.Unlock()
		if rebind {
			bindingCtx, cancel := context.WithTimeout(service.ctx, 10*time.Second)
			_, lookup = service.waitRestartBinding(bindingCtx, Sandbox{ID: id, TenantID: tenant}, settings)
			cancel()
		}
		if lookup != nil {
			service.mu.Lock()
			service.lastErrors["binding-maintenance:"+id] = lookup
			service.mu.Unlock()
		}
	}
	return nil
}

func (service *Service) waitRestartBinding(ctx context.Context, sandbox Sandbox, settings sandboxSettings) (Operation, error) {
	env := map[string]EnvVar{}
	for name, value := range settings.Env {
		env[name] = EnvVar{Value: value}
	}
	operation, err := service.AdmitConfigure(ctx, Principal{TenantID: sandbox.TenantID}, "rebind:"+service.epoch+":"+sandbox.ID+":"+settings.Generation, Command{Action: "configure", SandboxID: sandbox.ID, ObservedGeneration: settings.Generation, Env: env, Policy: settings.Policy})
	if err != nil {
		return operation, err
	}
	return service.Wait(ctx, Principal{TenantID: sandbox.TenantID}, operation.ID)
}
