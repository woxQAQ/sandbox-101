package haven

import (
	"context"
	"errors"
	"time"
)

// Maintain is called by the daemon scheduler. All effects still enter normal
// durable workflows; full pools and active backoff create no new intent.
func (service *Service) Maintain(ctx context.Context) error {
	if service.store.reviewed {
		return nil
	}
	if err := service.beginDataCall(); err != nil {
		return err
	}
	defer service.wg.Done()
	if err := service.Reconcile(); err != nil {
		return err
	}
	if err := service.Reap(ctx); err != nil {
		return err
	}
	service.mu.Lock()
	draining := service.draining
	service.mu.Unlock()
	if draining {
		return nil
	}
	templates, err := service.Templates(ctx, Principal{
		Root: true,
	})
	if err != nil {
		return err
	}
	var failures []error
	for _, template := range templates {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if template.TargetWarm < 1 || template.SnapshotID == "" {
			continue
		}
		var count int
		if err := service.store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM pool_slots WHERE template_id=?", template.ID).Scan(&count); err != nil {
			failures = append(failures, err)
			continue
		}
		if count >= template.TargetWarm {
			continue
		}
		blocked, err := refillBackoff(ctx, service.store.db, template.ID, time.Now().UTC())
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if blocked {
			continue
		}
		_, err = service.AdmitRefill(ctx, Principal{
			Root:     true,
			TenantID: template.TenantID,
		}, "refill:"+newID(""), Command{
			Action:     "refill",
			TemplateID: template.ID,
		})
		if err != nil && !errors.Is(err, ErrExhausted) && !errors.Is(err, ErrConflict) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
