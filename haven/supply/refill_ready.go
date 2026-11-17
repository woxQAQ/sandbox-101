package supply

import (
	"context"
	"errors"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

// RefillReady considers verified templates and admits only missing warm supply.
// Full pools and active backoff create no intent; admission rechecks capacity.
func (service *Supply) RefillReady(ctx context.Context) error {
	templates, err := NewTemplateCatalog(service.store.Database).Templates(ctx, authorization.Principal{
		Root: true,
	})
	if err != nil {
		return err
	}
	var failures []error
	for _, candidate := range templates {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if candidate.TargetWarm < 1 || candidate.SnapshotID == "" {
			continue
		}
		count, err := dbgen.New(service.store.Database).CountTemplateReservations(ctx, candidate.ID)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if count >= int64(candidate.TargetWarm) {
			continue
		}
		blocked, err := RefillBackoff(ctx, service.store.Database, candidate.ID, time.Now().UTC())
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if blocked {
			continue
		}
		_, err = service.AdmitRefill(ctx, authorization.Principal{
			Root:     true,
			TenantID: candidate.TenantID,
		}, "refill:"+operation.NewID(""), operation.Command{
			Action:     "refill",
			TemplateID: candidate.ID,
		})
		if err != nil && !errors.Is(err, errs.ErrExhausted) && !errors.Is(err, errs.ErrConflict) {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
