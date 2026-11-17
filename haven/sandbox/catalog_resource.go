package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
)

func (s *Catalog) Unfenced(ctx context.Context, id string) error {
	_, err := LifecycleFence(ctx, s.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("%w: sandbox configuration is fenced", errs.ErrConflict)
}

// ResourceForData verifies persisted source and instance readiness for an
// authorized view. Callers separately check the lease and live runtime state.
func (s *Catalog) ResourceForData(ctx context.Context, view View) (machine.ResourceStep, error) {
	record, err := LoadSandbox(ctx, s.database, view.ID, view.TenantID)
	if err != nil {
		return machine.ResourceStep{}, err
	}
	source, err := machine.LoadResourceStep(ctx, s.database, record.SourceOperation)
	if err != nil {
		return source, err
	}
	resource, current, owner, err := machine.LoadVM(ctx, s.database, source)
	if err != nil {
		return source, err
	}
	if source.Status != "succeeded" || source.State.Witness != view.Instance || current != view.Instance || owner != "" || !resource.Verified || !resource.AgentReady || resource.Deleting {
		return source, fmt.Errorf("%w: sandbox instance is not verified ready", errs.ErrConflict)
	}
	return source, nil
}
