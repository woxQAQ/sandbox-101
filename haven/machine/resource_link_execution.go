package machine

import (
	"context"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
)

// ResourceLinks coordinates child receipts in the parent's workflow. The parent
// supplies its domain child execution; locks, transactions and HEAD are shared.
type ResourceLinks struct {
	store     store.Workflow
	ctx       context.Context
	admission execution.Guard
}

func NewResourceLinks(state *store.State, admission execution.Guard, ctx context.Context) *ResourceLinks {
	return &ResourceLinks{
		store: store.Workflow{
			Database:   state.Database(),
			Checkpoint: state.Checkpoint,
		},
		ctx:       ctx,
		admission: admission,
	}
}

func (service *ResourceLinks) Complete(parent, role string, link ResourceLink) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	changed, err := CompleteResourceLink(service.ctx, tx, parent, role, link)
	if err != nil || !changed {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

func (service *ResourceLinks) Run(parent, role string, run func(ResourceLink) error) (ResourceLink, error) {
	link, status, err := LoadResourceLink(service.ctx, service.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status == "failed" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	if status == "succeeded" {
		return link, nil
	}
	runErr := run(link)
	if errors.Is(runErr, errs.ErrInterrupted) {
		return link, runErr
	}
	if err = service.Complete(parent, role, link); err != nil {
		return link, errors.Join(err, runErr)
	}
	_, status, err = LoadResourceLink(service.ctx, service.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status != "succeeded" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	return link, nil
}
