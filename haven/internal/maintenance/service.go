package maintenance

import (
	"context"
	"fmt"
	"slices"

	"github.com/woxQAQ/haven/data"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

// ResumeWorkflow selects a frozen workflow and resumes it under its own guards.
// The ordered routing list contains method values bound to actual services.
type ResumeWorkflow func(string) (bool, error)

type MaintenanceDependencies struct {
	Availability *execution.Availability
	Lifecycle    *sandbox.Lifecycle
	Supply       *supply.Supply
	Data         *data.Data
	Provisioning *sandbox.VMProvisioning
}

// Maintenance schedules recovery and periodic work through normal durable
// admissions. It shares worker custody and never owns resource effects.
type Maintenance struct {
	operations   *operation.Repository
	reviewed     bool
	lifetime     *execution.Lifetime
	workers      *execution.Workers
	routes       []ResumeWorkflow
	availability *execution.Availability
	lifecycle    *sandbox.Lifecycle
	supply       *supply.Supply
	data         *data.Data
	provisioning *sandbox.VMProvisioning
}

func NewMaintenance(state *store.State, lifetime *execution.Lifetime, workers *execution.Workers, routes []ResumeWorkflow, dependencies MaintenanceDependencies) *Maintenance {
	return &Maintenance{
		operations:   operation.New(state.Database()),
		reviewed:     state.Reviewed(),
		lifetime:     lifetime,
		workers:      workers,
		routes:       slices.Clone(routes),
		availability: dependencies.Availability,
		lifecycle:    dependencies.Lifecycle,
		supply:       dependencies.Supply,
		data:         dependencies.Data,
		provisioning: dependencies.Provisioning,
	}
}

func (service *Maintenance) Dispatch(operationID string) {
	if service.reviewed {
		return
	}
	service.workers.Start(operationID, func() error {
		for _, resume := range service.routes {
			if handled, err := resume(operationID); handled || err != nil {
				return err
			}
		}
		return service.provisioning.Continue(operationID)
	})
}

// Reconcile reads only retained steps; resource directories never establish
// ownership and another operation service cannot supply missing receipts.
func (service *Maintenance) Reconcile() error {
	if service.reviewed {
		return nil
	}
	pending, err := service.operations.Unfinished(service.workers.Context())
	if err != nil {
		return err
	}
	for _, operationID := range pending {
		service.Dispatch(operationID)
	}
	return service.data.ReconcileBindings()
}

func (service *Maintenance) Maintain(ctx context.Context) error {
	if service.reviewed {
		return nil
	}
	if !service.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	defer service.lifetime.End()
	if err := service.Reconcile(); err != nil {
		return err
	}
	if err := service.lifecycle.Reap(ctx); err != nil {
		return err
	}
	if service.availability.IsDraining() {
		return nil
	}
	return service.supply.RefillReady(ctx)
}
