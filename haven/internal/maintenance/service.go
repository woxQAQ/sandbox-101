package maintenance

import (
	"context"
	"fmt"
	"slices"

	"github.com/woxQAQ/haven/dataplane"
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
	Lifetime     *execution.Lifetime
	Workers      *execution.Workers
	Availability *execution.Availability
	Lifecycle    *sandbox.Lifecycle
	Supply       *supply.Supply
	Dataplane    *dataplane.Plane
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
	plane        *dataplane.Plane
	provisioning *sandbox.VMProvisioning
}

func NewMaintenance(state *store.State, dependencies MaintenanceDependencies, routes []ResumeWorkflow) *Maintenance {
	return &Maintenance{
		operations:   operation.New(state.Database()),
		reviewed:     state.Reviewed(),
		lifetime:     dependencies.Lifetime,
		workers:      dependencies.Workers,
		routes:       slices.Clone(routes),
		availability: dependencies.Availability,
		lifecycle:    dependencies.Lifecycle,
		supply:       dependencies.Supply,
		plane:        dependencies.Dataplane,
		provisioning: dependencies.Provisioning,
	}
}

func (m *Maintenance) Dispatch(operationID string) {
	if m.reviewed {
		return
	}
	m.workers.Start(operationID, func() error {
		for _, resume := range m.routes {
			if handled, err := resume(operationID); handled || err != nil {
				return err
			}
		}
		return m.provisioning.Continue(operationID)
	})
}

// Reconcile reads only retained steps; resource directories never establish
// ownership and another operation service cannot supply missing receipts.
func (m *Maintenance) Reconcile() error {
	if m.reviewed {
		return nil
	}
	pending, err := m.operations.Unfinished(m.workers.Context())
	if err != nil {
		return err
	}
	for _, operationID := range pending {
		m.Dispatch(operationID)
	}
	return m.plane.ReconcileBindings()
}

func (m *Maintenance) Maintain(ctx context.Context) error {
	if m.reviewed {
		return nil
	}
	if !m.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	defer m.lifetime.End()
	if err := m.Reconcile(); err != nil {
		return err
	}
	if err := m.lifecycle.Reap(ctx); err != nil {
		return err
	}
	if m.availability.IsDraining() {
		return nil
	}
	return m.supply.RefillReady(ctx)
}
