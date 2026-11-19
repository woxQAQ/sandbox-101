// Package reconcile schedules recovery and periodic rounds through normal
// durable admissions. It shares worker custody and never owns resource effects.
package reconcile

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

type Dependencies struct {
	Lifetime     *execution.Lifetime
	Workers      *execution.Workers
	Availability *execution.Availability
	Lifecycle    *sandbox.Lifecycle
	Supply       *supply.Supply
	Dataplane    *dataplane.Plane
	Provisioning *sandbox.Provisioning
}

// Coordinator resumes unfinished workflows and runs periodic recovery, lease
// and warm-pool rounds. It shares worker custody and never owns resource effects.
type Coordinator struct {
	operations   *operation.Repository
	reviewed     bool
	lifetime     *execution.Lifetime
	workers      *execution.Workers
	routes       []ResumeWorkflow
	availability *execution.Availability
	lifecycle    *sandbox.Lifecycle
	supply       *supply.Supply
	plane        *dataplane.Plane
	provisioning *sandbox.Provisioning
}

func New(state *store.State, dependencies Dependencies, routes []ResumeWorkflow) *Coordinator {
	return &Coordinator{
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

// Dispatch resumes one retained operation. It applies after admission or
// whenever a caller learns an operation lost its worker, and is safe to repeat.
func (c *Coordinator) Dispatch(operationID string) {
	if c.reviewed {
		return
	}
	c.workers.Start(operationID, func() error {
		for _, resume := range c.routes {
			if handled, err := resume(operationID); handled || err != nil {
				return err
			}
		}
		return c.provisioning.Continue(operationID)
	})
}

// ResumePending resumes every durably unfinished operation and verifies
// dataplane bindings. It runs once at startup before serving, and again inside
// each periodic round. It reads only retained steps; resource directories never
// establish ownership and another operation service cannot supply missing receipts.
func (c *Coordinator) ResumePending() error {
	if c.reviewed {
		return nil
	}
	pending, err := c.operations.Unfinished(c.workers.Context())
	if err != nil {
		return err
	}
	for _, operationID := range pending {
		c.Dispatch(operationID)
	}
	return c.plane.ReconcileBindings()
}

// RunOnce executes one periodic round: resume pending work, reap expired
// leases, and refill the warm pool while admission allows. It applies only to
// the live serving loop, never to a reviewed (manually recovered) store.
func (c *Coordinator) RunOnce(ctx context.Context) error {
	if c.reviewed {
		return nil
	}
	if !c.lifetime.Begin() {
		return fmt.Errorf("%w: sandbox data admission is closed", errs.ErrConflict)
	}
	defer c.lifetime.End()
	if err := c.ResumePending(); err != nil {
		return err
	}
	if err := c.lifecycle.Reap(ctx); err != nil {
		return err
	}
	if c.availability.IsDraining() {
		return nil
	}
	return c.supply.RefillReady(ctx)
}
