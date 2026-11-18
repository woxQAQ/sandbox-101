// Package dataplane owns the live side of Haven: readiness admission,
// tracked connection handoff, lease enforcement, audits and preview
// capabilities. Durable operation workflows live in the capability
// packages; this package only guards what is live right now.
package dataplane

import (
	"context"
	"net"

	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
)

type PlaneBackend interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
}

type PlaneGuest interface {
	Connect(context.Context, loom.Resource) (net.Conn, error)
	Forward(context.Context, loom.Resource, int) (net.Conn, error)
}

type PlaneGuards interface {
	Registration() execution.Guard
	Sandbox(string) execution.Guard
	Configuration(string) execution.Guard
	AutoResume(string) execution.Guard
}

type PlaneOptions struct {
	Backend PlaneBackend
	Guest   PlaneGuest
	Epoch   string
}

type PlaneDependencies struct {
	Guards      PlaneGuards
	Lifetime    *execution.Lifetime
	Registry    *ephemeral.Registry
	Commands    *sandbox.SandboxCommands
	Sandboxes   *sandbox.Sandboxes
	Lifecycle   *sandbox.Lifecycle
	Operations  *operation.Operations
	Activations *sandbox.Activations
}

// Plane owns live admission, tracked socket handoff, lease enforcement and preview
// capabilities. All setup and connections borrow the application's one Lifetime.
type Plane struct {
	store       store.Workflow
	ctx         context.Context
	lifetime    *execution.Lifetime
	guards      PlaneGuards
	mu          execution.Guard
	registry    *ephemeral.Registry
	epoch       string
	sign        func(...string) string
	catalog     *sandbox.Catalog
	commands    *sandbox.SandboxCommands
	sandboxes   *sandbox.Sandboxes
	lifecycle   *sandbox.Lifecycle
	operations  *operation.Operations
	activations *sandbox.Activations
	opts        PlaneOptions
}

func NewPlane(state *store.State, dependencies PlaneDependencies, options PlaneOptions) *Plane {
	return &Plane{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         dependencies.Lifetime.Context(),
		lifetime:    dependencies.Lifetime,
		guards:      dependencies.Guards,
		mu:          dependencies.Guards.Registration(),
		registry:    dependencies.Registry,
		epoch:       options.Epoch,
		sign:        state.Sign,
		catalog:     sandbox.NewCatalog(state.Database()),
		commands:    dependencies.Commands,
		sandboxes:   dependencies.Sandboxes,
		lifecycle:   dependencies.Lifecycle,
		operations:  dependencies.Operations,
		activations: dependencies.Activations,
		opts:        options,
	}
}
