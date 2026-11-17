// Package dataplane owns the live side of Haven: readiness admission,
// tracked connection handoff, lease enforcement, audits and preview
// capabilities. Durable operation workflows live in the capability
// packages; this package only guards what is live right now.
package dataplane

import (
	"context"
	"net"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
)

type Backend interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
}

type Guest interface {
	Connect(context.Context, loom.Resource) (net.Conn, error)
	Forward(context.Context, loom.Resource, int) (net.Conn, error)
}

type Guards interface {
	Registration() execution.Guard
	Sandbox(string) execution.Guard
	Configuration(string) execution.Guard
	AutoResume(string) execution.Guard
}

type Options struct {
	Backend Backend
	Guest   Guest
	Epoch   string
}

type Dependencies struct {
	Guards      Guards
	Lifetime    *execution.Lifetime
	Registry    *access.Registry
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
	guards      Guards
	mu          execution.Guard
	access      *access.Registry
	epoch       string
	sign        func(...string) string
	catalog     *sandbox.Catalog
	commands    *sandbox.SandboxCommands
	sandboxes   *sandbox.Sandboxes
	lifecycle   *sandbox.Lifecycle
	operations  *operation.Operations
	activations *sandbox.Activations
	opts        Options
}

func NewPlane(state *store.State, dependencies Dependencies, options Options) *Plane {
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
		access:      dependencies.Registry,
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
