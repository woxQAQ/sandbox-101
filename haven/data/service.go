package data

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

type DataBackend interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
}

type DataGuest interface {
	Connect(context.Context, loom.Resource) (net.Conn, error)
	Forward(context.Context, loom.Resource, int) (net.Conn, error)
}

type DataGuards interface {
	Registration() execution.Guard
	Sandbox(string) execution.Guard
	Configuration(string) execution.Guard
	AutoResume(string) execution.Guard
}

type DataOptions struct {
	Backend DataBackend
	Guest   DataGuest
	Epoch   string
}

type DataDependencies struct {
	Commands    *sandbox.SandboxCommands
	Sandboxes   *sandbox.Sandboxes
	Lifecycle   *sandbox.Lifecycle
	Operations  *operation.Operations
	Activations *sandbox.Activations
}

// Data owns live admission, tracked socket handoff, lease enforcement and preview
// capabilities. All setup and connections borrow the application's one Lifetime.
type Data struct {
	store       store.Workflow
	ctx         context.Context
	lifetime    *execution.Lifetime
	guards      DataGuards
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
	opts        DataOptions
}

func NewData(state *store.State, guards DataGuards, lifetime *execution.Lifetime, registry *access.Registry, dependencies DataDependencies, options DataOptions) *Data {
	return &Data{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         lifetime.Context(),
		lifetime:    lifetime,
		guards:      guards,
		mu:          guards.Registration(),
		access:      registry,
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
