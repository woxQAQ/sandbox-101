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
	// Inspect observes the identified instance and reports uncertainty as an error.
	Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error)
}

type PlaneGuest interface {
	// Connect opens a raw Workload stream to the admitted instance. The caller owns the connection.
	Connect(ctx context.Context, resource loom.Resource) (net.Conn, error)
	// Forward negotiates a TCP forward through the Workload portal and returns the connected stream.
	Forward(ctx context.Context, resource loom.Resource, port int) (net.Conn, error)
}

type PlaneGuards interface {
	// ForServiceAdmission returns the lock shared by drain, shutdown and connection registration.
	ForServiceAdmission() execution.Guard
	// ForSandboxLifecycle returns the lock for lifecycle transitions and readiness checks across VM generations.
	ForSandboxLifecycle(sandboxID string) execution.Guard
	// ForSandboxConfiguration returns the lock shared by settings changes, revocation and final data admission.
	ForSandboxConfiguration(sandboxID string) execution.Guard
	// ForSandboxWakeup returns the lock that coalesces data requests waking the same paused sandbox.
	ForSandboxWakeup(sandboxID string) execution.Guard
}

type PlaneOptions struct {
	Backend PlaneBackend
	Guest   PlaneGuest
	Epoch   string
}

type PlaneDependencies struct {
	Guards          PlaneGuards
	Lifetime        *execution.Lifetime
	Registry        *ephemeral.Registry
	AdmissionPolicy *sandbox.AdmissionPolicy
	Sandboxes       *sandbox.Sandboxes
	Lifecycle       *sandbox.Lifecycle
	Operations      *operation.Monitor
	Activations     *sandbox.Activations
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
	policy      *sandbox.AdmissionPolicy
	sandboxes   *sandbox.Sandboxes
	lifecycle   *sandbox.Lifecycle
	operations  *operation.Monitor
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
		mu:          dependencies.Guards.ForServiceAdmission(),
		registry:    dependencies.Registry,
		epoch:       options.Epoch,
		sign:        state.Sign,
		catalog:     sandbox.NewCatalog(state.Database()),
		policy:      dependencies.AdmissionPolicy,
		sandboxes:   dependencies.Sandboxes,
		lifecycle:   dependencies.Lifecycle,
		operations:  dependencies.Operations,
		activations: dependencies.Activations,
		opts:        options,
	}
}
