package application

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/guest"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/publication"
	"github.com/woxQAQ/haven/internal/reconcile"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

// Options requires an explicit host-effects implementation. The daemon
// command supplies loom/host; fixtures inject their own owned runtime state.
type Options struct {
	// Context is the parent context for application workers. Nil uses context.Background().
	Context context.Context
	// StateDir is the private directory holding the database, node key, and managed resources.
	StateDir string
	// Initialize creates a new store in a missing or empty StateDir.
	// When false, Open requires an existing store.
	Initialize bool
	// Backend is the required runtime for host-side VM and resource operations.
	Backend loom.Backend
	// MaxMemoryMiB caps total VM memory reservations on this node and must be positive.
	MaxMemoryMiB int64
	// MaxVCPUs caps total vCPU reservations on this node and must be positive.
	MaxVCPUs int
	// SafetyBytes is the minimum free disk space to preserve when admitting
	// storage reservations. It must be nonnegative.
	SafetyBytes int64
	// Guest provides the guest-agent RPCs for activation, template preparation, and workload access.
	Guest guest.Agent
	// Egress supplies an existing egress manager that App.Close will close.
	// Set at most one of Egress and EgressFactory.
	Egress Egress
	// EgressFactory receives host callbacks and returns the egress manager and
	// its PEM-encoded CA certificate before startup reconciliation.
	EgressFactory func(EgressCallbacks) (Egress, string, error)
	// AdminToken is the bearer token for root API access. An empty value disables this authentication path.
	AdminToken string
	// CAPEM is the egress CA certificate sent to guests during activation.
	// EgressFactory supplies this value when configured.
	CAPEM string
	// MaxTTL caps a running sandbox lease. Nonpositive values default to one hour.
	MaxTTL time.Duration
	// PausedTTL sets the initial paused lease and caps each renewal. Nonpositive values default to 24 hours.
	PausedTTL time.Duration
	// Interval records the configured maintenance cadence. Open defaults it to one second when nonpositive.
	// The daemon schedules its ticker from config.Daemon.MaintenanceInterval.
	Interval time.Duration
	// MaxWarm caps the warm-pool target per template. Nonpositive values default to 32.
	MaxWarm int
	// Fault is an optional test hook that returns an injected error at a named workflow point.
	Fault func(string) error
}

// App assembles the single store, shared execution mechanisms and business
// services. Components own their use cases; App owns final shutdown order.
type App struct {
	Authentication   *authorization.Authentication
	Availability     *execution.Availability
	Catalog          *sandbox.Catalog
	TemplateCatalog  *supply.TemplateCatalog
	OperationMonitor *operation.Monitor
	OperationViews   *sandbox.OperationViews
	Tenants          *authorization.Tenants
	SnapshotDeletion *machine.SnapshotDeletion
	CaptureDiscard   *machine.CaptureDiscard
	Captures         *machine.Captures
	Stops            *machine.Stops
	DirectPolicies   *machine.DirectPolicies
	DirectBindings   *sandbox.DirectBindings
	Deletion         *machine.Deletion
	SupplyCleanup    *supply.SupplyCleanup
	GC               *gc.Collector
	Provisioning     *sandbox.Provisioning
	Supply           *supply.Supply
	Lifecycle        *sandbox.Lifecycle
	Sandboxes        *sandbox.Sandboxes
	Forks            *sandbox.Forks
	Dataplane        *dataplane.Plane
	Audits           *dataplane.Audits
	ProxySockets     *dataplane.ProxySockets
	Node             *node.Service
	CommandAdmission *sandbox.CommandAdmission
	Reconcile        *reconcile.Coordinator

	WarmClaims      *sandbox.WarmClaims
	ResourceLinks   *machine.ResourceLinks
	CommandIdentity *operation.CommandIdentity
	AdmissionPolicy *sandbox.AdmissionPolicy
	Activations     *sandbox.Activations
	Images          *image.Images
	CaptureCleanup  *machine.CaptureCleanup

	Ephemeral      *ephemeral.Registry
	Options        Options
	State          *store.State
	ctx            context.Context
	Lifetime       *execution.Lifetime
	StoreWriteMu   execution.Guard
	registrationMu execution.Guard
	Workers        *execution.Workers
}

func Open(opts Options) (*App, error) {
	committer := operation.NewCommitter(publication.Project)

	if opts.Backend == nil || opts.MaxMemoryMiB <= 0 || opts.MaxVCPUs <= 0 || opts.SafetyBytes < 0 {
		return nil, fmt.Errorf("%w: runtime and positive host budgets are required", errs.ErrInvalid)
	}
	if opts.MaxTTL <= 0 {
		opts.MaxTTL = time.Hour
	}
	if opts.PausedTTL <= 0 {
		opts.PausedTTL = 24 * time.Hour
	}
	if opts.Interval <= 0 {
		opts.Interval = time.Second
	}
	if opts.MaxWarm <= 0 {
		opts.MaxWarm = 32
	}
	if opts.Egress != nil && opts.EgressFactory != nil {
		return nil, fmt.Errorf("%w: egress and egress factory are mutually exclusive", errs.ErrInvalid)
	}
	state, err := openStore(opts.StateDir, opts.Initialize)
	if err != nil {
		return nil, err
	}
	parent := opts.Context
	if parent == nil {
		parent = context.Background()
	}
	lifetime := execution.NewLifetime(parent)
	workers := execution.NewWorkers(lifetime)
	storeWriteMu := new(sync.Mutex)
	registrationMu := new(sync.Mutex)
	imageArtifactsMu := new(sync.Mutex)
	vmGuards := machine.VMGuards{
		StoreWriteMu:   storeWriteMu,
		LifecycleLocks: new(execution.KeyedLocks[string]),
	}
	sandboxConfigLocks := new(execution.KeyedLocks[string])
	sandboxLifecycleLocks := new(execution.KeyedLocks[string])
	templateSupplyLocks := new(execution.KeyedLocks[string])
	tenantIdempotencyLocks := new(operation.IdempotencyLocks)
	nodeIdempotencyLocks := new(operation.IdempotencyLocks)
	admissionGate := execution.NewAdmissionGate(registrationMu, lifetime, state.Reviewed())
	snapshotProofs := machine.NewSnapshotProofs(state.Directory(), opts.Backend)
	resourceProofs := machine.NewResourceProofs(snapshotProofs, sandbox.ActivationHistory{})
	authentication := authorization.NewAuthentication(opts.AdminToken, authorization.New(state.Database()), sandbox.NewCapabilityLookup(state.Database()), state.Sign)
	operations := operation.NewMonitor(state.Database(), workers, state.Identity().Identity)
	registry := ephemeral.New()
	commandIdentity := operation.NewCommandIdentity(lifetime.Context(), operation.New(state.Database()), operations, registry, state.Sign)
	service := &App{
		Authentication:   authentication,
		Availability:     execution.NewAvailability(registrationMu),
		registrationMu:   registrationMu,
		CommandIdentity:  commandIdentity,
		AdmissionPolicy:  sandbox.NewAdmissionPolicy(opts.MaxTTL, registry),
		Catalog:          sandbox.NewCatalog(state.Database()),
		TemplateCatalog:  supply.NewTemplateCatalog(state.Database()),
		OperationMonitor: operations,
		OperationViews:   sandbox.NewOperationViews(state.Database(), operations, authentication),
		Tenants: authorization.NewTenants(authorization.New(state.Database()), authorization.TenantDependencies{
			StoreWriteMu: storeWriteMu,
			Lifetime:     lifetime,
			Reviewed:     state.Reviewed(),
			Checkpoint:   state.Checkpoint,
		}),
		StoreWriteMu: storeWriteMu,
		Ephemeral:    registry,
		Options:      opts,
		State:        state,
		ctx:          lifetime.Context(),
		Lifetime:     lifetime,
		Workers:      workers,
	}
	service.Images = image.NewImages(state, image.ImageDependencies{
		StoreWriteMu:         storeWriteMu,
		ImageArtifactsMu:     imageArtifactsMu,
		NodeIdempotencyLocks: nodeIdempotencyLocks,
		Gate:                 admissionGate,
		Workers:              workers,
	}, image.ImageOptions{
		SafetyBytes: opts.SafetyBytes,
		Fault:       opts.Fault,
	})
	service.SnapshotDeletion = machine.NewSnapshotDeletion(state, machine.SnapshotDeletionDependencies{
		Proofs:       snapshotProofs,
		StoreWriteMu: storeWriteMu,
		Lifetime:     lifetime,
		Workers:      workers,
		Usage:        supply.ResourceUsage{},
	}, opts.Fault)
	service.CaptureCleanup = machine.NewCaptureCleanup(state, machine.CaptureCleanupDependencies{
		Gate:      admissionGate,
		VMGuards:  vmGuards,
		Workers:   workers,
		Inspector: opts.Backend,
		Ownership: sandbox.OwnershipHistory{},
	}, opts.Fault)
	service.CaptureDiscard = machine.NewCaptureDiscard(state, machine.CaptureDiscardDependencies{
		VMGuards:  vmGuards,
		Lifetime:  lifetime,
		Workers:   workers,
		Proofs:    resourceProofs,
		Inspector: opts.Backend,
		Ownership: sandbox.CaptureDiscardHistory{},
	}, opts.Fault)
	service.Captures = machine.NewCaptures(state, machine.CapturesDependencies{
		VMGuards:  vmGuards,
		Lifetime:  lifetime,
		Workers:   workers,
		Backend:   opts.Backend,
		Ownership: sandbox.OwnershipHistory{},
	}, machine.CapturesOptions{
		SafetyBytes: opts.SafetyBytes,
		Fault:       opts.Fault,
	})
	service.Stops = machine.NewVMStops(state, machine.VMStopsDependencies{
		VMGuards:  vmGuards,
		Lifetime:  lifetime,
		Workers:   workers,
		Proofs:    resourceProofs,
		Backend:   opts.Backend,
		Ownership: sandbox.StopHistory{},
	}, opts.Fault)
	service.DirectPolicies = machine.NewDirectPolicies(state, machine.DirectPoliciesDependencies{
		StoreWriteMu:       storeWriteMu,
		SandboxConfigLocks: sandboxConfigLocks,
		Proofs:             resourceProofs,
		Backend:            opts.Backend,
	}, opts.Fault)
	service.DirectBindings = sandbox.NewDirectBindings(state.Database(), service.DirectPolicies)
	service.Deletion = machine.NewVMDeletion(state, machine.VMDeletionDependencies{
		VMGuards:  vmGuards,
		Lifetime:  lifetime,
		Workers:   workers,
		Proofs:    resourceProofs,
		Backend:   opts.Backend,
		Committer: committer,
		Ownership: sandbox.DeletionHistory{},
		Handoff:   sandbox.DeletionHandoff{},
		Usage:     supply.ResourceUsage{},
	}, opts.Fault)
	service.SupplyCleanup = supply.NewSupplyCleanup(state, supply.SupplyCleanupDependencies{
		StoreWriteMu:         storeWriteMu,
		TemplateSupplyLocks:  templateSupplyLocks,
		NodeIdempotencyLocks: nodeIdempotencyLocks,
		Gate:                 admissionGate,
		Workers:              workers,
		Proofs:               resourceProofs,
		Stops:                service.Stops,
		Captures:             service.CaptureDiscard,
		Deletion:             service.Deletion,
		Ownership:            sandbox.SupplyCleanupHistory{},
	}, opts.Fault)
	service.GC = gc.NewCollector(state, gc.CollectorDependencies{
		StoreWriteMu:         storeWriteMu,
		ImageArtifactsMu:     imageArtifactsMu,
		NodeIdempotencyLocks: nodeIdempotencyLocks,
		Gate:                 admissionGate,
		Workers:              workers,
		Proofs:               snapshotProofs,
		OperationMonitor:     service.OperationMonitor,
		Images:               service.Images,
		Supply:               service.SupplyCleanup,
		Snapshots:            service.SnapshotDeletion,
		Raw:                  service.CaptureCleanup,
	}, opts.Fault)
	audits := dataplane.NewAudits(state, storeWriteMu, lifetime)
	proxySockets := dataplane.NewProxySockets(state, lifetime.Context(), opts.Backend)
	service.Audits = audits
	service.ProxySockets = proxySockets
	if opts.EgressFactory != nil {
		bindings := service.DirectBindings
		service.Options.Egress, service.Options.CAPEM, err = opts.EgressFactory(EgressCallbacks{
			Audit:              audits.Audit,
			PrepareProxySocket: proxySockets.PrepareProxySocket,
			RecoverStaleSocket: proxySockets.RecoverStaleSocket,
			InstallDirect:      bindings.InstallDirect,
		})
		if err != nil {
			closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			return nil, errors.Join(err, service.Close(closeCtx))
		}
	}
	service.Activations = sandbox.NewActivations(state, sandbox.ActivationDependencies{
		VMGuards:           vmGuards,
		SandboxConfigLocks: sandboxConfigLocks,
		Lifetime:           lifetime,
		Registry:           service.Ephemeral,
		Proofs:             resourceProofs,
		Committer:          committer,
	}, sandbox.ActivationOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Egress:  service.Options.Egress,
		CAPEM:   service.Options.CAPEM,
		Epoch:   registry.Epoch(),
		Fault:   opts.Fault,
	})
	service.Provisioning = sandbox.NewVMProvisioning(state, sandbox.ProvisionDependencies{
		VMGuards:    vmGuards,
		Lifetime:    lifetime,
		Gate:        admissionGate,
		Workers:     workers,
		Registry:    service.Ephemeral,
		Proofs:      resourceProofs,
		Activations: service.Activations,
		Policies:    service.DirectPolicies,
		Committer:   committer,
		Ownership:   sandbox.ProvisionHistory{},
		Handoff:     sandbox.ProvisionHandoff{},
	}, sandbox.ProvisionOptions{
		Backend: opts.Backend,
		Capacity: sandbox.ProvisionCapacity{
			MaxMemoryMiB: opts.MaxMemoryMiB,
			MaxVCPUs:     opts.MaxVCPUs,
			SafetyBytes:  opts.SafetyBytes,
		},
		Fault: opts.Fault,
	})
	service.ResourceLinks = machine.NewResourceLinks(state, storeWriteMu, lifetime.Context())
	service.Supply = supply.NewSupply(state, supply.SupplyDependencies{
		VMGuards:               vmGuards,
		TemplateSupplyLocks:    templateSupplyLocks,
		TenantIdempotencyLocks: tenantIdempotencyLocks,
		Gate:                   admissionGate,
		Workers:                workers,
		Proofs:                 resourceProofs,
		Committer:              committer,
		Commands:               commandIdentity,
		Ownership:              sandbox.OwnershipHistory{},
		Availability:           service.Availability,
		Provisioning:           service.Provisioning,
		Captures:               service.Captures,
		Deletion:               service.Deletion,
		Policies:               service.DirectPolicies,
		Links:                  service.ResourceLinks,
	}, supply.SupplyOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		MaxWarm: opts.MaxWarm,
		Fault:   opts.Fault,
	})
	service.WarmClaims = sandbox.NewWarmClaims(state, sandbox.WarmClaimDependencies{
		StoreWriteMu:          storeWriteMu,
		SandboxLifecycleLocks: sandboxLifecycleLocks,
		SandboxConfigLocks:    sandboxConfigLocks,
		Gate:                  admissionGate,
		Workers:               workers,
		Registry:              registry,
		Proofs:                resourceProofs,
		Committer:             committer,
		Activations:           service.Activations,
		Provisioning:          service.Provisioning,
		Deletion:              service.Deletion,
		Policies:              service.DirectPolicies,
		Links:                 service.ResourceLinks,
	}, opts.Fault)
	service.Lifecycle = sandbox.NewLifecycle(state, sandbox.LifecycleDependencies{
		StoreWriteMu:           storeWriteMu,
		TenantIdempotencyLocks: tenantIdempotencyLocks,
		SandboxLifecycleLocks:  sandboxLifecycleLocks,
		SandboxConfigLocks:     sandboxConfigLocks,
		Gate:                   admissionGate,
		Workers:                workers,
		Registry:               registry,
		Proofs:                 resourceProofs,
		Committer:              committer,
		CommandIdentity:        commandIdentity,
		AdmissionPolicy:        service.AdmissionPolicy,
		Activations:            service.Activations,
		WarmClaims:             service.WarmClaims,
		Provisioning:           service.Provisioning,
		Captures:               service.Captures,
		Stops:                  service.Stops,
		Deletion:               service.Deletion,
		CaptureDiscard:         service.CaptureDiscard,
		CaptureCleanup:         service.CaptureCleanup,
		Policies:               service.DirectPolicies,
		Links:                  service.ResourceLinks,
	}, sandbox.LifecycleOptions{
		Backend:   opts.Backend,
		MaxTTL:    opts.MaxTTL,
		PausedTTL: opts.PausedTTL,
		Fault:     opts.Fault,
	})
	service.Sandboxes = sandbox.NewSandboxes(state, sandbox.SandboxDependencies{
		StoreWriteMu:           storeWriteMu,
		TenantIdempotencyLocks: tenantIdempotencyLocks,
		SandboxConfigLocks:     sandboxConfigLocks,
		Workers:                workers,
		Closing:                lifetime,
		Registry:               registry,
		SnapshotProofs:         snapshotProofs,
		Committer:              committer,
		CommandIdentity:        commandIdentity,
		AdmissionPolicy:        service.AdmissionPolicy,
		Availability:           service.Availability,
		OperationMonitor:       service.OperationMonitor,
		Activations:            service.Activations,
		WarmClaims:             service.WarmClaims,
		Provisioning:           service.Provisioning,
		Policies:               service.DirectPolicies,
		Backend:                opts.Backend,
		Guest:                  opts.Guest,
		Fault:                  opts.Fault,
	})
	service.Forks = sandbox.NewForks(state, sandbox.ForkDependencies{
		StoreWriteMu:           storeWriteMu,
		TenantIdempotencyLocks: tenantIdempotencyLocks,
		Gate:                   admissionGate,
		Workers:                workers,
		Registry:               registry,
		History:                snapshotProofs,
		Committer:              committer,
		CommandIdentity:        commandIdentity,
		AdmissionPolicy:        service.AdmissionPolicy,
		Availability:           service.Availability,
		OperationMonitor:       service.OperationMonitor,
		Provisioning:           service.Provisioning,
		Captures:               service.Captures,
		Deletion:               service.Deletion,
		Lifecycle:              service.Lifecycle,
		Policies:               service.DirectPolicies,
		Links:                  service.ResourceLinks,
	}, sandbox.ForkOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Capacity: sandbox.ProvisionCapacity{
			MaxMemoryMiB: opts.MaxMemoryMiB,
			MaxVCPUs:     opts.MaxVCPUs,
			SafetyBytes:  opts.SafetyBytes,
		},
		Fault: opts.Fault,
	})
	service.Dataplane = dataplane.NewPlane(state, dataplane.PlaneDependencies{
		RegistrationMu:        registrationMu,
		SandboxLifecycleLocks: sandboxLifecycleLocks,
		SandboxConfigLocks:    sandboxConfigLocks,
		Lifetime:              lifetime,
		Registry:              registry,
		AdmissionPolicy:       service.AdmissionPolicy,
		Sandboxes:             service.Sandboxes,
		Lifecycle:             service.Lifecycle,
		OperationMonitor:      service.OperationMonitor,
		Activations:           service.Activations,
	}, dataplane.PlaneOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Epoch:   registry.Epoch(),
	})
	service.Node = node.New(state, service.Availability)
	service.CommandAdmission = sandbox.NewCommandAdmission(service.Sandboxes, service.Lifecycle, service.Supply, service.Forks, service.OperationMonitor)
	policies := service.DirectPolicies
	deletion := service.Deletion
	service.Reconcile = reconcile.New(state, reconcile.Dependencies{
		Lifetime:     lifetime,
		Workers:      workers,
		Availability: service.Availability,
		Lifecycle:    service.Lifecycle,
		Supply:       service.Supply,
		Dataplane:    service.Dataplane,
		Provisioning: service.Provisioning,
	}, []reconcile.ResumeWorkflow{
		func(id string) (bool, error) {
			return policies.Resume(lifetime.Context(), id)
		},
		service.Images.Resume,
		service.GC.Resume,
		service.SupplyCleanup.Resume,
		service.Forks.Resume,
		service.WarmClaims.Resume,
		service.Supply.Resume,
		service.CaptureCleanup.Resume,
		service.Lifecycle.Resume,
		service.SnapshotDeletion.Resume,
		service.CaptureDiscard.Resume,
		service.Stops.Resume,
		deletion.ResumeDiscard,
		service.Captures.Resume,
		service.Activations.Resume,
		deletion.ResumeRelease,
	})
	if err := service.Reconcile.ResumePending(); err != nil {
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		return nil, errors.Join(err, service.Close(closeCtx))
	}
	return service, nil
}

func (a *App) Close(ctx context.Context) error {
	return a.Lifetime.Close(ctx, func() {
		// Registration and the closing check share this guard. Once admission
		// closes, the snapshot includes every socket accepted before the barrier.
		a.registrationMu.Lock()
		dataConnections := a.Ephemeral.AllConnections()
		a.registrationMu.Unlock()
		for _, connection := range dataConnections {
			_ = connection.Close()
		}
	}, func() error {
		var err error
		if a.Options.Egress != nil {
			err = a.Options.Egress.Close()
		}
		a.StoreWriteMu.Lock()
		defer a.StoreWriteMu.Unlock()
		if closer, ok := a.Options.Backend.(io.Closer); ok {
			err = errors.Join(err, closer.Close())
		}
		return errors.Join(err, a.State.Close())
	})
}
