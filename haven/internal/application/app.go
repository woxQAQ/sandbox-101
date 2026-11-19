package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/guest"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/maintenance"
	"github.com/woxQAQ/haven/internal/publication"
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
	Context      context.Context
	StateDir     string
	Initialize   bool
	Backend      loom.Backend
	MaxMemoryMiB int64
	MaxVCPUs     int
	SafetyBytes  int64
	Guest        guest.Port
	Egress       Egress
	// EgressFactory binds callbacks before startup reconciliation can use portals.
	EgressFactory func(EgressCallbacks) (Egress, string, error)
	RootToken     string
	CAPEM         string
	MaxTTL        time.Duration
	PausedTTL     time.Duration
	Interval      time.Duration
	MaxWarm       int
	Fault         func(string) error
}

// App assembles the single store, shared execution mechanisms and business
// services. Components own their use cases; App owns final shutdown order.
type App struct {
	Authentication   *authorization.Authentication
	Availability     *execution.Availability
	Catalog          *sandbox.Catalog
	TemplateCatalog  *supply.TemplateCatalog
	Operations       *operation.Operations
	Delivery         *sandbox.OperationDelivery
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
	Management       *node.Management
	SandboxRequests  *sandbox.SandboxRequests
	Maintenance      *maintenance.Maintenance

	WarmClaims     *sandbox.WarmClaims
	ResourceLinks  *machine.ResourceLinks
	Commands       *sandbox.SandboxCommands
	Activations    *sandbox.Activations
	Images         *image.Images
	CaptureCleanup *machine.CaptureCleanup

	guards    *execution.Guards
	Ephemeral *ephemeral.Registry
	Options   Options
	State     *store.State
	ctx       context.Context
	Lifetime  *execution.Lifetime
	Admission execution.Guard
	mu        execution.Guard
	Workers   *execution.Workers
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
	guards := execution.NewGuards()
	admission := guards.Admission()
	admissionGate := execution.NewAdmissionGate(guards.Registration(), lifetime, state.Reviewed())
	snapshotProofs := machine.NewSnapshotProofs(state.Directory(), opts.Backend)
	resourceProofs := machine.NewResourceProofs(snapshotProofs, sandbox.ActivationHistory{})
	authentication := authorization.NewAuthentication(opts.RootToken, authorization.New(state.Database()), sandbox.NewCapabilityLookup(state.Database()), state.Sign)
	operations := operation.NewOperations(state.Database(), workers, state.Identity().Identity)
	registry := ephemeral.New()
	commandIdentity := operation.NewCommandIdentity(lifetime.Context(), operation.CommandIdentityDependencies{
		Operations: operation.New(state.Database()),
		View:       operations,
		Registry:   registry,
		Sign:       state.Sign,
	})
	service := &App{
		Authentication:  authentication,
		Availability:    execution.NewAvailability(guards.Registration()),
		mu:              guards.Registration(),
		Commands:        sandbox.NewSandboxCommands(opts.MaxTTL, commandIdentity, registry),
		Catalog:         sandbox.NewCatalog(state.Database()),
		TemplateCatalog: supply.NewTemplateCatalog(state.Database()),
		Operations:      operations,
		Delivery:        sandbox.NewOperationDelivery(state.Database(), operations, authentication),
		Tenants: authorization.NewTenants(authorization.New(state.Database()), authorization.TenantDependencies{
			Admission:  admission,
			Lifetime:   lifetime,
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		}),
		Admission: admission,
		Ephemeral: registry,
		Options:   opts,
		State:     state,
		ctx:       lifetime.Context(),
		Lifetime:  lifetime,
		guards:    guards,
		Workers:   workers,
	}
	service.Images = image.NewImages(state, image.ImageDependencies{
		Guards:  guards,
		Gate:    admissionGate,
		Workers: workers,
	}, image.ImageOptions{
		SafetyBytes: opts.SafetyBytes,
		Fault:       opts.Fault,
	})
	service.SnapshotDeletion = machine.NewSnapshotDeletion(state, machine.SnapshotDeletionDependencies{
		Proofs:   snapshotProofs,
		Guards:   guards,
		Lifetime: lifetime,
		Workers:  workers,
		Usage:    supply.ResourceUsage{},
	}, opts.Fault)
	service.CaptureCleanup = machine.NewCaptureCleanup(state, machine.CaptureCleanupDependencies{
		Gate:      admissionGate,
		Guards:    guards,
		Workers:   workers,
		Inspector: opts.Backend,
		Ownership: sandbox.OwnershipHistory{},
	}, opts.Fault)
	service.CaptureDiscard = machine.NewCaptureDiscard(state, machine.CaptureDiscardDependencies{
		Guards:    guards,
		Lifetime:  lifetime,
		Workers:   workers,
		Proofs:    resourceProofs,
		Inspector: opts.Backend,
		Ownership: sandbox.CaptureDiscardHistory{},
	}, opts.Fault)
	service.Captures = machine.NewCaptures(state, machine.CapturesDependencies{
		Guards:    guards,
		Lifetime:  lifetime,
		Workers:   workers,
		Backend:   opts.Backend,
		Ownership: sandbox.OwnershipHistory{},
	}, machine.CapturesOptions{
		SafetyBytes: opts.SafetyBytes,
		Fault:       opts.Fault,
	})
	service.Stops = machine.NewVMStops(state, machine.VMStopsDependencies{
		Guards:    guards,
		Lifetime:  lifetime,
		Workers:   workers,
		Proofs:    resourceProofs,
		Backend:   opts.Backend,
		Ownership: sandbox.StopHistory{},
	}, opts.Fault)
	service.DirectPolicies = machine.NewDirectPolicies(state, machine.DirectPoliciesDependencies{
		Guards:  guards,
		Proofs:  resourceProofs,
		Backend: opts.Backend,
	}, opts.Fault)
	service.DirectBindings = sandbox.NewDirectBindings(state.Database(), service.DirectPolicies)
	service.Deletion = machine.NewVMDeletion(state, machine.VMDeletionDependencies{
		Guards:    guards,
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
		Guards:    guards,
		Gate:      admissionGate,
		Workers:   workers,
		Proofs:    resourceProofs,
		Stops:     service.Stops,
		Captures:  service.CaptureDiscard,
		Deletion:  service.Deletion,
		Ownership: sandbox.SupplyCleanupHistory{},
	}, opts.Fault)
	service.GC = gc.NewCollector(state, gc.CollectorDependencies{
		Guards:     guards,
		Gate:       admissionGate,
		Workers:    workers,
		Proofs:     snapshotProofs,
		Operations: service.Operations,
		Images:     service.Images,
		Supply:     service.SupplyCleanup,
		Snapshots:  service.SnapshotDeletion,
		Raw:        service.CaptureCleanup,
	}, opts.Fault)
	audits := dataplane.NewAudits(state, admission, lifetime)
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
		Guards:    guards,
		Lifetime:  lifetime,
		Registry:  service.Ephemeral,
		Proofs:    resourceProofs,
		Committer: committer,
	}, sandbox.ActivationOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Egress:  service.Options.Egress,
		CAPEM:   service.Options.CAPEM,
		Epoch:   registry.Epoch(),
		Fault:   opts.Fault,
	})
	physicalProvisioning := machine.NewProvisioning(state, machine.ProvisioningDependencies{
		Context:   lifetime.Context(),
		Admission: admission,
		Proofs:    resourceProofs,
		Backend:   opts.Backend,
		Ownership: sandbox.ProvisionHistory{},
		Handoff:   sandbox.ProvisionHandoff{},
		Committer: committer,
	}, opts.Fault)
	service.Provisioning = sandbox.NewVMProvisioning(state, sandbox.ProvisionDependencies{
		Guards:      guards,
		Lifetime:    lifetime,
		Gate:        admissionGate,
		Workers:     workers,
		Registry:    service.Ephemeral,
		Proofs:      resourceProofs,
		Activations: service.Activations,
		Policies:    service.DirectPolicies,
		Committer:   committer,
		Physical:    physicalProvisioning,
	}, sandbox.ProvisionOptions{
		Backend: opts.Backend,
		Capacity: sandbox.ProvisionCapacity{
			MaxMemoryMiB: opts.MaxMemoryMiB,
			MaxVCPUs:     opts.MaxVCPUs,
			SafetyBytes:  opts.SafetyBytes,
		},
		Fault: opts.Fault,
	})
	service.ResourceLinks = machine.NewResourceLinks(state, admission, lifetime.Context())
	service.Supply = supply.NewSupply(state, supply.SupplyDependencies{
		Guards:       guards,
		Gate:         admissionGate,
		Workers:      workers,
		Proofs:       resourceProofs,
		Committer:    committer,
		Commands:     commandIdentity,
		Ownership:    sandbox.OwnershipHistory{},
		Availability: service.Availability,
		Provisioning: service.Provisioning,
		Captures:     service.Captures,
		Deletion:     service.Deletion,
		Policies:     service.DirectPolicies,
		Links:        service.ResourceLinks,
	}, supply.SupplyOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		MaxWarm: opts.MaxWarm,
		Fault:   opts.Fault,
	})
	service.WarmClaims = sandbox.NewWarmClaims(state, sandbox.WarmClaimDependencies{
		Guards:       guards,
		Gate:         admissionGate,
		Workers:      workers,
		Registry:     registry,
		Proofs:       resourceProofs,
		Committer:    committer,
		Activations:  service.Activations,
		Provisioning: service.Provisioning,
		Deletion:     service.Deletion,
		Policies:     service.DirectPolicies,
		Links:        service.ResourceLinks,
	}, opts.Fault)
	service.Lifecycle = sandbox.NewLifecycle(state, sandbox.LifecycleDependencies{
		Guards:         guards,
		Gate:           admissionGate,
		Workers:        workers,
		Registry:       registry,
		Proofs:         resourceProofs,
		Committer:      committer,
		Commands:       service.Commands,
		Activations:    service.Activations,
		WarmClaims:     service.WarmClaims,
		Provisioning:   service.Provisioning,
		Captures:       service.Captures,
		Stops:          service.Stops,
		Deletion:       service.Deletion,
		CaptureDiscard: service.CaptureDiscard,
		CaptureCleanup: service.CaptureCleanup,
		Policies:       service.DirectPolicies,
		Links:          service.ResourceLinks,
	}, sandbox.LifecycleOptions{
		Backend:   opts.Backend,
		MaxTTL:    opts.MaxTTL,
		PausedTTL: opts.PausedTTL,
		Fault:     opts.Fault,
	})
	service.Sandboxes = sandbox.NewSandboxes(state, sandbox.SandboxDependencies{
		Guards:       guards,
		Workers:      workers,
		Closing:      lifetime,
		Registry:     registry,
		History:      snapshotProofs,
		Committer:    committer,
		Commands:     service.Commands,
		Availability: service.Availability,
		Operations:   service.Operations,
		Activations:  service.Activations,
		WarmClaims:   service.WarmClaims,
		Provisioning: service.Provisioning,
		Policies:     service.DirectPolicies,
	}, sandbox.SandboxOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Fault:   opts.Fault,
	})
	service.Forks = sandbox.NewForks(state, sandbox.ForkDependencies{
		Guards:       guards,
		Gate:         admissionGate,
		Workers:      workers,
		Registry:     registry,
		History:      snapshotProofs,
		Committer:    committer,
		Commands:     service.Commands,
		Availability: service.Availability,
		Operations:   service.Operations,
		Provisioning: service.Provisioning,
		Captures:     service.Captures,
		Deletion:     service.Deletion,
		Lifecycle:    service.Lifecycle,
		Policies:     service.DirectPolicies,
		Links:        service.ResourceLinks,
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
		Guards:      guards,
		Lifetime:    lifetime,
		Registry:    registry,
		Commands:    service.Commands,
		Sandboxes:   service.Sandboxes,
		Lifecycle:   service.Lifecycle,
		Operations:  service.Operations,
		Activations: service.Activations,
	}, dataplane.PlaneOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Epoch:   registry.Epoch(),
	})
	service.Management = node.NewManagement(state, node.ManagementDependencies{
		Availability:  service.Availability,
		Operations:    service.Operations,
		Images:        service.Images,
		GC:            service.GC,
		SupplyCleanup: service.SupplyCleanup,
	})
	service.SandboxRequests = sandbox.NewSandboxRequests(sandbox.SandboxRequestDependencies{
		Sandboxes:  service.Sandboxes,
		Lifecycle:  service.Lifecycle,
		Supply:     service.Supply,
		Forks:      service.Forks,
		Operations: service.Operations,
	})
	policies := service.DirectPolicies
	deletion := service.Deletion
	service.Maintenance = maintenance.NewMaintenance(state, maintenance.MaintenanceDependencies{
		Lifetime:     lifetime,
		Workers:      workers,
		Availability: service.Availability,
		Lifecycle:    service.Lifecycle,
		Supply:       service.Supply,
		Dataplane:    service.Dataplane,
		Provisioning: service.Provisioning,
	}, []maintenance.ResumeWorkflow{
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
	if err := service.Maintenance.Reconcile(); err != nil {
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
		a.mu.Lock()
		dataConnections := a.Ephemeral.AllConnections()
		a.mu.Unlock()
		for _, connection := range dataConnections {
			_ = connection.Close()
		}
	}, func() error {
		var err error
		if a.Options.Egress != nil {
			err = a.Options.Egress.Close()
		}
		a.Admission.Lock()
		defer a.Admission.Unlock()
		if closer, ok := a.Options.Backend.(interface{ Close() error }); ok {
			err = errors.Join(err, closer.Close())
		}
		return errors.Join(err, a.State.Close())
	})
}
