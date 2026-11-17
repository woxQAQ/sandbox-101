package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/collection"
	"github.com/woxQAQ/haven/data"
	"github.com/woxQAQ/haven/execution"
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
	Authentication    *authorization.Authentication
	Availability      *execution.Availability
	Catalog           *sandbox.Catalog
	TemplateCatalog   *supply.TemplateCatalog
	Operations        *operation.Operations
	Delivery          *sandbox.OperationDelivery
	Tenants           *authorization.Tenants
	SnapshotDeletion  *machine.SnapshotDeletion
	CaptureDiscard    *machine.CaptureDiscard
	Captures          *machine.Captures
	VMStops           *machine.VMStops
	DirectPolicies    *machine.DirectPolicies
	DirectBindings    *sandbox.DirectBindings
	VMDeletion        *machine.VMDeletion
	SupplyCleanup     *supply.SupplyCleanup
	GarbageCollection *collection.GarbageCollection
	VMProvisioning    *sandbox.VMProvisioning
	Supply            *supply.Supply
	Lifecycle         *sandbox.Lifecycle
	Sandboxes         *sandbox.Sandboxes
	Forks             *sandbox.Forks
	Data              *data.Data
	Audits            *data.Audits
	ProxySockets      *data.ProxySockets
	Management        *node.Management
	SandboxRequests   *sandbox.SandboxRequests
	Maintenance       *maintenance.Maintenance

	WarmClaims     *sandbox.WarmClaims
	ResourceLinks  *machine.ResourceLinks
	Commands       *sandbox.SandboxCommands
	Activations    *sandbox.Activations
	Images         *image.Images
	CaptureCleanup *machine.CaptureCleanup

	guards    *execution.Guards
	Access    *access.Registry
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
	registry := access.New()
	commandIdentity := operation.NewCommandIdentity(lifetime.Context(), operation.New(state.Database()), operations, registry, state.Sign)
	service := &App{
		Authentication:  authentication,
		Availability:    execution.NewAvailability(guards.Registration()),
		mu:              guards.Registration(),
		Commands:        sandbox.NewSandboxCommands(opts.MaxTTL, commandIdentity, registry),
		Catalog:         sandbox.NewCatalog(state.Database()),
		TemplateCatalog: supply.NewTemplateCatalog(state.Database()),
		Operations:      operations,
		Delivery:        sandbox.NewOperationDelivery(state.Database(), operations, authentication),
		Tenants:         authorization.NewTenants(authorization.New(state.Database()), admission, lifetime, state.Reviewed(), state.Checkpoint),
		Admission:       admission,
		Access:          registry,
		Options:         opts,
		State:           state,
		ctx:             lifetime.Context(),
		Lifetime:        lifetime,
		guards:          guards,
		Workers:         workers,
	}
	service.Images = image.NewImages(state, guards, admissionGate, workers, image.ImageOptions{
		SafetyBytes: opts.SafetyBytes,
		Fault:       opts.Fault,
	})
	service.SnapshotDeletion = machine.NewSnapshotDeletion(state, snapshotProofs, guards, lifetime, workers, opts.Fault, supply.ResourceUsage{})
	service.CaptureCleanup = machine.NewCaptureCleanup(state, admissionGate, guards, workers, opts.Backend, opts.Fault, sandbox.OwnershipHistory{})
	service.CaptureDiscard = machine.NewCaptureDiscard(state, guards, lifetime, workers, resourceProofs, opts.Backend, opts.Fault, sandbox.CaptureDiscardHistory{})
	service.Captures = machine.NewCaptures(state, guards, lifetime, workers, opts.Backend, opts.SafetyBytes, opts.Fault, sandbox.OwnershipHistory{})
	service.VMStops = machine.NewVMStops(state, guards, lifetime, workers, resourceProofs, opts.Backend, opts.Fault, sandbox.StopHistory{})
	service.DirectPolicies = machine.NewDirectPolicies(state, guards, resourceProofs, opts.Backend, opts.Fault)
	service.DirectBindings = sandbox.NewDirectBindings(state.Database(), service.DirectPolicies)
	service.VMDeletion = machine.NewVMDeletion(state, guards, lifetime, workers, resourceProofs, opts.Backend, opts.Fault, committer, sandbox.DeletionHistory{}, sandbox.DeletionHandoff{}, supply.ResourceUsage{})
	service.SupplyCleanup = supply.NewSupplyCleanup(state, guards, admissionGate, workers, resourceProofs, service.VMStops, service.CaptureDiscard, service.VMDeletion, opts.Fault, sandbox.SupplyCleanupHistory{})
	service.GarbageCollection = collection.NewGarbageCollection(state, guards, admissionGate, workers, snapshotProofs, service.Operations, service.Images, service.SupplyCleanup, service.SnapshotDeletion, service.CaptureCleanup, opts.Fault)
	audits := data.NewAudits(state, admission, lifetime)
	proxySockets := data.NewProxySockets(state, lifetime.Context(), opts.Backend)
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
	service.Activations = sandbox.NewActivations(state, guards, lifetime, service.Access, resourceProofs, sandbox.ActivationOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Egress:  service.Options.Egress,
		CAPEM:   service.Options.CAPEM,
		Epoch:   registry.Epoch(),
		Fault:   opts.Fault,
	}, committer)
	physicalProvisioning := machine.NewProvisioning(state, lifetime.Context(), admission, resourceProofs, opts.Backend, sandbox.ProvisionHistory{}, sandbox.ProvisionHandoff{}, opts.Fault, committer)
	service.VMProvisioning = sandbox.NewVMProvisioning(state, guards, lifetime, admissionGate, workers, service.Access, resourceProofs, service.Activations, service.DirectPolicies, sandbox.ProvisionOptions{
		Backend: opts.Backend,
		Capacity: sandbox.ProvisionCapacity{
			MaxMemoryMiB: opts.MaxMemoryMiB,
			MaxVCPUs:     opts.MaxVCPUs,
			SafetyBytes:  opts.SafetyBytes,
		},
		Fault: opts.Fault,
	}, committer, physicalProvisioning)
	service.ResourceLinks = machine.NewResourceLinks(state, admission, lifetime.Context())
	service.Supply = supply.NewSupply(state, guards, admissionGate, workers, resourceProofs, supply.SupplyDependencies{
		Commands:     commandIdentity,
		Ownership:    sandbox.OwnershipHistory{},
		Availability: service.Availability,
		Provisioning: service.VMProvisioning,
		Captures:     service.Captures,
		Deletion:     service.VMDeletion,
		Policies:     service.DirectPolicies,
		Links:        service.ResourceLinks,
	}, supply.SupplyOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		MaxWarm: opts.MaxWarm,
		Fault:   opts.Fault,
	}, committer)
	service.WarmClaims = sandbox.NewWarmClaims(state, guards, admissionGate, workers, registry, resourceProofs, sandbox.WarmClaimDependencies{
		Activations:  service.Activations,
		Provisioning: service.VMProvisioning,
		Deletion:     service.VMDeletion,
		Policies:     service.DirectPolicies,
		Links:        service.ResourceLinks,
	}, opts.Fault, committer)
	service.Lifecycle = sandbox.NewLifecycle(state, guards, admissionGate, workers, registry, resourceProofs, sandbox.LifecycleDependencies{
		Commands:       service.Commands,
		Activations:    service.Activations,
		WarmClaims:     service.WarmClaims,
		Provisioning:   service.VMProvisioning,
		Captures:       service.Captures,
		Stops:          service.VMStops,
		Deletion:       service.VMDeletion,
		CaptureDiscard: service.CaptureDiscard,
		CaptureCleanup: service.CaptureCleanup,
		Policies:       service.DirectPolicies,
		Links:          service.ResourceLinks,
	}, sandbox.LifecycleOptions{
		Backend:   opts.Backend,
		MaxTTL:    opts.MaxTTL,
		PausedTTL: opts.PausedTTL,
		Fault:     opts.Fault,
	}, committer)
	service.Sandboxes = sandbox.NewSandboxes(state, guards, workers, lifetime, registry, snapshotProofs, sandbox.SandboxDependencies{
		Commands:     service.Commands,
		Availability: service.Availability,
		Operations:   service.Operations,
		Activations:  service.Activations,
		WarmClaims:   service.WarmClaims,
		Provisioning: service.VMProvisioning,
		Policies:     service.DirectPolicies,
	}, sandbox.SandboxOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Fault:   opts.Fault,
	}, committer)
	service.Forks = sandbox.NewForks(state, guards, admissionGate, workers, registry, snapshotProofs, sandbox.ForkDependencies{
		Commands:     service.Commands,
		Availability: service.Availability,
		Operations:   service.Operations,
		Provisioning: service.VMProvisioning,
		Captures:     service.Captures,
		Deletion:     service.VMDeletion,
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
	}, committer)
	service.Data = data.NewData(state, guards, lifetime, registry, data.DataDependencies{
		Commands:    service.Commands,
		Sandboxes:   service.Sandboxes,
		Lifecycle:   service.Lifecycle,
		Operations:  service.Operations,
		Activations: service.Activations,
	}, data.DataOptions{
		Backend: opts.Backend,
		Guest:   opts.Guest,
		Epoch:   registry.Epoch(),
	})
	service.Management = node.NewManagement(state, node.ManagementDependencies{
		Availability:  service.Availability,
		Operations:    service.Operations,
		Images:        service.Images,
		Collection:    service.GarbageCollection,
		SupplyCleanup: service.SupplyCleanup,
	})
	service.SandboxRequests = sandbox.NewSandboxRequests(service.Sandboxes, service.Lifecycle, service.Supply, service.Forks, service.Operations)
	policies := service.DirectPolicies
	deletion := service.VMDeletion
	service.Maintenance = maintenance.NewMaintenance(state, lifetime, workers, []maintenance.ResumeWorkflow{
		func(id string) (bool, error) {
			return policies.Resume(lifetime.Context(), id)
		},
		service.Images.Resume,
		service.GarbageCollection.Resume,
		service.SupplyCleanup.Resume,
		service.Forks.Resume,
		service.WarmClaims.Resume,
		service.Supply.Resume,
		service.CaptureCleanup.Resume,
		service.Lifecycle.Resume,
		service.SnapshotDeletion.Resume,
		service.CaptureDiscard.Resume,
		service.VMStops.Resume,
		deletion.ResumeDiscard,
		service.Captures.Resume,
		service.Activations.Resume,
		deletion.ResumeRelease,
	}, maintenance.MaintenanceDependencies{
		Availability: service.Availability,
		Lifecycle:    service.Lifecycle,
		Supply:       service.Supply,
		Data:         service.Data,
		Provisioning: service.VMProvisioning,
	})
	if err := service.Maintenance.Reconcile(); err != nil {
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		return nil, errors.Join(err, service.Close(closeCtx))
	}
	return service, nil
}

func (service *App) Close(ctx context.Context) error {
	return service.Lifetime.Close(ctx, func() {
		// Registration and the closing check share this guard. Once admission
		// closes, the snapshot includes every socket accepted before the barrier.
		service.mu.Lock()
		dataConnections := service.Access.AllConnections()
		service.mu.Unlock()
		for _, connection := range dataConnections {
			_ = connection.Close()
		}
	}, func() error {
		var err error
		if service.Options.Egress != nil {
			err = service.Options.Egress.Close()
		}
		service.Admission.Lock()
		defer service.Admission.Unlock()
		if closer, ok := service.Options.Backend.(interface{ Close() error }); ok {
			err = errors.Join(err, closer.Close())
		}
		return errors.Join(err, service.State.Close())
	})
}
