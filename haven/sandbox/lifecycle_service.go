package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type LifecycleBackend interface {
	Check(context.Context, loom.Config) error
}

type LifecycleGuards interface {
	Admission() execution.Guard
	TenantRequest(string, string) execution.Guard
	Configuration(string) execution.Guard
	Sandbox(string) execution.Guard
	VM(string) execution.Guard
}

type LifecycleOptions struct {
	Backend   LifecycleBackend
	MaxTTL    time.Duration
	PausedTTL time.Duration
	Fault     func(string) error
}

type LifecycleDependencies struct {
	Commands       *SandboxCommands
	Activations    *Activations
	WarmClaims     *WarmClaims
	Provisioning   *VMProvisioning
	Captures       *machine.Captures
	Stops          *machine.VMStops
	Deletion       *machine.VMDeletion
	CaptureDiscard *machine.CaptureDiscard
	CaptureCleanup *machine.CaptureCleanup
	Policies       *machine.DirectPolicies
	Links          *machine.ResourceLinks
}

// Lifecycle owns sandbox pause/resume, rollback, release and checkpoint custody.
// Child effects keep their original services and guards; publication joins the
// original parent transaction before the same store checkpoint boundary.
type Lifecycle struct {
	store          store.Workflow
	ctx            context.Context
	admission      execution.Guard
	gate           execution.AdmissionGate
	guards         LifecycleGuards
	workers        *execution.Workers
	access         *access.Registry
	proofs         machine.ResourceProofs
	catalog        *Catalog
	commands       *SandboxCommands
	activations    *Activations
	warmClaims     *WarmClaims
	provisioning   *VMProvisioning
	captures       *machine.Captures
	stops          *machine.VMStops
	deletion       *machine.VMDeletion
	captureDiscard *machine.CaptureDiscard
	captureCleanup *machine.CaptureCleanup
	policies       *machine.DirectPolicies
	links          *machine.ResourceLinks
	opts           LifecycleOptions
	committer      *operation.Committer
}

func NewLifecycle(state *store.State, guards LifecycleGuards, gate execution.AdmissionGate, workers *execution.Workers, registry *access.Registry, proofs machine.ResourceProofs, dependencies LifecycleDependencies, options LifecycleOptions, committer *operation.Committer) *Lifecycle {
	return &Lifecycle{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:            workers.Context(),
		admission:      guards.Admission(),
		gate:           gate,
		guards:         guards,
		workers:        workers,
		access:         registry,
		proofs:         proofs,
		catalog:        NewCatalog(state.Database()),
		commands:       dependencies.Commands,
		activations:    dependencies.Activations,
		warmClaims:     dependencies.WarmClaims,
		provisioning:   dependencies.Provisioning,
		captures:       dependencies.Captures,
		stops:          dependencies.Stops,
		deletion:       dependencies.Deletion,
		captureDiscard: dependencies.CaptureDiscard,
		captureCleanup: dependencies.CaptureCleanup,
		policies:       dependencies.Policies,
		links:          dependencies.Links,
		opts:           options, committer: committer,
	}
}

func (service *Lifecycle) fault(point string) error {
	if service.opts.Fault != nil {
		return service.opts.Fault(point)
	}
	return nil
}

func (service *Lifecycle) existing(ctx context.Context, principal authorization.Principal, key string, admission *ClaimAdmission) (operation.Record, bool, error) {
	accepted, found, err := service.commands.Identity.Existing(ctx, principal, key, admission.MAC, admission.Secrets)
	if found && err == nil && !operation.Terminal(accepted.Status) {
		service.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (service *Lifecycle) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.policies.Resume(service.ctx, id); handled || err != nil {
			return err
		}
		_, err := service.Resume(id)
		return err
	})
}

func (service *Lifecycle) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume lifecycle", errs.ErrManualRecovery)
	}
	step, err := LoadLifecycle(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lock := service.guards.Sandbox(step.Input.Sandbox.ID)
	lock.Lock()
	defer lock.Unlock()
	step, err = LoadLifecycle(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runLifecycle(step)
}

func (service *Lifecycle) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return service.links.Run(parent, role, service.runResource)
}

func (service *Lifecycle) runResource(link machine.ResourceLink) error {
	if handled, err := service.policies.Resume(service.ctx, link.ChildID); handled || err != nil {
		return err
	}
	var handled bool
	var err error
	switch link.Kind {
	case "cold_start", "clone", "restore":
		return service.provisioning.Continue(link.ChildID)
	case "capture":
		handled, err = service.captures.Resume(link.ChildID)
	case "release":
		handled, err = service.deletion.ResumeRelease(link.ChildID)
	case "discard":
		handled, err = service.deletion.ResumeDiscard(link.ChildID)
	case "stop":
		handled, err = service.stops.Resume(link.ChildID)
	case "capture_discard":
		handled, err = service.captureDiscard.Resume(link.ChildID)
	case "capture_cleanup":
		handled, err = service.captureCleanup.Resume(link.ChildID)
	default:
		return fmt.Errorf("unsupported lifecycle resource kind %q", link.Kind)
	}
	if !handled && err == nil {
		return fmt.Errorf("lifecycle resource step missing")
	}
	return err
}
