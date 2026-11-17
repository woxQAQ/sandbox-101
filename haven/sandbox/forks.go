package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type ForkBackend interface {
	Check(context.Context, loom.Config) error
}

type ForkGuards interface {
	Admission() execution.Guard
	TenantRequest(string, string) execution.Guard
	ForkGroup(string) execution.Guard
}

type ForkOptions struct {
	Backend  ForkBackend
	Guest    ActivationGuest
	Capacity ProvisionCapacity
	Fault    func(string) error
}

type ForkDependencies struct {
	Commands     *SandboxCommands
	Availability *execution.Availability
	Operations   *operation.Operations
	Provisioning *VMProvisioning
	Captures     *machine.Captures
	Deletion     *machine.VMDeletion
	Lifecycle    *Lifecycle
	Policies     *machine.DirectPolicies
	Links        *machine.ResourceLinks
}

// Forks owns batch placement and compensation. Every target and reservation is
// committed together before branch effects; child publications remain immutable.
type Forks struct {
	store        store.Workflow
	ctx          context.Context
	admission    execution.Guard
	gate         execution.AdmissionGate
	guards       ForkGuards
	workers      *execution.Workers
	access       *access.Registry
	history      machine.SnapshotProofs
	catalog      *Catalog
	commands     *SandboxCommands
	availability *execution.Availability
	operations   *operation.Operations
	provisioning *VMProvisioning
	captures     *machine.Captures
	deletion     *machine.VMDeletion
	lifecycle    *Lifecycle
	policies     *machine.DirectPolicies
	links        *machine.ResourceLinks
	opts         ForkOptions
	committer    *operation.Committer
}

func NewForks(state *store.State, guards ForkGuards, gate execution.AdmissionGate, workers *execution.Workers, registry *access.Registry, history machine.SnapshotProofs, dependencies ForkDependencies, options ForkOptions, committer *operation.Committer) *Forks {
	return &Forks{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          workers.Context(),
		admission:    guards.Admission(),
		gate:         gate,
		guards:       guards,
		workers:      workers,
		access:       registry,
		history:      history,
		catalog:      NewCatalog(state.Database()),
		commands:     dependencies.Commands,
		availability: dependencies.Availability,
		operations:   dependencies.Operations,
		provisioning: dependencies.Provisioning,
		captures:     dependencies.Captures,
		deletion:     dependencies.Deletion,
		lifecycle:    dependencies.Lifecycle,
		policies:     dependencies.Policies,
		links:        dependencies.Links,
		opts:         options, committer: committer,
	}
}

func (service *Forks) fault(point string) error {
	if service.opts.Fault != nil {
		return service.opts.Fault(point)
	}
	return nil
}

func (service *Forks) existing(ctx context.Context, principal authorization.Principal, key string, admission *ClaimAdmission) (operation.Record, bool, error) {
	accepted, found, err := service.commands.Identity.Existing(ctx, principal, key, admission.MAC, admission.Secrets)
	if found && err == nil && !operation.Terminal(accepted.Status) {
		service.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (service *Forks) dispatch(id string) {
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

func (service *Forks) dispatchBranch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.policies.Resume(service.ctx, id); handled || err != nil {
			return err
		}
		return service.provisioning.Continue(id)
	})
}

func (service *Forks) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume fork", errs.ErrManualRecovery)
	}
	group, err := LoadFork(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.ForkGroup(id)
	guard.Lock()
	defer guard.Unlock()
	group, err = LoadFork(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	if operation.Terminal(group.Status) {
		return true, nil
	}
	return true, service.runForkGroup(group)
}

func (service *Forks) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return service.links.Run(parent, role, func(link machine.ResourceLink) error {
		if handled, err := service.policies.Resume(service.ctx, link.ChildID); handled || err != nil {
			return err
		}
		var handled bool
		var err error
		switch link.Kind {
		case "capture":
			handled, err = service.captures.Resume(link.ChildID)
		case "discard":
			handled, err = service.deletion.ResumeDiscard(link.ChildID)
		case "lifecycle":
			handled, err = service.lifecycle.Resume(link.ChildID)
		default:
			return fmt.Errorf("unsupported fork resource kind %q", link.Kind)
		}
		if !handled && err == nil {
			return fmt.Errorf("fork resource step missing")
		}
		return err
	})
}
