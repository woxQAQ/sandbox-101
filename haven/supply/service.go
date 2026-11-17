package supply

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type SupplyBackend interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
	Network(context.Context, loom.Resource, bool) error

	Check(context.Context, loom.Config) error
}

type SupplyGuest interface {
	Info(context.Context, loom.Resource) error
	Activate(context.Context, loom.Resource, guest.Activation) error

	PrepareTemplate(context.Context, loom.Resource) error
	Exec(context.Context, loom.Resource, string, string) error
}

type SupplyGuards interface {
	Admission() execution.Guard
	TenantRequest(string, string) execution.Guard
	TemplateWork(string) execution.Guard
	VM(string) execution.Guard
}

type SupplyOptions struct {
	Backend SupplyBackend
	Guest   SupplyGuest
	MaxWarm int
	Fault   func(string) error
}

// SupplyDependencies names the business capabilities needed for template
// publication and warm pool production. Each service retains its own custody.
type SupplyDependencies struct {
	Commands     *operation.CommandIdentity
	Availability *execution.Availability
	Provisioning RawProvisioner
	Ownership    machine.SandboxOwnershipReader
	Captures     *machine.Captures
	Deletion     *machine.VMDeletion
	Policies     *machine.DirectPolicies
	Links        *machine.ResourceLinks
}

// Supply owns template put/build, refill, publication and failure retention.
// All effects and mutable execution mechanisms are borrowed at construction.
type Supply struct {
	store        store.Workflow
	ctx          context.Context
	admission    execution.Guard
	gate         execution.AdmissionGate
	guards       SupplyGuards
	workers      *execution.Workers
	proofs       machine.ResourceProofs
	commands     *operation.CommandIdentity
	availability *execution.Availability
	provisioning RawProvisioner
	ownership    machine.SandboxOwnershipReader
	captures     *machine.Captures
	deletion     *machine.VMDeletion
	policies     *machine.DirectPolicies
	links        *machine.ResourceLinks
	opts         SupplyOptions
	committer    *operation.Committer
}

func NewSupply(state *store.State, guards SupplyGuards, gate execution.AdmissionGate, workers *execution.Workers, proofs machine.ResourceProofs, dependencies SupplyDependencies, options SupplyOptions, committer *operation.Committer) *Supply {
	return &Supply{
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
		proofs:       proofs,
		commands:     dependencies.Commands,
		availability: dependencies.Availability,
		provisioning: dependencies.Provisioning,
		ownership:    dependencies.Ownership,
		captures:     dependencies.Captures,
		deletion:     dependencies.Deletion,
		policies:     dependencies.Policies,
		links:        dependencies.Links,
		opts:         options,
		committer:    committer,
	}
}

func (service *Supply) fault(point string) error {
	if service.opts.Fault != nil {
		return service.opts.Fault(point)
	}
	return nil
}

func (service *Supply) existing(ctx context.Context, principal authorization.Principal, key, mac string) (operation.Record, bool, error) {
	accepted, found, err := service.commands.Existing(ctx, principal, key, mac, nil)
	if found && err == nil && !operation.Terminal(accepted.Status) {
		service.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (service *Supply) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		_, err := service.Resume(id)
		return err
	})
}

func (service *Supply) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume supply", errs.ErrManualRecovery)
	}
	step, err := LoadSupply(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lock := service.guards.TemplateWork(step.Input.Template.ID)
	lock.Lock()
	defer lock.Unlock()
	step, err = LoadSupply(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runSupply(step)
}

func (service *Supply) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return service.links.Run(parent, role, service.runResource)
}

func (service *Supply) runResource(link machine.ResourceLink) error {
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
	default:
		return fmt.Errorf("unsupported supply resource kind %q", link.Kind)
	}
	if !handled && err == nil {
		return fmt.Errorf("supply resource step missing")
	}
	return err
}
