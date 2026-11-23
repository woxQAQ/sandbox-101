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
	// Inspect observes the identified instance and reports uncertainty as an error.
	Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error)
	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource loom.Resource, enabled bool) error

	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config loom.Config) error
}

type SupplyGuest interface {
	// Info checks that the candidate's Workload portal answers the supported protocol.
	Info(ctx context.Context, resource loom.Resource) error
	// Activate delivers sandbox identity, environment and network settings through the Admin portal.
	Activate(ctx context.Context, resource loom.Resource, activation guest.Activation) error
	// PrepareTemplate requests guest quiescence before capturing a template.
	PrepareTemplate(ctx context.Context, resource loom.Resource) error
	// Exec executes or replays the keyed warmup and reports its recorded outcome.
	Exec(ctx context.Context, resource loom.Resource, execKey, command string) error
}

type SupplyOptions struct {
	Backend SupplyBackend
	Guest   SupplyGuest
	MaxWarm int
	Fault   func(string) error
}

// SupplyDependencies names the shared execution mechanisms and capabilities
// needed for template publication and warm pool production. Each service retains
// its own custody.
type SupplyDependencies struct {
	VMGuards               machine.VMGuards
	TemplateSupplyLocks    *execution.KeyedLocks[string]
	TenantIdempotencyLocks *operation.IdempotencyLocks
	Gate                   execution.AdmissionGate
	Workers                *execution.Workers
	Proofs                 machine.ResourceProofs
	Committer              *operation.Committer
	Commands               *operation.CommandIdentity
	Availability           *execution.Availability
	Provisioning           BareProvisioner
	Ownership              machine.SandboxOwnershipReader
	Captures               *machine.Captures
	Deletion               *machine.Deletion
	Policies               *machine.DirectPolicies
	Links                  *machine.ResourceLinks
}

// Supply owns template put/build, refill, publication and failure retention.
// All effects and mutable execution mechanisms are borrowed at construction.
type Supply struct {
	store                  store.Workflow
	ctx                    context.Context
	storeWriteMu           execution.Guard
	gate                   execution.AdmissionGate
	vmLifecycleLocks       *execution.KeyedLocks[string]
	templateSupplyLocks    *execution.KeyedLocks[string]
	tenantIdempotencyLocks *operation.IdempotencyLocks
	workers                *execution.Workers
	proofs                 machine.ResourceProofs
	commands               *operation.CommandIdentity
	availability           *execution.Availability
	provisioning           BareProvisioner
	ownership              machine.SandboxOwnershipReader
	captures               *machine.Captures
	deletion               *machine.Deletion
	policies               *machine.DirectPolicies
	links                  *machine.ResourceLinks
	opts                   SupplyOptions
	committer              *operation.Committer
}

func NewSupply(state *store.State, dependencies SupplyDependencies, options SupplyOptions) *Supply {
	return &Supply{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:                    dependencies.Workers.Context(),
		storeWriteMu:           dependencies.VMGuards.StoreWriteMu,
		gate:                   dependencies.Gate,
		vmLifecycleLocks:       dependencies.VMGuards.LifecycleLocks,
		templateSupplyLocks:    dependencies.TemplateSupplyLocks,
		tenantIdempotencyLocks: dependencies.TenantIdempotencyLocks,
		workers:                dependencies.Workers,
		proofs:                 dependencies.Proofs,
		commands:               dependencies.Commands,
		availability:           dependencies.Availability,
		provisioning:           dependencies.Provisioning,
		ownership:              dependencies.Ownership,
		captures:               dependencies.Captures,
		deletion:               dependencies.Deletion,
		policies:               dependencies.Policies,
		links:                  dependencies.Links,
		opts:                   options,
		committer:              dependencies.Committer,
	}
}

func (s *Supply) fault(point string) error {
	if s.opts.Fault != nil {
		return s.opts.Fault(point)
	}
	return nil
}

func (s *Supply) existing(ctx context.Context, principal authorization.Principal, key, mac string) (operation.State, bool, error) {
	accepted, found, err := s.commands.Existing(ctx, principal, key, operation.CommandCredentials{
		MAC: mac,
	})
	if found && err == nil && !operation.Terminal(accepted.Status) {
		s.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (s *Supply) dispatch(id string) {
	if s.store.Reviewed {
		return
	}
	s.workers.Start(id, func() error {
		_, err := s.Resume(id)
		return err
	})
}

func (s *Supply) Resume(id string) (bool, error) {
	if s.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume supply", errs.ErrManualRecovery)
	}
	step, err := LoadSupply(s.ctx, s.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	unlock := s.templateSupplyLocks.Lock(step.Input.Template.ID)
	defer unlock()
	step, err = LoadSupply(s.ctx, s.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, s.runSupply(step)
}

func (s *Supply) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return s.links.Run(parent, role, s.runResource)
}

func (s *Supply) runResource(link machine.ResourceLink) error {
	if handled, err := s.policies.ResumePendingRetirement(s.ctx, link.ChildID); handled || err != nil {
		return err
	}
	var handled bool
	var err error
	switch link.Kind {
	case "cold_start", "clone", "restore":
		return s.provisioning.Continue(link.ChildID)
	case "capture":
		handled, err = s.captures.Resume(link.ChildID)
	case "release":
		handled, err = s.deletion.ResumeRelease(link.ChildID)
	default:
		return fmt.Errorf("unsupported supply resource kind %q", link.Kind)
	}
	if !handled && err == nil {
		return fmt.Errorf("supply resource step missing")
	}
	return err
}
