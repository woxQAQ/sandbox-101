package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type ProvisionGuards interface {
	Admission() execution.Guard
	VM(string) execution.Guard
}

// ProvisionOptions freezes the runtime effects and node capacity at construction.
type ProvisionOptions struct {
	Backend  machine.ProvisionBackend
	Capacity ProvisionCapacity
	Fault    func(string) error
}

// ProvisionDependencies supplies the coordinator's shared execution owners and
// physical capabilities. Construction does not select workflow modes.
type ProvisionDependencies struct {
	Guards      ProvisionGuards
	Lifetime    *execution.Lifetime
	Gate        execution.AdmissionGate
	Workers     *execution.Workers
	Registry    *access.Registry
	Proofs      machine.ResourceProofs
	Activations *Activations
	Policies    *machine.DirectPolicies
	Committer   *operation.Committer
	Physical    *machine.Provisioning
}

// VMProvisioning admits and executes cold/clone/restore intents using the shared
// admission gate and workers. It owns no independent shutdown or runner state.
type VMProvisioning struct {
	store       store.Workflow
	ctx         context.Context
	lifetime    *execution.Lifetime
	admission   execution.Guard
	gate        execution.AdmissionGate
	workers     *execution.Workers
	access      *access.Registry
	policies    *machine.DirectPolicies
	guards      ProvisionGuards
	proofs      machine.ResourceProofs
	backend     machine.ProvisionBackend
	activations *Activations
	capacity    ProvisionCapacity
	faultPoint  func(string) error
	committer   *operation.Committer
	physical    *machine.Provisioning
}

func NewVMProvisioning(state *store.State, dependencies ProvisionDependencies, options ProvisionOptions) *VMProvisioning {
	return &VMProvisioning{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         dependencies.Lifetime.Context(),
		lifetime:    dependencies.Lifetime,
		admission:   dependencies.Guards.Admission(),
		guards:      dependencies.Guards,
		proofs:      dependencies.Proofs,
		gate:        dependencies.Gate,
		workers:     dependencies.Workers,
		access:      dependencies.Registry,
		policies:    dependencies.Policies,
		backend:     options.Backend,
		activations: dependencies.Activations,
		physical:    dependencies.Physical,
		capacity:    options.Capacity,
		faultPoint:  options.Fault,
		committer:   dependencies.Committer,
	}
}

func (service *VMProvisioning) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *VMProvisioning) Continue(id string) error {
	if service.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot provision VM", errs.ErrManualRecovery)
	}
	step, err := machine.LoadResourceStep(service.ctx, service.store.Database, id)
	if err != nil {
		return err
	}
	if err = machine.ValidateResourceStep(service.store.Directory, step); err != nil {
		return err
	}
	lock := service.guards.VM(step.Candidate)
	lock.Lock()
	defer lock.Unlock()
	step, err = machine.LoadResourceStep(service.ctx, service.store.Database, id)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		if step.Status == "succeeded" {
			return service.activations.Continue(id)
		}
		return nil
	}
	if err = service.physical.AdvanceLocked(step); err != nil {
		return err
	}
	return service.activations.Continue(id)
}
