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

func NewVMProvisioning(state *store.State, guards ProvisionGuards, lifetime *execution.Lifetime, gate execution.AdmissionGate, workers *execution.Workers, registry *access.Registry, proofs machine.ResourceProofs, activations *Activations, policies *machine.DirectPolicies, options ProvisionOptions, committer *operation.Committer, physical *machine.Provisioning) *VMProvisioning {
	return &VMProvisioning{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         lifetime.Context(),
		lifetime:    lifetime,
		admission:   guards.Admission(),
		guards:      guards,
		proofs:      proofs,
		gate:        gate,
		workers:     workers,
		access:      registry,
		policies:    policies,
		backend:     options.Backend,
		activations: activations,
		physical:    physical,
		capacity:    options.Capacity,
		faultPoint:  options.Fault, committer: committer,
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
