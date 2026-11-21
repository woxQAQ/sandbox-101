package machine

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// Provisioning advances physical generations and completes their sandbox handoff
// within the same VM lifecycle scope.
type Provisioning struct {
	store            store.Workflow
	ctx              context.Context
	storeWriteMu     execution.Guard
	vmLifecycleLocks *execution.KeyedLocks[string]
	proofs           ResourceProofs
	backend          ProvisionBackend
	ownership        ProvisionOwnershipReader
	handoff          ProvisionParticipant
	activate         func(vmID, operationID string) error
	faultPoint       func(string) error
	committer        *operation.Committer
}

type ProvisioningDependencies struct {
	Context   context.Context
	VMGuards  VMGuards
	Proofs    ResourceProofs
	Backend   ProvisionBackend
	Ownership ProvisionOwnershipReader
	Handoff   ProvisionParticipant
	Activate  func(vmID, operationID string) error
	Committer *operation.Committer
}

func NewProvisioning(state *store.State, dependencies ProvisioningDependencies, fault func(string) error) *Provisioning {
	return &Provisioning{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:              dependencies.Context,
		storeWriteMu:     dependencies.VMGuards.StoreWriteMu,
		vmLifecycleLocks: dependencies.VMGuards.LifecycleLocks,
		proofs:           dependencies.Proofs,
		backend:          dependencies.Backend,
		ownership:        dependencies.Ownership,
		handoff:          dependencies.Handoff,
		activate:         dependencies.Activate,
		faultPoint:       fault,
		committer:        dependencies.Committer,
	}
}

func (p *Provisioning) fault(point string) error {
	if p.faultPoint != nil {
		return p.faultPoint(point)
	}
	return nil
}

// Continue reloads the frozen operation inside its VM scope. Successful new
// and replayed generations finish activation before competing effects can enter.
func (p *Provisioning) Continue(operationID string) error {
	if p.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot provision VM", errs.ErrManualRecovery)
	}
	if p.ownership == nil || p.handoff == nil || p.activate == nil {
		return fmt.Errorf("%w: provisioning or activation unavailable", errs.ErrManualRecovery)
	}
	original, err := LoadResourceStep(p.ctx, p.store.Database, operationID)
	if err != nil {
		return err
	}
	return p.vmLifecycleLocks.With(original.Candidate, func() error {
		step, err := LoadResourceStep(p.ctx, p.store.Database, operationID)
		if err != nil {
			return err
		}
		if step.Candidate != original.Candidate || step.Hash != original.Hash {
			return fmt.Errorf("%w: provisioning identity changed", errs.ErrManualRecovery)
		}
		if err := ValidateResourceStep(p.store.Directory, step); err != nil {
			return err
		}
		if step.Status == "failed" {
			return nil
		}
		if step.Status != "succeeded" {
			if err := p.advance(step); err != nil {
				return err
			}
		}
		return p.activate(step.Candidate, operationID)
	})
}

func (p *Provisioning) advance(step ResourceStep) error {
	currentMachine, instance, owner, err := LoadVM(p.ctx, p.store.Database, step)
	if err != nil {
		return err
	}
	if owner != step.OperationID || currentMachine.Deleting || instance != "" && !ExpectedCurrentInstance(step, instance) {
		return fmt.Errorf("resource step lost current VM ownership")
	}
	switch step.Action {
	case "clone", "restore":
		return p.runClone(step)
	case "cold_start":
		return p.runCold(step)
	default:
		return fmt.Errorf("unsupported resource action")
	}
}
