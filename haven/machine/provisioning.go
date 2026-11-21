package machine

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// ResourceProvisioning advances physical generations under the coordinator's
// continuous VM guard. It owns no worker, shutdown state, or separate lock domain.
type Provisioning struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	proofs     ResourceProofs
	backend    ProvisionBackend
	ownership  ProvisionOwnershipReader
	handoff    ProvisionParticipant
	faultPoint func(string) error
	committer  *operation.Committer
}

type ProvisioningDependencies struct {
	Context   context.Context
	Admission execution.Guard
	Proofs    ResourceProofs
	Backend   ProvisionBackend
	Ownership ProvisionOwnershipReader
	Handoff   ProvisionParticipant
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
		ctx:        dependencies.Context,
		admission:  dependencies.Admission,
		proofs:     dependencies.Proofs,
		backend:    dependencies.Backend,
		ownership:  dependencies.Ownership,
		handoff:    dependencies.Handoff,
		faultPoint: fault,
		committer:  dependencies.Committer,
	}
}

func (p *Provisioning) fault(point string) error {
	if p.faultPoint != nil {
		return p.faultPoint(point)
	}
	return nil
}

// AdvanceLocked requires the original VM guard to remain held through the
// subsequent activation. It neither obtains nor releases that guard.
func (p *Provisioning) AdvanceLocked(step ResourceStep) error {
	if p.
		ownership ==
		nil || p.handoff == nil {
		return fmt.Errorf("%w: required Provisioning capability port unavailable",

			errs.ErrManualRecovery)
	}

	if err := ValidateResourceStep(p.store.Directory, step); err != nil {
		return err
	}
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
