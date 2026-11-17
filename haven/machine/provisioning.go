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

func NewProvisioning(state *store.State, ctx context.Context, admission execution.Guard, proofs ResourceProofs, backend ProvisionBackend, ownership ProvisionOwnershipReader, handoff ProvisionParticipant, fault func(string) error, committer *operation.Committer) *Provisioning {
	return &Provisioning{
		store:      store.Workflow{Database: state.Database(), Directory: state.Directory(), Reviewed: state.Reviewed(), Checkpoint: state.Checkpoint},
		ctx:        ctx,
		admission:  admission,
		proofs:     proofs,
		backend:    backend,
		ownership:  ownership,
		handoff:    handoff,
		faultPoint: fault,
		committer:  committer,
	}
}

func (service *Provisioning) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

// AdvanceLocked requires the original VM guard to remain held through the
// subsequent activation. It neither obtains nor releases that guard.
func (service *Provisioning) AdvanceLocked(step ResourceStep) error {
	if service.
		ownership ==
		nil || service.handoff == nil {
		return fmt.Errorf("%w: required Provisioning capability port unavailable",

			errs.ErrManualRecovery)
	}

	if err := ValidateResourceStep(service.store.Directory, step); err != nil {
		return err
	}
	currentMachine, instance, owner, err := LoadVM(service.ctx, service.store.Database, step)
	if err != nil {
		return err
	}
	if owner != step.OperationID || currentMachine.Deleting || instance != "" && !ExpectedCurrentInstance(step, instance) {
		return fmt.Errorf("resource step lost current VM ownership")
	}
	switch step.Action {
	case "clone", "restore":
		return service.runClone(step)
	case "cold_start":
		return service.runCold(step)
	default:
		return fmt.Errorf("unsupported resource action")
	}
}
