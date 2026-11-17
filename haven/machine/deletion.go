package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type DeletionBackend interface {
	StopBackend

	Remove(context.Context, loom.Resource) error
}

// VMDeletion reclaims retained successful or failed generations using their
// original funding and deletion receipts before the final transaction retires
// physical responsibility. Release and discard retain distinct frozen proofs.
type VMDeletion struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	proofs     ResourceProofs
	backend    DeletionBackend
	faultPoint func(string) error
	committer  *operation.Committer
	ownership  DeletionOwnershipReader
	handoff    DeletionParticipant
	usage      VMUsageReader
}

type VMDeletionDependencies struct {
	Guards    VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Backend   DeletionBackend
	Committer *operation.Committer
	Ownership DeletionOwnershipReader
	Handoff   DeletionParticipant
	Usage     VMUsageReader
}

func NewVMDeletion(state *store.State, dependencies VMDeletionDependencies, fault func(string) error) *VMDeletion {
	return &VMDeletion{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Workers.Context(),
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		lifetime:   dependencies.Lifetime,
		workers:    dependencies.Workers,
		proofs:     dependencies.Proofs,
		backend:    dependencies.Backend,
		ownership:  dependencies.Ownership,
		handoff:    dependencies.Handoff,
		usage:      dependencies.Usage,
		faultPoint: fault,
		committer:  dependencies.Committer,
	}
}

func (service *VMDeletion) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *VMDeletion) validateStep(step ResourceStep) error {
	return ValidateResourceStep(service.store.Directory, step)
}

func (service *VMDeletion) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(service.ctx, service.backend, generations)
}

func (service *VMDeletion) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.ResumeDiscard(id); handled || err != nil {
			return err
		}
		if handled, err := service.ResumeRelease(id); handled || err != nil {
			return err
		}
		return fmt.Errorf("VM deletion lacks matching immutable step")
	})
}

// ResumeDiscard and ResumeRelease preserve their separate positions in the
// application's recovery order while sharing the original per-VM lock domain.
func (service *VMDeletion) ResumeDiscard(id string) (bool, error) {
	if service.
		ownership ==
		nil || service.
		handoff ==
		nil || service.usage == nil {
		return false, fmt.Errorf("%w: required VMDeletion capability port unavailable",

			errs.ErrManualRecovery)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume discard", errs.ErrManualRecovery)
	}
	step, err := LoadDiscard(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.VM(step.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadDiscard(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runDiscard(step)
}

func (service *VMDeletion) ResumeRelease(id string) (bool, error) {
	if service.
		ownership ==
		nil || service.
		handoff ==
		nil || service.usage == nil {
		return false, fmt.Errorf("%w: required VMDeletion capability port unavailable",

			errs.ErrManualRecovery)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume release", errs.ErrManualRecovery)
	}
	step, err := LoadResourceStep(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if step.Action != "release" {
		return false, nil
	}
	if err = service.validateStep(step); err != nil {
		return true, err
	}
	guard := service.guards.VM(step.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadResourceStep(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	if operation.Terminal(step.Status) {
		return true, nil
	}
	if err = service.validateStep(step); err != nil {
		return true, err
	}
	currentMachine, instance, owner, err := LoadVM(service.ctx, service.store.Database, step)
	if err != nil {
		return true, err
	}
	if owner != step.OperationID || !currentMachine.Deleting || instance != "" && !ExpectedCurrentInstance(step, instance) {
		return true, fmt.Errorf("resource step lost current VM ownership")
	}
	return true, service.runRelease(step)
}
