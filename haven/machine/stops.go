package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/store"
)

type StopBackend interface {
	InstanceInspector

	Stop(context.Context, loom.Resource) error
	Network(context.Context, loom.Resource, bool) error
}

// VMStops retains the original generation and funding proof while confirming
// process exit and network removal before publishing the stopped state.
type VMStops struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	proofs     ResourceProofs
	backend    StopBackend
	ownership  StopOwnershipReader
	faultPoint func(string) error
}

type VMStopsDependencies struct {
	Guards    VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Backend   StopBackend
	Ownership StopOwnershipReader
}

func NewVMStops(state *store.State, dependencies VMStopsDependencies, fault func(string) error) *VMStops {
	return &VMStops{
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
		faultPoint: fault,
	}
}

func (service *VMStops) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *VMStops) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("stop lacks matching immutable step")
		}
		return nil
	})
}

func (service *VMStops) Resume(id string) (bool, error) {
	if service.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required VMStops capability port unavailable",
				errs.ErrManualRecovery,
			)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume stop", errs.ErrManualRecovery)
	}
	step, err := LoadStop(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	source, err := LoadResourceStep(service.ctx, service.store.Database, step.Input.SourceOperation)
	if err != nil {
		return true, err
	}
	guard := service.guards.VM(source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadStop(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runStop(step)
}
