package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
)

// CaptureDiscard reclaims a failed capture while retaining the original VM and
// disk. Frozen stop, generation and funding proofs authorize each handoff.
type CaptureDiscard struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	proofs     ResourceProofs
	inspector  InstanceInspector
	ownership  CaptureDiscardOwnershipReader
	faultPoint func(string) error
}

type CaptureDiscardDependencies struct {
	Guards    VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Inspector InstanceInspector
	Ownership CaptureDiscardOwnershipReader
}

func NewCaptureDiscard(state *store.State, dependencies CaptureDiscardDependencies, fault func(string) error) *CaptureDiscard {
	return &CaptureDiscard{
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
		inspector:  dependencies.Inspector,
		ownership:  dependencies.Ownership,
		faultPoint: fault,
	}
}

func (service *CaptureDiscard) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *CaptureDiscard) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(service.ctx, service.inspector, generations)
}

func (service *CaptureDiscard) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("capture discard lacks matching immutable step")
		}
		return nil
	})
}

func (service *CaptureDiscard) Resume(id string) (bool, error) {
	if service.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required CaptureDiscard capability port unavailable",
				errs.
					ErrManualRecovery)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume capture discard", errs.ErrManualRecovery)
	}
	step, err := LoadCaptureDiscard(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.VM(step.Input.Source.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadCaptureDiscard(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runCaptureDiscard(step)
}

func ValidateCaptureDiscard(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadCaptureDiscard(ctx, database, id)
	return err
}
