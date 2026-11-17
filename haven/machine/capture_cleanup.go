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

// CaptureCleanup retires raw VMM output only after its original publication and
// stop proofs establish that the retained generation no longer runs.
type CaptureCleanup struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	gate       execution.AdmissionGate
	workers    *execution.Workers
	inspector  InstanceInspector
	ownership  SandboxOwnershipReader
	faultPoint func(string) error
}

type CaptureCleanupDependencies struct {
	Gate      execution.AdmissionGate
	Guards    VMGuards
	Workers   *execution.Workers
	Inspector InstanceInspector
	Ownership SandboxOwnershipReader
}

func NewCaptureCleanup(state *store.State, dependencies CaptureCleanupDependencies, fault func(string) error) *CaptureCleanup {
	return &CaptureCleanup{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Workers.Context(),
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		gate:       dependencies.Gate,
		workers:    dependencies.Workers,
		inspector:  dependencies.Inspector,
		ownership:  dependencies.Ownership,
		faultPoint: fault,
	}
}

func (service *CaptureCleanup) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *CaptureCleanup) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(service.ctx, service.inspector, generations)
}

func (service *CaptureCleanup) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("raw cleanup lacks matching immutable step")
		}
		return nil
	})
}

func (service *CaptureCleanup) Resume(id string) (bool, error) {
	if service.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required CaptureCleanup capability port unavailable",
				errs.
					ErrManualRecovery)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume raw cleanup", errs.ErrManualRecovery)
	}
	step, err := LoadCaptureCleanup(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.VM(step.Input.Capture.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadCaptureCleanup(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runCaptureCleanup(step)
}

func ValidateCaptureCleanup(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadCaptureCleanup(ctx, database, id)
	return err
}
