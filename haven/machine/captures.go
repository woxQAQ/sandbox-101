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

type CaptureBackend interface {
	InstanceInspector

	Compatibility() string
	Pause(context.Context, loom.Resource) error
	Resume(context.Context, loom.Resource) error
	Capture(context.Context, loom.Resource, string) error
}

type CapturesGuards interface {
	VMGuards

	Capture(context.Context) (func(), error)
}

// Captures freezes source custody and peak responsibility before pausing, then
// resumes the source before completing durable snapshot publication.
type Captures struct {
	store       store.Workflow
	ctx         context.Context
	admission   execution.Guard
	guards      CapturesGuards
	lifetime    *execution.Lifetime
	workers     *execution.Workers
	backend     CaptureBackend
	ownership   SandboxOwnershipReader
	safetyBytes int64
	faultPoint  func(string) error
}

type CapturesDependencies struct {
	Guards    CapturesGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Backend   CaptureBackend
	Ownership SandboxOwnershipReader
}

type CapturesOptions struct {
	SafetyBytes int64
	Fault       func(string) error
}

func NewCaptures(state *store.State, dependencies CapturesDependencies, options CapturesOptions) *Captures {
	return &Captures{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         dependencies.Workers.Context(),
		admission:   dependencies.Guards.Admission(),
		guards:      dependencies.Guards,
		lifetime:    dependencies.Lifetime,
		workers:     dependencies.Workers,
		backend:     dependencies.Backend,
		ownership:   dependencies.Ownership,
		safetyBytes: options.SafetyBytes,
		faultPoint:  options.Fault,
	}
}

func (service *Captures) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *Captures) validateStep(step ResourceStep) error {
	return ValidateResourceStep(service.store.Directory, step)
}

func (service *Captures) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("capture lacks matching immutable step")
		}
		return nil
	})
}

func (service *Captures) Resume(id string) (bool, error) {
	if service.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required Captures capability port unavailable",
				errs.ErrManualRecovery,
			)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume capture", errs.ErrManualRecovery)
	}
	step, err := LoadCapture(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.VM(step.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadCapture(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runCapture(step)
}
