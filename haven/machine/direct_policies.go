package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type DirectBackend interface {
	InstanceInspector

	Network(context.Context, loom.Resource, bool) error
}

type DirectPoliciesGuards interface {
	Admission() execution.Guard
	Configuration(string) execution.Guard
}

// DirectPolicies binds teardown to the original activation and instance. The
// effect caller retains its existing lifecycle/configuration guard; recovery
// acquires the same configuration guard as the original application runner.
type DirectPolicies struct {
	store      store.Workflow
	admission  execution.Guard
	guards     DirectPoliciesGuards
	proofs     ResourceProofs
	backend    DirectBackend
	faultPoint func(string) error
}

type DirectPoliciesDependencies struct {
	Guards  DirectPoliciesGuards
	Proofs  ResourceProofs
	Backend DirectBackend
}

func NewDirectPolicies(state *store.State, dependencies DirectPoliciesDependencies, fault func(string) error) *DirectPolicies {
	return &DirectPolicies{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		proofs:     dependencies.Proofs,
		backend:    dependencies.Backend,
		faultPoint: fault,
	}
}

func (service *DirectPolicies) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *DirectPolicies) Resume(ctx context.Context, id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume direct retirement", errs.ErrManualRecovery)
	}
	step, err := LoadDirectRetirement(ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if operation.Terminal(step.Status) || step.State.Phase != "pending" {
		return false, nil
	}
	guard := service.guards.Configuration(step.Input.Source.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	return true, service.retireDirect(ctx, id)
}
