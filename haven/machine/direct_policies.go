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

	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource loom.Resource, enabled bool) error
}

type DirectPoliciesGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForSandboxConfiguration returns the lock shared by settings changes, revocation and final data admission.
	ForSandboxConfiguration(sandboxID string) execution.Guard
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
		admission:  dependencies.Guards.ForStoreMutation(),
		guards:     dependencies.Guards,
		proofs:     dependencies.Proofs,
		backend:    dependencies.Backend,
		faultPoint: fault,
	}
}

func (dp *DirectPolicies) fault(point string) error {
	if dp.faultPoint != nil {
		return dp.faultPoint(point)
	}
	return nil
}

func (dp *DirectPolicies) Resume(ctx context.Context, id string) (bool, error) {
	if dp.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume direct retirement", errs.ErrManualRecovery)
	}
	step, err := LoadDirectRetirement(ctx, dp.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if operation.Terminal(step.Status) || step.State.Phase != "pending" {
		return false, nil
	}
	guard := dp.guards.ForSandboxConfiguration(step.Input.Source.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	return true, dp.retireDirect(ctx, id)
}
