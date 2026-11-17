package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type ActivationBackend interface {
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
	Network(context.Context, loom.Resource, bool) error
}

type ActivationGuest interface {
	Info(context.Context, loom.Resource) error
	Activate(context.Context, loom.Resource, guest.Activation) error
}

type EgressBindings interface {
	Bind(context.Context, string, string, string, egress.Policy, map[string]egress.Secret) error
	Revoke(string) error
}

type ActivationGuards interface {
	VM(string) execution.Guard
	Admission() execution.Guard
	Configuration(string) execution.Guard
}

type ActivationOptions struct {
	Backend ActivationBackend
	Guest   ActivationGuest
	Egress  EgressBindings
	CAPEM   string
	Epoch   string
	Fault   func(string) error
}

// Activations establishes guest delivery under a frozen configuration. The
// parent runner retains its original VM guard and worker custody throughout.
type Activations struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     ActivationGuards
	proofs     machine.ResourceProofs
	backend    ActivationBackend
	guest      ActivationGuest
	egress     EgressBindings
	caPEM      string
	epoch      string
	access     *access.Registry
	sign       func(...string) string
	faultPoint func(string) error
	committer  *operation.Committer
}

func NewActivations(state *store.State, guards ActivationGuards, lifetime *execution.Lifetime, registry *access.Registry, proofs machine.ResourceProofs, options ActivationOptions, committer *operation.Committer) *Activations {
	return &Activations{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        lifetime.Context(),
		admission:  guards.Admission(),
		guards:     guards,
		proofs:     proofs,
		backend:    options.Backend,
		guest:      options.Guest,
		egress:     options.Egress,
		caPEM:      options.CAPEM,
		epoch:      options.Epoch,
		access:     registry,
		sign:       state.Sign,
		faultPoint: options.Fault, committer: committer,
	}
}

func (service *Activations) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}
