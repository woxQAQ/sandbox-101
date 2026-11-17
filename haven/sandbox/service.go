// Package sandbox owns stable sandbox identity, settings and catalog facts.
package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type SandboxBackend interface {
	Check(context.Context, loom.Config) error
	Network(context.Context, loom.Resource, bool) error
}

type SandboxGuards interface {
	Admission() execution.Guard
	TenantRequest(string, string) execution.Guard
	Configuration(string) execution.Guard
	VM(string) execution.Guard
}

type SandboxOptions struct {
	Backend SandboxBackend
	Guest   ActivationGuest
	Fault   func(string) error
}

type SandboxDependencies struct {
	Guards       SandboxGuards
	Workers      *execution.Workers
	Closing      interface{ Closing() bool }
	Registry     *access.Registry
	History      machine.SnapshotProofs
	Committer    *operation.Committer
	Commands     *SandboxCommands
	Availability *execution.Availability
	Operations   *operation.Operations
	Activations  *Activations
	WarmClaims   *WarmClaims
	Provisioning *VMProvisioning
	Policies     *machine.DirectPolicies
}

// Sandboxes owns logical sandbox claim and configuration admission. Resource
// execution and activation remain in their services with the same shared guards.
type Sandboxes struct {
	store        store.Workflow
	ctx          context.Context
	admission    execution.Guard
	guards       SandboxGuards
	workers      *execution.Workers
	closing      interface{ Closing() bool }
	access       *access.Registry
	history      machine.SnapshotProofs
	commands     *SandboxCommands
	availability *execution.Availability
	operations   *operation.Operations
	activations  *Activations
	warmClaims   *WarmClaims
	provisioning *VMProvisioning
	policies     *machine.DirectPolicies
	opts         SandboxOptions
	committer    *operation.Committer
}

func NewSandboxes(state *store.State, dependencies SandboxDependencies, options SandboxOptions) *Sandboxes {
	return &Sandboxes{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Workers.Context(),
		admission:    dependencies.Guards.Admission(),
		guards:       dependencies.Guards,
		workers:      dependencies.Workers,
		closing:      dependencies.Closing,
		access:       dependencies.Registry,
		history:      dependencies.History,
		commands:     dependencies.Commands,
		availability: dependencies.Availability,
		operations:   dependencies.Operations,
		activations:  dependencies.Activations,
		warmClaims:   dependencies.WarmClaims,
		provisioning: dependencies.Provisioning,
		policies:     dependencies.Policies,
		opts:         options,
		committer:    dependencies.Committer,
	}
}

func (service *Sandboxes) fault(point string) error {
	if service.opts.Fault != nil {
		return service.opts.Fault(point)
	}
	return nil
}

func (service *Sandboxes) existing(ctx context.Context, principal authorization.Principal, key string, admission *ClaimAdmission) (operation.Record, bool, error) {
	accepted, found, err := service.commands.Identity.Existing(ctx, principal, key, operation.CommandCredentials{
		MAC:     admission.MAC,
		Secrets: admission.Secrets,
	})
	if found && err == nil && !operation.Terminal(accepted.Status) {
		service.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (service *Sandboxes) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.policies.Resume(service.ctx, id); handled || err != nil {
			return err
		}
		if handled, err := service.warmClaims.Resume(id); handled || err != nil {
			return err
		}
		activation, err := LoadActivation(service.ctx, service.store.Database, id)
		if err == nil && activation.Input.ResourceOperation != id {
			source, lookup := machine.LoadResourceStep(service.ctx, service.store.Database, activation.Input.ResourceOperation)
			if lookup != nil {
				return lookup
			}
			guard := service.guards.VM(source.Candidate)
			guard.Lock()
			defer guard.Unlock()
			return service.activations.Continue(id)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return service.provisioning.Continue(id)
	})
}

func (service *Sandboxes) checkConfigurationOpen() error {
	if service.closing.Closing() || service.store.Reviewed {
		return fmt.Errorf("haven is closing")
	}
	return nil
}
