// Package sandbox owns stable sandbox identity, settings and catalog facts.
package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// SandboxBackend contains the Loom effects needed during claim and configure.
type SandboxBackend interface {
	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config loom.Config) error
	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource loom.Resource, enabled bool) error
}

type SandboxDependencies struct {
	Backend                SandboxBackend
	Guest                  ActivationGuest
	Fault                  func(string) error
	StoreWriteMu           execution.Guard
	TenantIdempotencyLocks *operation.IdempotencyLocks
	SandboxConfigLocks     *execution.KeyedLocks[string]
	Workers                *execution.Workers
	Closing                authorization.ClosingReader
	Registry               *ephemeral.Registry
	SnapshotProofs         machine.SnapshotProofs
	Committer              *operation.Committer
	CommandIdentity        *operation.CommandIdentity
	AdmissionPolicy        *AdmissionPolicy
	Availability           *execution.Availability
	Operations             *operation.Monitor
	Activations            *Activations
	WarmClaims             *WarmClaims
	Provisioning           *Provisioning
	Policies               *machine.DirectPolicies
}

// Sandboxes owns logical sandbox claim and configuration admission. Resource
// execution and activation remain in their services with the same shared guards.
type Sandboxes struct {
	store                  store.Workflow
	ctx                    context.Context
	storeWriteMu           execution.Guard
	tenantIdempotencyLocks *operation.IdempotencyLocks
	sandboxConfigLocks     *execution.KeyedLocks[string]
	workers                *execution.Workers
	closing                authorization.ClosingReader
	registry               *ephemeral.Registry
	snapshotProofs         machine.SnapshotProofs
	identity               *operation.CommandIdentity
	policy                 *AdmissionPolicy
	availability           *execution.Availability
	operations             *operation.Monitor
	activations            *Activations
	warmClaims             *WarmClaims
	provisioning           *Provisioning
	policies               *machine.DirectPolicies
	backend                SandboxBackend
	guest                  ActivationGuest
	injectFault            func(string) error
	committer              *operation.Committer
}

func NewSandboxes(state *store.State, dependencies SandboxDependencies) *Sandboxes {
	return &Sandboxes{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:                    dependencies.Workers.Context(),
		storeWriteMu:           dependencies.StoreWriteMu,
		tenantIdempotencyLocks: dependencies.TenantIdempotencyLocks,
		sandboxConfigLocks:     dependencies.SandboxConfigLocks,
		workers:                dependencies.Workers,
		closing:                dependencies.Closing,
		registry:               dependencies.Registry,
		snapshotProofs:         dependencies.SnapshotProofs,
		identity:               dependencies.CommandIdentity,
		policy:                 dependencies.AdmissionPolicy,
		availability:           dependencies.Availability,
		operations:             dependencies.Operations,
		activations:            dependencies.Activations,
		warmClaims:             dependencies.WarmClaims,
		provisioning:           dependencies.Provisioning,
		policies:               dependencies.Policies,
		backend:                dependencies.Backend,
		guest:                  dependencies.Guest,
		injectFault:            dependencies.Fault,
		committer:              dependencies.Committer,
	}
}

func (sb *Sandboxes) fault(point string) error {
	if sb.injectFault != nil {
		return sb.injectFault(point)
	}
	return nil
}

func (sb *Sandboxes) existing(ctx context.Context, principal authorization.Principal, key string, admission *ClaimAdmission) (operation.State, bool, error) {
	accepted, found, err := sb.identity.Existing(ctx, principal, key, operation.CommandCredentials{
		MAC:     admission.MAC,
		Secrets: admission.Secrets,
	})
	if found && err == nil && !operation.Terminal(accepted.Status) {
		sb.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (sb *Sandboxes) dispatch(id string) {
	if sb.store.Reviewed {
		return
	}
	sb.workers.Start(id, func() error {
		if handled, err := sb.policies.Resume(sb.ctx, id); handled || err != nil {
			return err
		}
		if handled, err := sb.warmClaims.Resume(id); handled || err != nil {
			return err
		}
		if handled, err := sb.activations.Resume(id); handled || err != nil {
			return err
		}
		return sb.provisioning.Continue(id)
	})
}

func (sb *Sandboxes) checkConfigurationOpen() error {
	if sb.closing.Closing() || sb.store.Reviewed {
		return fmt.Errorf("haven is closing")
	}
	return nil
}
