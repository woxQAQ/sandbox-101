package sandbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type WarmClaimGuards interface {
	Admission() execution.Guard
	Configuration(string) execution.Guard
	Sandbox(string) execution.Guard
	VM(string) execution.Guard
}

type WarmClaimDependencies struct {
	Guards       WarmClaimGuards
	Gate         execution.AdmissionGate
	Workers      *execution.Workers
	Registry     *access.Registry
	Proofs       machine.ResourceProofs
	Committer    *operation.Committer
	Activations  *Activations
	Provisioning *VMProvisioning
	Deletion     *machine.VMDeletion
	Policies     *machine.DirectPolicies
	Links        *machine.ResourceLinks
}

// WarmClaims selects original ready pool custody and owns warm activation,
// release/fallback, failure retention and supersession by sandbox release.
type WarmClaims struct {
	store        store.Workflow
	ctx          context.Context
	admission    execution.Guard
	gate         execution.AdmissionGate
	guards       WarmClaimGuards
	workers      *execution.Workers
	access       *access.Registry
	proofs       machine.ResourceProofs
	activations  *Activations
	provisioning *VMProvisioning
	deletion     *machine.VMDeletion
	policies     *machine.DirectPolicies
	links        *machine.ResourceLinks
	faultPoint   func(string) error
	committer    *operation.Committer
}

func NewWarmClaims(state *store.State, dependencies WarmClaimDependencies, fault func(string) error) *WarmClaims {
	return &WarmClaims{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Workers.Context(),
		admission:    dependencies.Guards.Admission(),
		gate:         dependencies.Gate,
		guards:       dependencies.Guards,
		workers:      dependencies.Workers,
		access:       dependencies.Registry,
		proofs:       dependencies.Proofs,
		activations:  dependencies.Activations,
		provisioning: dependencies.Provisioning,
		deletion:     dependencies.Deletion,
		policies:     dependencies.Policies,
		links:        dependencies.Links,
		faultPoint:   fault,
		committer:    dependencies.Committer,
	}
}

func (service *WarmClaims) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *WarmClaims) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.policies.Resume(service.ctx, id); handled || err != nil {
			return err
		}
		_, err := service.Resume(id)
		return err
	})
}

func (service *WarmClaims) Resume(id string) (bool, error) {
	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume warm claim", errs.ErrManualRecovery)
	}
	step, err := LoadWarmClaim(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lock := service.guards.Sandbox(step.Input.Pool.SandboxID)
	lock.Lock()
	defer lock.Unlock()
	step, err = LoadWarmClaim(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runWarmClaim(step)
}

func (service *WarmClaims) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return service.links.Run(parent, role, service.runResource)
}

func (service *WarmClaims) runResource(link machine.ResourceLink) error {
	if handled, err := service.policies.Resume(service.ctx, link.ChildID); handled || err != nil {
		return err
	}
	switch link.Kind {
	case "clone":
		return service.provisioning.Continue(link.ChildID)
	case "release":
		handled, err := service.deletion.ResumeRelease(link.ChildID)
		if !handled && err == nil {
			return fmt.Errorf("warm release step missing")
		}
		return err
	default:
		return fmt.Errorf("unsupported warm claim resource kind %q", link.Kind)
	}
}
