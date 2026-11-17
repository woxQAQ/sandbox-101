package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

type ProvisionGuards interface {
	Admission() execution.Guard
	VM(string) execution.Guard
}

// ProvisionOptions freezes the runtime effects and node capacity at construction.
type ProvisionOptions struct {
	Backend  machine.ProvisionBackend
	Capacity ProvisionCapacity
	Fault    func(string) error
}

// ProvisionDependencies supplies the coordinator's shared execution owners and
// physical capabilities. Construction does not select workflow modes.
type ProvisionDependencies struct {
	Guards      ProvisionGuards
	Lifetime    *execution.Lifetime
	Gate        execution.AdmissionGate
	Workers     *execution.Workers
	Registry    *ephemeral.Registry
	Proofs      machine.ResourceProofs
	Activations *Activations
	Policies    *machine.DirectPolicies
	Committer   *operation.Committer
	Physical    *machine.Provisioning
}

// VMProvisioning admits and executes cold/clone/restore intents using the shared
// admission gate and workers. It owns no independent shutdown or runner state.
type VMProvisioning struct {
	store       store.Workflow
	ctx         context.Context
	lifetime    *execution.Lifetime
	admission   execution.Guard
	gate        execution.AdmissionGate
	workers     *execution.Workers
	registry    *ephemeral.Registry
	policies    *machine.DirectPolicies
	guards      ProvisionGuards
	proofs      machine.ResourceProofs
	backend     machine.ProvisionBackend
	activations *Activations
	capacity    ProvisionCapacity
	faultPoint  func(string) error
	committer   *operation.Committer
	physical    *machine.Provisioning
}

func NewVMProvisioning(state *store.State, dependencies ProvisionDependencies, options ProvisionOptions) *VMProvisioning {
	return &VMProvisioning{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:         dependencies.Lifetime.Context(),
		lifetime:    dependencies.Lifetime,
		admission:   dependencies.Guards.Admission(),
		guards:      dependencies.Guards,
		proofs:      dependencies.Proofs,
		gate:        dependencies.Gate,
		workers:     dependencies.Workers,
		registry:    dependencies.Registry,
		policies:    dependencies.Policies,
		backend:     options.Backend,
		activations: dependencies.Activations,
		physical:    dependencies.Physical,
		capacity:    options.Capacity,
		faultPoint:  options.Fault,
		committer:   dependencies.Committer,
	}
}

func (p *VMProvisioning) fault(point string) error {
	if p.faultPoint != nil {
		return p.faultPoint(point)
	}
	return nil
}

func (p *VMProvisioning) Continue(id string) error {
	if p.store.Reviewed {
		return fmt.Errorf("%w: reviewed rollback cannot provision VM", errs.ErrManualRecovery)
	}
	step, err := machine.LoadResourceStep(p.ctx, p.store.Database, id)
	if err != nil {
		return err
	}
	if err = machine.ValidateResourceStep(p.store.Directory, step); err != nil {
		return err
	}
	lock := p.guards.VM(step.Candidate)
	lock.Lock()
	defer lock.Unlock()
	step, err = machine.LoadResourceStep(p.ctx, p.store.Database, id)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) {
		if step.Status == "succeeded" {
			return p.activations.Continue(id)
		}
		return nil
	}
	if err = p.physical.AdvanceLocked(step); err != nil {
		return err
	}
	return p.activations.Continue(id)
}

// ClaimAdmission carries the sanitized command/settings and ephemeral host
// credentials. Only the explicit frozen inputs and credential MAC are persisted.
type ClaimAdmission struct {
	Command  operation.Command
	Settings Settings
	MAC      string
	Secrets  map[string]egress.Secret
}

// ProvisionAdmission joins one preflight request to its business transaction
// participants. Supply's raw port cannot supply claim credentials or placement
// on an existing sandbox.
type ProvisionAdmission struct {
	Request supply.ProvisionRequest
	Claim   *ClaimAdmission
	Target  *ProvisionTarget
}

type ProvisionTarget struct {
	Activation    *ClaimAdmission
	Identity      *ownership.Birth
	SlotHolder    string
	Budget        *quota.SlotBudget
	NewSandboxID  string
	Sandbox       ownership.Sandbox
	SlotOperation string
	Parent        machine.ResourceParent
}

// ProvisionCapacity is the node's fixed admission budget for VM reservations.
type ProvisionCapacity struct {
	MaxMemoryMiB int64
	MaxVCPUs     int
	SafetyBytes  int64
}

type provisionalTemplateParent interface {
	ProvisioningTemplate() (supply.TemplateRecord, bool)
}

func provisionalTemplate(parent machine.ResourceParent) (supply.TemplateRecord, bool) {
	if provider, ok := parent.(provisionalTemplateParent); ok {
		return provider.ProvisioningTemplate()
	}
	return supply.TemplateRecord{}, false
}

// ProvisionSlotClaim binds the current sandbox and restore source to the
// original funded slot holder. It is verified in the allocation transaction.
type ProvisionSlotClaim struct {
	Sandbox ownership.Sandbox
	Source  machine.SnapshotSource
	Holder  string
}

type ProvisionTargetValidator interface {
	ValidateProvisionTarget(context.Context, *sql.Tx, ProvisionSlotClaim) (*machine.QuotaDestination, error)
}

func (admission *LifecycleAdmission) ValidateProvisionTarget(_ context.Context, _ *sql.Tx, claim ProvisionSlotClaim) (*machine.QuotaDestination, error) {
	record, source, holder := claim.Sandbox, claim.Source, claim.Holder
	if admission == nil || admission.Input.Command.Action != "resume" || record.State != "paused" || record.VMID != "" || record.Instance != "" || record.SnapshotID != source.Snapshot.ID || holder != "sleep:"+record.ID {
		return nil, fmt.Errorf("%w: resume target differs from frozen sleep responsibility", errs.ErrConflict)
	}
	return &machine.QuotaDestination{
		OperationID: admission.Parent.ID,
		Kind:        "sleep",
		ID:          record.ID,
		SnapshotID:  record.SnapshotID,
	}, nil
}

// AdmitRaw exposes only the undelivered build/refill subset of resource admission.
// It shares the original transaction, gate, workers and continuous VM guard.
func (p *VMProvisioning) AdmitRaw(ctx context.Context, request supply.ProvisionRequest, target supply.RawTarget) (operation.Record, error) {
	return p.Admit(ctx, ProvisionAdmission{
		Request: request,
		Target: &ProvisionTarget{
			NewSandboxID: target.NewSandboxID,
			Budget:       target.Budget,
			Parent:       target.Parent,
		},
	})
}
