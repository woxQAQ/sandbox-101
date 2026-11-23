package sandbox

import (
	"context"
	"database/sql"
	"fmt"

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

// ProvisionOptions freezes the runtime effects and node capacity at construction.
type ProvisionOptions struct {
	Backend  machine.ProvisionBackend
	Capacity ProvisionCapacity
	Fault    func(string) error
}

// ProvisionDependencies supplies the coordinator's shared execution owners and
// physical capabilities. Construction does not select workflow modes.
type ProvisionDependencies struct {
	VMGuards    machine.VMGuards
	Lifetime    *execution.Lifetime
	Gate        execution.AdmissionGate
	Workers     *execution.Workers
	Registry    *ephemeral.Registry
	Proofs      machine.ResourceProofs
	Activations *Activations
	Policies    *machine.DirectPolicies
	Committer   *operation.Committer
	Ownership   machine.ProvisionOwnershipReader
	Handoff     machine.ProvisionParticipant
}

// Provisioning admits and executes cold/clone/restore intents using the shared
// admission gate and workers. It owns no independent shutdown or runner state.
type Provisioning struct {
	store        store.Workflow
	ctx          context.Context
	lifetime     *execution.Lifetime
	storeWriteMu execution.Guard
	gate         execution.AdmissionGate
	workers      *execution.Workers
	registry     *ephemeral.Registry
	policies     *machine.DirectPolicies
	proofs       machine.ResourceProofs
	backend      machine.ProvisionBackend
	activations  *Activations
	capacity     ProvisionCapacity
	faultPoint   func(string) error
	committer    *operation.Committer
	physical     *machine.Provisioning
}

func NewVMProvisioning(state *store.State, dependencies ProvisionDependencies, options ProvisionOptions) *Provisioning {
	physical := machine.NewProvisioning(state, machine.ProvisioningDependencies{
		Context:   dependencies.Lifetime.Context(),
		VMGuards:  dependencies.VMGuards,
		Proofs:    dependencies.Proofs,
		Backend:   options.Backend,
		Ownership: dependencies.Ownership,
		Handoff:   dependencies.Handoff,
		Activate:  dependencies.Activations.activate,
		Committer: dependencies.Committer,
	}, options.Fault)
	return &Provisioning{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Lifetime.Context(),
		lifetime:     dependencies.Lifetime,
		storeWriteMu: dependencies.VMGuards.StoreWriteMu,
		proofs:       dependencies.Proofs,
		gate:         dependencies.Gate,
		workers:      dependencies.Workers,
		registry:     dependencies.Registry,
		policies:     dependencies.Policies,
		backend:      options.Backend,
		activations:  dependencies.Activations,
		physical:     physical,
		capacity:     options.Capacity,
		faultPoint:   options.Fault,
		committer:    dependencies.Committer,
	}
}

func (p *Provisioning) fault(point string) error {
	if p.faultPoint != nil {
		return p.faultPoint(point)
	}
	return nil
}

func (p *Provisioning) Continue(id string) error {
	return p.physical.Continue(id)
}

// ProvisionAdmission joins one preflight request to its business transaction
// participants. Supply's bare port cannot supply claim credentials or placement
// on an existing sandbox.
type ProvisionAdmission struct {
	Request supply.ProvisionRequest
	Claim   *ClaimAdmission
	Target  *ProvisionTarget
}

type ProvisionTarget struct {
	Activation    *Settings
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
	// ProvisioningTemplate returns the parent-authorized template when provisioning precedes publication.
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
	// ValidateProvisionTarget verifies the slot claim and resolves its authorized destination in the admission transaction.
	ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, claim ProvisionSlotClaim) (*machine.QuotaDestination, error)
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

// AdmitBare exposes only the undelivered build/refill subset of resource admission.
// It shares the original transaction, gate, workers and continuous VM guard.
func (p *Provisioning) AdmitBare(ctx context.Context, request supply.ProvisionRequest, target supply.BareTarget) (operation.State, error) {
	return p.Admit(ctx, ProvisionAdmission{
		Request: request,
		Target: &ProvisionTarget{
			NewSandboxID: target.NewSandboxID,
			Budget:       target.Budget,
			Parent:       target.Parent,
		},
	})
}
