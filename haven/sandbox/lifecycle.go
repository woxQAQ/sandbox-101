package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type LifecycleBackend interface {
	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config loom.Config) error
}

type LifecycleGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForTenantIdempotencyKey returns the lock shared by admissions using this tenant's key.
	ForTenantIdempotencyKey(tenantID, key string) execution.Guard
	// ForSandboxConfiguration returns the lock shared by settings changes, revocation and final data admission.
	ForSandboxConfiguration(sandboxID string) execution.Guard
	// ForSandboxLifecycle returns the lock for lifecycle transitions and readiness checks across VM generations.
	ForSandboxLifecycle(sandboxID string) execution.Guard
	// ForVMLifecycle returns the lock for host effects and completion checks on one logical VM.
	ForVMLifecycle(vmID string) execution.Guard
}

type LifecycleOptions struct {
	Backend   LifecycleBackend
	MaxTTL    time.Duration
	PausedTTL time.Duration
	Fault     func(string) error
}

type LifecycleDependencies struct {
	Guards          LifecycleGuards
	Gate            execution.AdmissionGate
	Workers         *execution.Workers
	Registry        *ephemeral.Registry
	Proofs          machine.ResourceProofs
	Committer       *operation.Committer
	CommandIdentity *operation.CommandIdentity
	AdmissionPolicy *AdmissionPolicy
	Activations     *Activations
	WarmClaims      *WarmClaims
	Provisioning    *Provisioning
	Captures        *machine.Captures
	Stops           *machine.Stops
	Deletion        *machine.Deletion
	CaptureDiscard  *machine.CaptureDiscard
	CaptureCleanup  *machine.CaptureCleanup
	Policies        *machine.DirectPolicies
	Links           *machine.ResourceLinks
}

// Lifecycle owns sandbox pause/resume, rollback, release and checkpoint custody.
// Child effects keep their original services and guards; publication joins the
// original parent transaction before the same store checkpoint boundary.
type Lifecycle struct {
	store          store.Workflow
	ctx            context.Context
	admission      execution.Guard
	gate           execution.AdmissionGate
	guards         LifecycleGuards
	workers        *execution.Workers
	registry       *ephemeral.Registry
	proofs         machine.ResourceProofs
	catalog        *Catalog
	identity       *operation.CommandIdentity
	policy         *AdmissionPolicy
	activations    *Activations
	warmClaims     *WarmClaims
	provisioning   *Provisioning
	captures       *machine.Captures
	stops          *machine.Stops
	deletion       *machine.Deletion
	captureDiscard *machine.CaptureDiscard
	captureCleanup *machine.CaptureCleanup
	policies       *machine.DirectPolicies
	links          *machine.ResourceLinks
	opts           LifecycleOptions
	committer      *operation.Committer
}

func NewLifecycle(state *store.State, dependencies LifecycleDependencies, options LifecycleOptions) *Lifecycle {
	return &Lifecycle{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:            dependencies.Workers.Context(),
		admission:      dependencies.Guards.ForStoreMutation(),
		gate:           dependencies.Gate,
		guards:         dependencies.Guards,
		workers:        dependencies.Workers,
		registry:       dependencies.Registry,
		proofs:         dependencies.Proofs,
		catalog:        NewCatalog(state.Database()),
		identity:       dependencies.CommandIdentity,
		policy:         dependencies.AdmissionPolicy,
		activations:    dependencies.Activations,
		warmClaims:     dependencies.WarmClaims,
		provisioning:   dependencies.Provisioning,
		captures:       dependencies.Captures,
		stops:          dependencies.Stops,
		deletion:       dependencies.Deletion,
		captureDiscard: dependencies.CaptureDiscard,
		captureCleanup: dependencies.CaptureCleanup,
		policies:       dependencies.Policies,
		links:          dependencies.Links,
		opts:           options,
		committer:      dependencies.Committer,
	}
}

func (l *Lifecycle) fault(point string) error {
	if l.opts.Fault != nil {
		return l.opts.Fault(point)
	}
	return nil
}

func (l *Lifecycle) existing(ctx context.Context, principal authorization.Principal, key string, admission *ClaimAdmission) (operation.State, bool, error) {
	accepted, found, err := l.identity.Existing(ctx, principal, key, operation.CommandCredentials{
		MAC:     admission.MAC,
		Secrets: admission.Secrets,
	})
	if found && err == nil && !operation.Terminal(accepted.Status) {
		l.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (l *Lifecycle) dispatch(id string) {
	if l.store.Reviewed {
		return
	}
	l.workers.Start(id, func() error {
		if handled, err := l.policies.Resume(l.ctx, id); handled || err != nil {
			return err
		}
		_, err := l.Resume(id)
		return err
	})
}

func (l *Lifecycle) Resume(id string) (bool, error) {
	if l.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume lifecycle", errs.ErrManualRecovery)
	}
	step, err := LoadLifecycleStep(l.ctx, l.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lock := l.guards.ForSandboxLifecycle(step.Input.Sandbox.ID)
	lock.Lock()
	defer lock.Unlock()
	step, err = LoadLifecycleStep(l.ctx, l.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, l.runLifecycle(step)
}

func (l *Lifecycle) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return l.links.Run(parent, role, l.runResource)
}

func (l *Lifecycle) runResource(link machine.ResourceLink) error {
	if handled, err := l.policies.Resume(l.ctx, link.ChildID); handled || err != nil {
		return err
	}
	var handled bool
	var err error
	switch link.Kind {
	case "cold_start", "clone", "restore":
		return l.provisioning.Continue(link.ChildID)
	case "capture":
		handled, err = l.captures.Resume(link.ChildID)
	case "release":
		handled, err = l.deletion.ResumeRelease(link.ChildID)
	case "discard":
		handled, err = l.deletion.ResumeDiscard(link.ChildID)
	case "stop":
		handled, err = l.stops.Resume(link.ChildID)
	case "capture_discard":
		handled, err = l.captureDiscard.Resume(link.ChildID)
	case "capture_cleanup":
		handled, err = l.captureCleanup.Resume(link.ChildID)
	default:
		return fmt.Errorf("unsupported lifecycle resource kind %q", link.Kind)
	}
	if !handled && err == nil {
		return fmt.Errorf("lifecycle resource step missing")
	}
	return err
}

type LifecycleInput struct {
	CaptureFailures []ownership.GenerationRef `json:"capture_failures,omitempty"`
	PauseOperation  string                    `json:"pause_operation,omitempty"`
	PriorFence      string                    `json:"prior_fence,omitempty"`
	Rollback        *RollbackInput            `json:"rollback,omitempty"`
	SourceInstance  string                    `json:"source_instance"`
	CheckpointID    string                    `json:"checkpoint_id,omitempty"`
	RequestTenantID string                    `json:"request_tenant_id"`
	OwnerTenantID   string                    `json:"owner_tenant_id"`
	Command         operation.Intent          `json:"command"`
	Sandbox         ownership.Sandbox         `json:"sandbox"`
	Settings        Settings                  `json:"settings"`
	Source          ownership.GenerationRef   `json:"source"`
}

type RollbackInput struct {
	RawCaptures []ownership.GenerationRef `json:"raw_captures,omitempty"`
	Checkpoint  Checkpoint                `json:"checkpoint"`
	Source      machine.SnapshotSource    `json:"source"`
}

type LifecycleState struct {
	Phase      string `json:"phase"`
	SnapshotID string `json:"snapshot_id,omitempty"`
}

type LifecycleStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       LifecycleInput
	State       LifecycleState
}

func LoadLifecycleStep(ctx context.Context, database store.DBTX, id string) (LifecycleStep, error) {
	step := LifecycleStep{
		OperationID: id,
	}
	record, err := dbgen.New(database).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "lifecycle",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, instance := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("lifecycle input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != step.Input.Command.Action || candidate != step.Input.Sandbox.ID || instance != step.Input.Sandbox.Instance {
		return step, fmt.Errorf("lifecycle frozen identity changed")
	}
	return step, nil
}
