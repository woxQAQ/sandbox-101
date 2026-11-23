package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

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
	"github.com/woxQAQ/haven/supply"
)

type ForkBackend interface {
	// ValidateHostConfig validates the requested VM configuration against this host.
	ValidateHostConfig(ctx context.Context, config loom.Config) error
}

type ForkOptions struct {
	Backend  ForkBackend
	Guest    ActivationGuest
	Capacity ProvisionCapacity
	Fault    func(string) error
}

type ForkDependencies struct {
	StoreWriteMu           execution.Guard
	TenantIdempotencyLocks *operation.IdempotencyLocks
	Gate                   execution.AdmissionGate
	Workers                *execution.Workers
	Registry               *ephemeral.Registry
	History                machine.SnapshotProofs
	Committer              *operation.Committer
	CommandIdentity        *operation.CommandIdentity
	AdmissionPolicy        *AdmissionPolicy
	Availability           *execution.Availability
	OperationMonitor       *operation.Monitor
	Provisioning           *Provisioning
	Captures               *machine.Captures
	Deletion               *machine.Deletion
	Lifecycle              *Lifecycle
	Policies               *machine.DirectPolicies
	Links                  *machine.ResourceLinks
}

// Forks owns batch placement and compensation. Every target and reservation is
// committed together before branch effects; child publications remain immutable.
type Forks struct {
	store                  store.Workflow
	ctx                    context.Context
	storeWriteMu           execution.Guard
	gate                   execution.AdmissionGate
	tenantIdempotencyLocks *operation.IdempotencyLocks
	forkBatchLocks         execution.KeyedLocks[string]
	workers                *execution.Workers
	registry               *ephemeral.Registry
	history                machine.SnapshotProofs
	catalog                *Catalog
	identity               *operation.CommandIdentity
	admissionPolicy        *AdmissionPolicy
	availability           *execution.Availability
	operationMonitor       *operation.Monitor
	provisioning           *Provisioning
	captures               *machine.Captures
	deletion               *machine.Deletion
	lifecycle              *Lifecycle
	policies               *machine.DirectPolicies
	links                  *machine.ResourceLinks
	opts                   ForkOptions
	committer              *operation.Committer
}

func NewForks(state *store.State, dependencies ForkDependencies, options ForkOptions) *Forks {
	return &Forks{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:                    dependencies.Workers.Context(),
		storeWriteMu:           dependencies.StoreWriteMu,
		gate:                   dependencies.Gate,
		tenantIdempotencyLocks: dependencies.TenantIdempotencyLocks,
		workers:                dependencies.Workers,
		registry:               dependencies.Registry,
		history:                dependencies.History,
		catalog:                NewCatalog(state.Database()),
		identity:               dependencies.CommandIdentity,
		admissionPolicy:        dependencies.AdmissionPolicy,
		availability:           dependencies.Availability,
		operationMonitor:       dependencies.OperationMonitor,
		provisioning:           dependencies.Provisioning,
		captures:               dependencies.Captures,
		deletion:               dependencies.Deletion,
		lifecycle:              dependencies.Lifecycle,
		policies:               dependencies.Policies,
		links:                  dependencies.Links,
		opts:                   options,
		committer:              dependencies.Committer,
	}
}

func (f *Forks) fault(point string) error {
	if f.opts.Fault != nil {
		return f.opts.Fault(point)
	}
	return nil
}

func (f *Forks) existing(ctx context.Context, principal authorization.Principal, key string, credentials operation.CommandCredentials) (operation.State, bool, error) {
	accepted, found, err := f.identity.Existing(ctx, principal, key, credentials)
	if found && err == nil && !operation.Terminal(accepted.Status) {
		f.dispatch(accepted.ID)
	}
	return accepted, found, err
}

func (f *Forks) dispatch(id string) {
	if f.store.Reviewed {
		return
	}
	f.workers.Start(id, func() error {
		if handled, err := f.policies.Resume(f.ctx, id); handled || err != nil {
			return err
		}
		_, err := f.Resume(id)
		return err
	})
}

func (f *Forks) dispatchBranch(id string) {
	if f.store.Reviewed {
		return
	}
	f.workers.Start(id, func() error {
		if handled, err := f.policies.Resume(f.ctx, id); handled || err != nil {
			return err
		}
		return f.provisioning.Continue(id)
	})
}

func (f *Forks) Resume(id string) (bool, error) {
	if f.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume fork", errs.ErrManualRecovery)
	}
	group, err := LoadFork(f.ctx, f.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	unlock := f.forkBatchLocks.Lock(id)
	defer unlock()
	group, err = LoadFork(f.ctx, f.store.Database, id)
	if err != nil {
		return true, err
	}
	if operation.Terminal(group.Status) {
		return true, nil
	}
	return true, f.runForkGroup(group)
}

func (f *Forks) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return f.links.Run(parent, role, func(link machine.ResourceLink) error {
		if handled, err := f.policies.Resume(f.ctx, link.ChildID); handled || err != nil {
			return err
		}
		var handled bool
		var err error
		switch link.Kind {
		case "capture":
			handled, err = f.captures.Resume(link.ChildID)
		case "discard":
			handled, err = f.deletion.ResumeDiscard(link.ChildID)
		case "lifecycle":
			handled, err = f.lifecycle.Resume(link.ChildID)
		default:
			return fmt.Errorf("unsupported fork resource kind %q", link.Kind)
		}
		if !handled && err == nil {
			return fmt.Errorf("fork resource step missing")
		}
		return err
	})
}

// A fork group owns the whole requested batch before any capture or allocation.
// Source contains either a completed snapshot or the exact generation to capture.
type ForkInput struct {
	RequestTenantID string                  `json:"request_tenant_id"`
	OwnerTenantID   string                  `json:"owner_tenant_id"`
	Command         operation.Intent        `json:"command"`
	Template        supply.TemplateRecord   `json:"template"`
	Settings        Settings                `json:"settings"`
	Targets         []ownership.Sandbox     `json:"targets"`
	SourceSandbox   *ownership.Sandbox      `json:"source_sandbox,omitempty"`
	Source          *machine.ResourceStep   `json:"source,omitempty"`
	Snapshot        *machine.SnapshotSource `json:"snapshot,omitempty"`
	Checkpoint      *Checkpoint             `json:"checkpoint,omitempty"`
}

type ForkState struct {
	Phase    string                  `json:"phase"`
	Snapshot *machine.SnapshotSource `json:"snapshot,omitempty"`
	Children []string                `json:"children,omitempty"`
	Error    string                  `json:"error,omitempty"`
}

type ForkStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       ForkInput
	State       ForkState
}

func LoadFork(ctx context.Context, query store.DBTX, id string) (ForkStep, error) {
	var step ForkStep
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		step, err = readFork(ctx, tx, id)
		return err
	})
	return step, err
}

func readFork(ctx context.Context, query *sql.Tx, id string) (ForkStep, error) {
	step := ForkStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "fork_group",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload := record.Input
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("fork group input changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	progress, err := dbgen.New(query).ReadForkProgress(ctx, id)
	if err != nil {
		return step, fmt.Errorf("cannot read fork progress for %s: %v", id, err)
	}
	step.State = ForkState{
		Phase:    record.Phase.String,
		Error:    record.StepError,
		Snapshot: step.Input.Snapshot,
	}
	if progress.CaptureOperationID.Valid {
		if step.Input.Snapshot != nil {
			return step, fmt.Errorf("fork has conflicting snapshot sources")
		}
		source, err := machine.CapturePublication(ctx, query, progress.CaptureOperationID.String)
		if err != nil {
			return step, err
		}
		if operation.Digest(operation.Encode(source)) != progress.PublicationHash.String {
			return step, fmt.Errorf("fork original capture publication changed")
		}
		step.State.Snapshot = &source
	}
	for index := int64(0); index < progress.ChildCount; index++ {
		link, _, err := machine.LoadResourceLink(ctx, query, id, fmt.Sprintf("branch:%d", index))
		if err != nil {
			return step, fmt.Errorf("cannot read fork %s branch %d: %v", id, index, err)
		}
		step.State.Children = append(step.State.Children, link.ChildID)
	}
	if action != "fork" || candidate != id || expected != "" || step.Input.Command.Action != "fork" || len(step.Input.Targets) < 1 || len(step.Input.Targets) > 32 || len(step.Input.Targets) != step.Input.Command.Count || (step.Input.Source == nil) == (step.Input.Snapshot == nil) {
		return step, fmt.Errorf("fork group source or batch identity changed")
	}
	ids := map[string]bool{}
	for _, target := range step.Input.Targets {
		if target.TenantID != step.Input.OwnerTenantID || target.OperationID != id || target.State != "provisioning" || target.VMID != "" || target.SourceOperation != "" || target.Identity.TemplateID != step.Input.Template.ID || target.Identity.Shape != step.Input.Template.Config.Shape || target.Identity.Epoch == "" || ids[target.ID] {
			return step, fmt.Errorf("fork placement identity changed")
		}
		ids[target.ID] = true
	}
	if len(step.State.Children) != 0 && len(step.State.Children) != len(step.Input.Targets) {
		return step, fmt.Errorf("fork child batch is incomplete")
	}
	switch step.State.Phase {
	case "admitted", "captured", "allocated", "compensating", "complete", "failed":
	default:
		return step, fmt.Errorf("fork group phase is unknown")
	}
	return step, nil
}

type forkAdmission struct {
	gate     execution.AdmissionGate
	history  machine.SnapshotProofs
	capacity ProvisionCapacity
	Parent   operation.State
	Input    ForkInput
	MAC      string
	Free     int64
}

type forkBranchAdmission struct {
	Group  ForkStep
	Target ownership.Sandbox
	Index  int
}

func (admission *forkAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	child, kind, candidate := request.Child, request.Kind, request.Candidate
	if admission.Input.Source == nil || kind != "capture" {
		return fmt.Errorf("%w: fork capture dependency changed", errs.ErrConflict)
	}
	source, err := machine.LoadResourceStep(ctx, tx, admission.Input.Source.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, *admission.Input.Source) || child.Result.VMID != source.Candidate || child.Result.SandboxID != source.Input.SandboxID || candidate != child.Result.SnapshotID {
		return fmt.Errorf("%w: fork capture original source changed", errs.ErrConflict)
	}
	admission.Parent.Result.SnapshotID = child.Result.SnapshotID
	if err = admission.admitIntent(ctx, tx); err != nil {
		return err
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      admission.Parent.ID,
		Role:             "capture",
		ExpectedInstance: admission.Input.Source.State.Witness,
	}, request)
}

func (admission *forkBranchAdmission) ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, claim ProvisionSlotClaim) (*machine.QuotaDestination, error) {
	target, source, holder := claim.Sandbox, claim.Source, claim.Holder
	group, err := LoadFork(ctx, tx, admission.Group.OperationID)
	if err != nil {
		return nil, err
	}
	if group.Hash != admission.Group.Hash || operation.Terminal(group.Status) || group.State.Phase != "captured" || !reflect.DeepEqual(group.State.Snapshot, admission.Group.State.Snapshot) || group.State.Snapshot == nil || !reflect.DeepEqual(*group.State.Snapshot, source) || admission.Index < 0 || admission.Index >= len(group.Input.Targets) || !reflect.DeepEqual(group.Input.Targets[admission.Index], target) || holder != forkSlotHolder(group.OperationID, target.ID) {
		return nil, fmt.Errorf("%w: fork placement no longer owns its prefunded slot", errs.ErrConflict)
	}
	settings, err := LoadSettings(ctx, tx, target.ID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(settings, group.Input.Settings) {
		return nil, fmt.Errorf("%w: fork placement settings changed", errs.ErrConflict)
	}
	return nil, nil
}

func (admission *forkBranchAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	if request.Kind != "clone" || request.Child.Result.SandboxID != admission.Target.ID {
		return fmt.Errorf("%w: fork branch resource identity changed", errs.ErrConflict)
	}
	if err := matchSettings(ctx, tx, admission.Target.ID, admission.Group.Input.Settings); err != nil {
		return err
	}
	if err := store.RequireOne(dbgen.New(tx).DeleteUnchangedSettings(ctx, dbgen.DeleteUnchangedSettingsParams{
		SandboxID:  admission.Target.ID,
		Generation: admission.Group.Input.Settings.Generation,
	})); err != nil {
		return err
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID: admission.Group.OperationID,
		Role:        fmt.Sprintf("branch:%d", admission.Index),
	}, request)
}

func forkSlotHolder(group, sandbox string) string {
	return "fork:" + group + ":" + sandbox
}

func forkTargetIDs(input ForkInput) []string {
	ids := make([]string, len(input.Targets))
	for index, target := range input.Targets {
		ids[index] = target.ID
	}
	return ids
}

func transitionFork(ctx context.Context, tx *sql.Tx, step ForkStep, next ForkState, status string) error {
	current, err := readFork(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("fork progress changed")
	}
	if err = operation.AdvancePhase(ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "fork_group",
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		PreviousPhase:  step.State.Phase,
		NextStatus:     status,
		NextPhase:      next.Phase,
	}); err != nil {
		return err
	}
	var captureID, hash sql.NullString
	if step.Input.Snapshot == nil && next.Snapshot != nil {
		captureID = optionalID(next.Snapshot.CaptureOperation)
		hash = optionalID(operation.Digest(operation.Encode(next.Snapshot)))
	}
	q := dbgen.New(tx)
	if err = store.RequireOne(q.SaveForkProgress(ctx, dbgen.SaveForkProgressParams{
		OperationID:        step.OperationID,
		CaptureOperationID: captureID,
		PublicationHash:    hash,
		ChildCount:         int64(len(next.Children)),
	})); err != nil {
		return err
	}
	return store.RequireOne(q.SetForkError(ctx, dbgen.SetForkErrorParams{
		OperationID: step.OperationID,
		StepError:   next.Error,
	}))
}

func (f *Forks) saveFork(step *ForkStep, state ForkState) error {
	f.storeWriteMu.Lock()
	defer f.storeWriteMu.Unlock()
	tx, err := f.store.Database.BeginTx(f.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = transitionFork(f.ctx, tx, *step, state, "running"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = state, "running"
	return f.store.Checkpoint()
}

func (f *Forks) forkSecrets(group ForkStep) {
	f.registry.CopyOperationSecrets(group.OperationID, group.State.Children...)
}

func sameForkTargets(first, second []string) bool {
	return slices.Equal(first, second)
}
