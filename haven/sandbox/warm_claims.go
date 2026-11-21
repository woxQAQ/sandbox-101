package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
)

type WarmClaimGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForSandboxConfiguration returns the lock shared by settings changes, revocation and final data admission.
	ForSandboxConfiguration(sandboxID string) execution.Guard
	// ForSandboxLifecycle returns the lock for lifecycle transitions and readiness checks across VM generations.
	ForSandboxLifecycle(sandboxID string) execution.Guard
	// ForVMLifecycle returns the lock for host effects and completion checks on one logical VM.
	ForVMLifecycle(vmID string) execution.Guard
}

type WarmClaimDependencies struct {
	Guards       WarmClaimGuards
	Gate         execution.AdmissionGate
	Workers      *execution.Workers
	Registry     *ephemeral.Registry
	Proofs       machine.ResourceProofs
	Committer    *operation.Committer
	Activations  *Activations
	Provisioning *Provisioning
	Deletion     *machine.Deletion
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
	registry     *ephemeral.Registry
	proofs       machine.ResourceProofs
	activations  *Activations
	provisioning *Provisioning
	deletion     *machine.Deletion
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
		admission:    dependencies.Guards.ForStoreMutation(),
		gate:         dependencies.Gate,
		guards:       dependencies.Guards,
		workers:      dependencies.Workers,
		registry:     dependencies.Registry,
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

func (wc *WarmClaims) fault(point string) error {
	if wc.faultPoint != nil {
		return wc.faultPoint(point)
	}
	return nil
}

func (wc *WarmClaims) dispatch(id string) {
	if wc.store.Reviewed {
		return
	}
	wc.workers.Start(id, func() error {
		if handled, err := wc.policies.Resume(wc.ctx, id); handled || err != nil {
			return err
		}
		_, err := wc.Resume(id)
		return err
	})
}

func (wc *WarmClaims) Resume(id string) (bool, error) {
	if wc.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume warm claim", errs.ErrManualRecovery)
	}
	step, err := LoadWarmClaim(wc.ctx, wc.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	lock := wc.guards.ForSandboxLifecycle(step.Input.Pool.SandboxID)
	lock.Lock()
	defer lock.Unlock()
	step, err = LoadWarmClaim(wc.ctx, wc.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, wc.runWarmClaim(step)
}

func (wc *WarmClaims) runResourceLink(parent, role string) (machine.ResourceLink, error) {
	return wc.links.Run(parent, role, wc.runResource)
}

func (wc *WarmClaims) runResource(link machine.ResourceLink) error {
	if handled, err := wc.policies.Resume(wc.ctx, link.ChildID); handled || err != nil {
		return err
	}
	switch link.Kind {
	case "clone":
		return wc.provisioning.Continue(link.ChildID)
	case "release":
		handled, err := wc.deletion.ResumeRelease(link.ChildID)
		if !handled && err == nil {
			return fmt.Errorf("warm release step missing")
		}
		return err
	default:
		return fmt.Errorf("unsupported warm claim resource kind %q", link.Kind)
	}
}

type WarmClaimInput struct {
	RequestTenantID string                 `json:"request_tenant_id"`
	Command         operation.Intent       `json:"command"`
	PoolID          string                 `json:"pool_id"`
	Pool            supply.PoolReservation `json:"pool"`
	Source          machine.ResourceStep   `json:"source"`
	Settings        Settings               `json:"settings"`
	Restore         machine.SnapshotSource `json:"restore"`
}

type WarmClaimState struct {
	Phase string `json:"phase"`
}

type WarmClaimStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       WarmClaimInput
	State       WarmClaimState
}

type WarmClaimAdmission struct {
	ParentID string
	Role     string
	Input    WarmClaimInput
}

func LoadWarmClaim(ctx context.Context, query store.DBTX, id string) (WarmClaimStep, error) {
	step := WarmClaimStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "warm_claim",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("warm claim checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	switch step.State.Phase {
	case "selected", "warm_failed", "warm_retired", "complete", "failed", "superseded":
	default:
		return step, fmt.Errorf("warm claim phase is unsupported")
	}
	if action != "claim" || candidate != step.Input.Pool.SandboxID || expected != step.Input.Pool.Instance || step.Input.Pool.Source.OperationID != step.Input.Source.OperationID || step.Input.Pool.Source.InputHash != step.Input.Source.Hash {
		return step, fmt.Errorf("warm claim frozen resource identity changed")
	}
	return step, nil
}

func CurrentSlotBudget(ctx context.Context, query store.DBTX, record ownership.Sandbox, source machine.ResourceStep) (quota.SlotBudget, error) {
	if record.SlotAssignment == nil {
		if source.Input.SlotAssignment != nil {
			return quota.SlotBudget{}, fmt.Errorf("%w: current slot assignment proof missing", errs.ErrManualRecovery)
		}
		return source.Input.SlotBudget, nil
	}
	assignment := record.SlotAssignment
	claim, err := LoadWarmClaim(ctx, query, assignment.OperationID)
	if err != nil {
		return quota.SlotBudget{}, err
	}
	original := claim.Input.Source
	if claim.Hash != assignment.InputHash || claim.Input.Pool.SandboxID != record.ID || claim.Input.Pool.Template.TenantID != record.TenantID || original.OperationID != source.Input.SlotOperation || original.Hash != claim.Input.Pool.Source.InputHash || original.State.Witness == "" || original.State.Witness != claim.Input.Pool.Instance || original.Status != "succeeded" || original.Input.SlotBudget != (quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) || (source.Input.SlotAssignment != nil && !reflect.DeepEqual(source.Input.SlotAssignment, assignment)) {
		return quota.SlotBudget{}, fmt.Errorf("%w: slot charge differs from frozen warm selection", errs.ErrManualRecovery)
	}
	historical, err := machine.LoadResourceStep(ctx, query, original.OperationID)
	if err != nil {
		return quota.SlotBudget{}, err
	}
	if !reflect.DeepEqual(historical, original) {
		return quota.SlotBudget{}, fmt.Errorf("%w: warm slot birth history changed", errs.ErrManualRecovery)
	}
	return quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 1,
	}, nil
}

func (admission *WarmClaimAdmission) ValidateProvisionTarget(ctx context.Context, tx *sql.Tx, slot ProvisionSlotClaim) (*machine.QuotaDestination, error) {
	record, source, holder := slot.Sandbox, slot.Source, slot.Holder
	claim, err := LoadWarmClaim(ctx, tx, admission.ParentID)
	if err != nil {
		return nil, err
	}
	if claim.State.Phase != "warm_retired" || operation.Terminal(claim.Status) || record.State != "terminated" || holder != "claim:"+admission.ParentID || record.ID != claim.Input.Pool.SandboxID || source.Snapshot.ID != claim.Input.Restore.Snapshot.ID {
		return nil, fmt.Errorf("%w: fallback target differs from retired warm responsibility", errs.ErrConflict)
	}
	released, status, err := machine.LoadResourceLink(ctx, tx, admission.ParentID, "warm_release")
	if err != nil {
		return nil, err
	}
	if status != "succeeded" {
		return nil, fmt.Errorf("%w: fallback warm cleanup incomplete", errs.ErrConflict)
	}
	receipt, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
	if err != nil {
		return nil, err
	}
	if receipt.Status != "succeeded" || receipt.State.Phase != "complete" || receipt.Input.SourceOperation != claim.Input.Source.OperationID || receipt.Input.SlotDestination == nil || receipt.Input.SlotDestination.Kind != "claim" || receipt.Input.SlotDestination.OperationID != admission.ParentID {
		return nil, fmt.Errorf("%w: fallback original warm retirement proof changed", errs.ErrManualRecovery)
	}
	return nil, nil
}

func (admission *WarmClaimAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	kind, candidate := request.Kind, request.Candidate
	step, err := LoadWarmClaim(ctx, tx, admission.ParentID)
	if err != nil {
		return err
	}
	if operation.Terminal(step.Status) || !reflect.DeepEqual(step.Input, admission.Input) {
		return fmt.Errorf("%w: warm claim no longer owns resource admission", errs.ErrConflict)
	}
	fence, err := LifecycleFence(ctx, tx, step.Input.Pool.SandboxID)
	if err != nil {
		return err
	}
	if fence != step.OperationID {
		return fmt.Errorf("%w: warm claim was superseded", errs.ErrConflict)
	}
	switch admission.Role {
	case "warm_release":
		activation, err := LoadActivation(ctx, tx, admission.ParentID)
		if err != nil {
			return err
		}
		if kind != "release" || candidate != step.Input.Pool.VMID || step.State.Phase != "warm_failed" || activation.Status != "failed" || activation.Input.Attempt != "warm" || activation.Input.ResourceOperation != step.Input.Source.OperationID {
			return fmt.Errorf("%w: warm release lacks original failed attempt", errs.ErrConflict)
		}
	case "fallback":
		if kind != "clone" || step.State.Phase != "warm_retired" {
			return fmt.Errorf("%w: fallback has no completed warm retirement", errs.ErrConflict)
		}
	default:
		return fmt.Errorf("%w: unsupported warm claim resource role", errs.ErrInvalid)
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID:      admission.ParentID,
		Role:             admission.Role,
		ExpectedInstance: step.Input.Pool.Instance,
	}, request)
}

func validateWarmSlotDestination(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	destination := resource.Input.SlotDestination
	parent, err := LoadWarmClaim(ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || parent.State.Phase != "warm_failed" || destination.Kind != "claim" || destination.ID != parent.Input.Pool.SandboxID || destination.ID != resource.Input.SandboxID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Source.State.Witness != resource.ExpectedInstance || parent.Input.Pool.Template.TenantID != resource.Input.TenantID {
		return fmt.Errorf("warm pending slot destination changed")
	}
	link, _, err := machine.LoadResourceLink(ctx, tx, parent.OperationID, "warm_release")
	if err != nil {
		return err
	}
	if link.ChildID != resource.OperationID {
		return fmt.Errorf("warm retirement child changed")
	}
	activation, err := LoadActivation(ctx, tx, parent.OperationID)
	if err != nil {
		return err
	}
	if activation.Status != "failed" || activation.Input.Attempt != "warm" || !activation.Input.AllowFallback {
		return fmt.Errorf("warm retirement failed activation proof missing")
	}
	return nil
}

func RetireSelectedPool(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	parent, err := LoadWarmClaim(ctx, tx, resource.Input.SlotDestination.OperationID)
	if err != nil {
		return err
	}
	reservation, status, err := supply.LoadPoolReservation(ctx, tx, parent.Input.PoolID)
	if err != nil {
		return err
	}
	if status != "selected" || !reflect.DeepEqual(reservation, parent.Input.Pool) || reservation.VMID != resource.Candidate {
		return fmt.Errorf("retired warm pool responsibility changed")
	}
	return supply.RetireSelectedReservation(ctx, tx, parent.Input.PoolID, operation.Encode(reservation))
}
