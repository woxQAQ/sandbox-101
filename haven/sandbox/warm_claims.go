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

type WarmClaimDependencies struct {
	StoreWriteMu          execution.Guard
	SandboxConfigLocks    *execution.KeyedLocks[string]
	SandboxLifecycleLocks *execution.KeyedLocks[string]
	Gate                  execution.AdmissionGate
	Workers               *execution.Workers
	Registry              *ephemeral.Registry
	Proofs                machine.ResourceProofs
	Committer             *operation.Committer
	Activations           *Activations
	Provisioning          *Provisioning
	Deletion              *machine.Deletion
	Policies              *machine.DirectPolicies
	Links                 *machine.ResourceLinks
}

// WarmClaims selects original ready pool custody and owns warm activation,
// release/fallback, failure retention and supersession by sandbox release.
type WarmClaims struct {
	store                 store.Workflow
	ctx                   context.Context
	storeWriteMu          execution.Guard
	admissionGate         execution.AdmissionGate
	sandboxConfigLocks    *execution.KeyedLocks[string]
	sandboxLifecycleLocks *execution.KeyedLocks[string]
	workers               *execution.Workers
	registry              *ephemeral.Registry
	proofs                machine.ResourceProofs
	activations           *Activations
	provisioning          *Provisioning
	deletion              *machine.Deletion
	policies              *machine.DirectPolicies
	links                 *machine.ResourceLinks
	faultPoint            func(string) error
	committer             *operation.Committer
}

func NewWarmClaims(state *store.State, dependencies WarmClaimDependencies, fault func(string) error) *WarmClaims {
	return &WarmClaims{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:                   dependencies.Workers.Context(),
		storeWriteMu:          dependencies.StoreWriteMu,
		admissionGate:         dependencies.Gate,
		sandboxConfigLocks:    dependencies.SandboxConfigLocks,
		sandboxLifecycleLocks: dependencies.SandboxLifecycleLocks,
		workers:               dependencies.Workers,
		registry:              dependencies.Registry,
		proofs:                dependencies.Proofs,
		activations:           dependencies.Activations,
		provisioning:          dependencies.Provisioning,
		deletion:              dependencies.Deletion,
		policies:              dependencies.Policies,
		links:                 dependencies.Links,
		faultPoint:            fault,
		committer:             dependencies.Committer,
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
	unlock := wc.sandboxLifecycleLocks.Lock(step.Input.Pool.SandboxID)
	defer unlock()
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

const warmClaimStepKey = "warm_claim"

type warmClaimPhase string

const (
	warmSelected    warmClaimPhase = "selected"
	warmFailed      warmClaimPhase = "warm_failed"
	warmRetired     warmClaimPhase = "warm_retired"
	warmComplete    warmClaimPhase = "complete"
	warmClaimFailed warmClaimPhase = "failed"
	warmSuperseded  warmClaimPhase = "superseded"
)

type WarmClaimState struct {
	Phase warmClaimPhase `json:"phase"`
}

type WarmClaimStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       WarmClaimInput
	State       WarmClaimState
}

func LoadWarmClaim(ctx context.Context, query store.DBTX, id string) (WarmClaimStep, error) {
	var step WarmClaimStep
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		step, err = readWarmClaim(ctx, tx, id)
		return err
	})
	return step, err
}

func readWarmClaim(ctx context.Context, query *sql.Tx, id string) (WarmClaimStep, error) {
	step := WarmClaimStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     warmClaimStepKey,
	})
	if err != nil {
		return step, err
	}
	row, err := dbgen.New(query).GetWarmClaimInputs(ctx, id)
	if err != nil {
		return step, fmt.Errorf("%w: missing frozen warm claim inputs: %v", errs.ErrManualRecovery, err)
	}
	if operation.Digest(row.Activation) != row.ActivationHash {
		return step, fmt.Errorf("warm claim activation or request changed")
	}
	var activation ActivationInput
	if err = json.Unmarshal(row.Activation, &activation); err != nil {
		return step, err
	}
	parent, err := operation.New(query).Projection(ctx, id)
	if err != nil {
		return step, err
	}
	command := parent.Request.Command
	if command == nil || parent.Request.Resource != nil || parent.Request.Action != claimAction {
		return step, fmt.Errorf("warm claim command missing or changed")
	}
	refill, err := supply.LoadSupply(ctx, query, row.RefillOperationID)
	if err != nil {
		return step, fmt.Errorf("%w: missing warm refill history: %v", errs.ErrManualRecovery, err)
	}
	source, err := machine.LoadResourceStep(ctx, query, row.SourceOperationID)
	if err != nil || source.Input.Restore == nil {
		return step, fmt.Errorf("%w: missing warm resource history: %v", errs.ErrManualRecovery, err)
	}
	step.Input = WarmClaimInput{
		RequestTenantID: row.ScopeID,
		Command:         *command,
		PoolID:          refill.OperationID,
		Pool: supply.PoolReservation{
			Template:  refill.Input.Template,
			CreatedAt: refill.Input.CreatedAt,
			SandboxID: refill.Input.SandboxID,
			VMID:      source.Candidate,
			Instance:  source.State.Witness,
			Source: ownership.GenerationRef{
				OperationID: source.OperationID,
				InputHash:   source.Hash,
			},
		},
		Source:   source,
		Settings: activation.Settings,
		Restore:  *source.Input.Restore,
	}
	step.Hash, step.Status = record.InputHash, record.Status
	step.State.Phase = warmClaimPhase(row.Phase.String)
	if operation.Digest(operation.Encode(step.Input)) != step.Hash {
		return step, fmt.Errorf("warm claim checksum changed")
	}
	switch step.State.Phase {
	case warmSelected, warmFailed, warmRetired, warmComplete, warmClaimFailed, warmSuperseded:
	default:
		return step, fmt.Errorf("warm claim phase is unsupported")
	}
	if record.Action != claimAction || command.Action != claimAction || record.CandidateResourceID != refill.Input.SandboxID || record.ExpectedInstance != source.State.Witness || activation.ExpectedInstance != source.State.Witness || activation.ResourceHash != source.Hash || activation.ResourceOperation != source.OperationID {
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
	if claim.Hash != assignment.InputHash || claim.Input.Pool.SandboxID != record.ID || claim.Input.Pool.Template.TenantID != record.TenantID {
		return quota.SlotBudget{}, fmt.Errorf("%w: slot assignment differs from its warm claim", errs.ErrManualRecovery)
	}
	if original.OperationID != source.Input.SlotOperation || original.Hash != claim.Input.Pool.Source.InputHash {
		return quota.SlotBudget{}, fmt.Errorf("%w: slot operation differs from the selected refill source", errs.ErrManualRecovery)
	}
	if original.Status != "succeeded" || original.State.Witness == "" || original.State.Witness != claim.Input.Pool.Instance {
		return quota.SlotBudget{}, fmt.Errorf("%w: selected warm instance lacks its refill witness", errs.ErrManualRecovery)
	}
	warmBudget := quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}
	if original.Input.SlotBudget != warmBudget {
		return quota.SlotBudget{}, fmt.Errorf("%w: warm refill did not start with a zero-unit sandbox slot budget", errs.ErrManualRecovery)
	}
	if frozen := source.Input.SlotAssignment; frozen != nil && *frozen != *assignment {
		return quota.SlotBudget{}, fmt.Errorf("%w: frozen slot assignment differs from current assignment", errs.ErrManualRecovery)
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

func validateWarmSlotDestination(ctx context.Context, tx *sql.Tx, resource machine.ResourceStep) error {
	destination := resource.Input.SlotDestination
	parent, err := LoadWarmClaim(ctx, tx, destination.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || parent.State.Phase != warmFailed || destination.Kind != "claim" || destination.ID != parent.Input.Pool.SandboxID || destination.ID != resource.Input.SandboxID || parent.Input.Source.OperationID != resource.Input.SourceOperation || parent.Input.Source.State.Witness != resource.ExpectedInstance || parent.Input.Pool.Template.TenantID != resource.Input.TenantID {
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
	if activation.Status != "failed" || activation.Input.Attempt != warmAttempt || !activation.Input.AllowFallback {
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
	return supply.RetireSelectedReservation(ctx, tx, parent.Input.PoolID, reservation)
}
