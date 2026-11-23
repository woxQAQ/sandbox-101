package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/guest"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type ActivationBackend interface {
	// Inspect observes the identified instance and reports uncertainty as an error.
	Inspect(ctx context.Context, resource loom.Resource) (loom.Observation, error)
	// SetNetworkEnabled sets the host network link state for the identified instance.
	SetNetworkEnabled(ctx context.Context, resource loom.Resource, enabled bool) error
}

type ActivationGuest interface {
	// Info checks that the candidate's Workload portal answers the supported protocol.
	Info(ctx context.Context, resource loom.Resource) error
	// Activate delivers sandbox identity, environment and network settings through the Admin portal.
	Activate(ctx context.Context, resource loom.Resource, activation guest.Activation) error
}

type EgressBindings interface {
	// Bind activates the egress policy and in-memory credentials for the given target.
	Bind(ctx context.Context, target egress.BindingTarget, policy egress.Policy, secrets map[string]egress.Secret) error
	// Revoke retires the sandbox's active egress binding.
	Revoke(sandboxID string) error
}

type ActivationOptions struct {
	Backend ActivationBackend
	Guest   ActivationGuest
	Egress  EgressBindings
	CAPEM   string
	Epoch   string
	Fault   func(string) error
}

// ActivationDependencies names the shared guards, lifetime and publication
// authority borrowed while activating a sandbox.
type ActivationDependencies struct {
	VMGuards           machine.VMGuards
	SandboxConfigLocks *execution.KeyedLocks[string]
	Lifetime           *execution.Lifetime
	Registry           *ephemeral.Registry
	Proofs             machine.ResourceProofs
	Committer          *operation.Committer
}

// Activations establishes guest delivery under a frozen configuration. The
// parent runner retains its original VM guard and worker custody throughout.
type Activations struct {
	store              store.Workflow
	ctx                context.Context
	storeWriteMu       execution.Guard
	vmLifecycleLocks   *execution.KeyedLocks[string]
	sandboxConfigLocks *execution.KeyedLocks[string]
	proofs             machine.ResourceProofs
	backend            ActivationBackend
	guest              ActivationGuest
	egress             EgressBindings
	caPEM              string
	epoch              string
	registry           *ephemeral.Registry
	sign               func(...string) string
	faultPoint         func(string) error
	committer          *operation.Committer
}

func NewActivations(state *store.State, dependencies ActivationDependencies, options ActivationOptions) *Activations {
	return &Activations{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:                dependencies.Lifetime.Context(),
		storeWriteMu:       dependencies.VMGuards.StoreWriteMu,
		vmLifecycleLocks:   dependencies.VMGuards.LifecycleLocks,
		sandboxConfigLocks: dependencies.SandboxConfigLocks,
		proofs:             dependencies.Proofs,
		backend:            options.Backend,
		guest:              options.Guest,
		egress:             options.Egress,
		caPEM:              options.CAPEM,
		epoch:              options.Epoch,
		registry:           dependencies.Registry,
		sign:               state.Sign,
		faultPoint:         options.Fault,
		committer:          dependencies.Committer,
	}
}

func (a *Activations) fault(point string) error {
	if a.faultPoint != nil {
		return a.faultPoint(point)
	}
	return nil
}

// AdmitIn joins the initial activation and configuration fence to the caller's
// resource transaction. Secret registration and dispatch follow its commit.
func (a *Activations) AdmitIn(tx *sql.Tx, record operation.State, resource machine.ResourceInput, settings Settings) error {
	settings.Generation = record.ID
	input := ActivationInput{
		RequestTenantID:   resource.TenantID,
		TenantID:          resource.TenantID,
		SandboxID:         resource.SandboxID,
		ResourceOperation: record.ID,
		ResourceHash:      operation.Digest(operation.Encode(resource)),
		ExpectedInstance:  resource.Resource.Instance,
		Settings:          settings,
		ResetIdentity:     true,
	}
	if err := insertSettings(a.ctx, tx, resource.SandboxID, settings); err != nil {
		return err
	}
	if err := store.RequireOne(dbgen.New(tx).InsertFence(a.ctx, dbgen.InsertFenceParams{
		SandboxID:   resource.SandboxID,
		OperationID: record.ID,
	})); err != nil {
		return err
	}
	return insertActivationStep(a.ctx, tx, record.ID, resource.Resource.VMID, input)
}

const (
	warmAttempt        = "warm"
	goldenAttempt      = "golden"
	activateAction     = "activate"
	activationAdmitted = "admitted"
)

func insertActivationStep(ctx context.Context, tx *sql.Tx, operationID, vmID string, input ActivationInput) error {
	payload := operation.Encode(input)
	return store.RequireOne(dbgen.New(tx).InsertActivation(ctx, dbgen.InsertActivationParams{
		OperationID:         operationID,
		StepKey:             ActivationKey(input.Attempt),
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: vmID,
		ExpectedInstance:    input.ExpectedInstance,
	}))
}

func transitionActivation(ctx context.Context, tx *sql.Tx, step ActivationStep, phase, status string) error {
	return store.RequireOne(dbgen.New(tx).TransitionActivation(ctx, dbgen.TransitionActivationParams{
		OperationID:    step.OperationID,
		StepKey:        step.Key,
		InputHash:      step.Hash,
		PreviousPhase:  optionalID(step.Phase),
		PreviousStatus: step.Status,
		NextPhase:      optionalID(phase),
		NextStatus:     status,
	}))
}

// Resume selects standalone activation while resource-owned activation remains
// with its resource runner. The original source determines the shared VM guard.
func (a *Activations) Resume(operationID string) (bool, error) {
	activation, err := LoadActivation(a.ctx, a.store.Database, operationID)
	if err == nil && activation.Input.ResourceOperation != operationID {
		source, lookup := machine.LoadResourceStep(a.ctx, a.store.Database, activation.Input.ResourceOperation)
		if lookup != nil {
			return false, lookup
		}
		return true, a.activateVM(source.Candidate, operationID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return false, nil
}

// blockActivation retains the configuration fence and all resource charges. A
// superseded worker records only its own failure and cannot revoke a successor.
func (a *Activations) blockActivation(step ActivationStep, source machine.ResourceStep) error {
	unlock := a.sandboxConfigLocks.Lock(step.Input.SandboxID)
	defer unlock()
	return a.BlockGuarded(step, source)
}

// BlockGuarded borrows the configuration guard already held by the caller.
func (a *Activations) BlockGuarded(step ActivationStep, source machine.ResourceStep) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if current, err := LoadActivation(ctx, a.store.Database, step.OperationID); err != nil {
		return err
	} else if current.Key != step.Key || current.Hash != step.Hash || operation.Terminal(current.Status) {
		return nil
	}
	var cleanupErr error
	fence, queryErr := LifecycleFence(ctx, a.store.Database, step.Input.SandboxID)
	if queryErr == nil && fence == step.OperationID {
		cleanupErr = a.backend.SetNetworkEnabled(ctx, source.Input.Resource, false)
		cleanupErr = errors.Join(cleanupErr, a.RevokeSandbox(step.Input.SandboxID))
	}
	a.storeWriteMu.Lock()
	defer a.storeWriteMu.Unlock()
	tx, err := a.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadActivation(ctx, tx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if operation.Terminal(current.Status) {
		return cleanupErr
	}
	if err = transitionActivation(ctx, tx, current, "blocked", "failed"); err != nil {
		return errors.Join(cleanupErr, err)
	}
	status, result, err := operation.New(tx).StatusAndResult(ctx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	var outcomes []operation.Outcome
	if !operation.Terminal(status) && !step.Input.AllowFallback {
		result.Error = "sandbox activation could not establish delivery"
		outcomes = append(outcomes, operation.Outcome{
			ID:     step.OperationID,
			Result: result,
			Failed: true,
		})
	}
	if queryErr == nil && fence == step.OperationID {
		record, err := LoadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
		if err != nil {
			return errors.Join(cleanupErr, err)
		}
		if record.SourceOperation == step.Input.ResourceOperation && record.Instance == step.Input.ExpectedInstance {
			if err = store.RequireOne(dbgen.New(tx).SetSandboxError(ctx, dbgen.SetSandboxErrorParams{
				LastError:         "sandbox activation could not establish delivery",
				Instance:          record.Instance,
				SourceOperationID: optionalID(record.SourceOperation),
				ID:                record.ID,
				Tenant:            record.TenantID,
			})); err != nil {
				return errors.Join(cleanupErr, err)
			}
		}
	}
	if err = a.committer.Complete(tx, outcomes...); err != nil {
		return errors.Join(cleanupErr, err)
	}
	if err = tx.Commit(); err != nil {
		return errors.Join(cleanupErr, err)
	}
	return errors.Join(cleanupErr, a.store.Checkpoint())
}

func (a *Activations) activationSecrets(step ActivationStep) (map[string]egress.Secret, error) {
	secrets := a.registry.OperationSecrets(step.OperationID)
	if !step.Input.Settings.hasCredentials(secrets) {
		return nil, fmt.Errorf("%w: host credentials must be supplied again after restart", errs.ErrConflict)
	}
	return secrets, nil
}

func (a *Activations) bindActivation(step ActivationStep, source machine.ResourceStep) error {
	unlock := a.sandboxConfigLocks.Lock(step.Input.SandboxID)
	defer unlock()
	if _, err := a.checkActivation(step); err != nil {
		return err
	}
	secrets, err := a.activationSecrets(step)
	if err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "none" {
		return a.RevokeSandbox(step.Input.SandboxID)
	}
	if a.egress == nil {
		return fmt.Errorf("%w: egress effects required for this network mode", errs.ErrIncompatible)
	}
	// Binding fingerprints remain keyed and ephemeral. Secret values stay in the
	// shared ephemeral registry and never enter the unified metadata.
	fingerprint := a.sign("binding-v1", step.OperationID, guest.Socket(source.Input.Resource))
	bound := a.registry.Binding(step.Input.SandboxID)
	if bound == fingerprint {
		return nil
	}
	if err = a.RevokeSandbox(step.Input.SandboxID); err != nil {
		return err
	}
	if err = a.egress.Bind(a.ctx, egress.BindingTarget{
		SandboxID: step.Input.SandboxID,
		TenantID:  step.Input.TenantID,
		VSockPath: guest.Socket(source.Input.Resource),
	}, step.Input.Settings.Policy, secrets); err != nil {
		return err
	}
	a.registry.Bind(step.Input.SandboxID, fingerprint, secrets)
	return nil
}

func (a *Activations) RevokeSandbox(id string) error {
	if a.egress != nil {
		if err := a.egress.Revoke(id); err != nil {
			return err
		}
	}
	a.registry.ForgetBinding(id)
	return nil
}

type ActivationInput struct {
	PreviousSettings  *Settings `json:"previous_settings,omitempty"`
	Attempt           string    `json:"attempt,omitempty"`
	AllowFallback     bool      `json:"allow_fallback,omitempty"`
	RequestTenantID   string    `json:"request_tenant_id"`
	TenantID          string    `json:"tenant_id"`
	SandboxID         string    `json:"sandbox_id"`
	ResourceOperation string    `json:"resource_operation"`
	ResourceHash      string    `json:"resource_hash"`
	ExpectedInstance  string    `json:"expected_instance"`
	Settings          Settings  `json:"settings"`
	ResetIdentity     bool      `json:"reset_identity"`
}

type ActivationStep struct {
	OperationID string
	Key         string
	Hash        string
	Status      string
	Phase       string
	Input       ActivationInput
}

func ActivationKey(attempt string) string {
	if attempt == "" {
		return "activation"
	}
	return "activation:" + attempt
}

func LoadActivation(ctx context.Context, query store.DBTX, id string) (ActivationStep, error) {
	step := ActivationStep{
		OperationID: id,
	}
	record, err := dbgen.New(query).GetLatestActivationStep(ctx, id)
	step.Key, step.Hash, step.Status = record.StepKey, record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(record.Input) != step.Hash {
		return step, fmt.Errorf("activation input checksum changed")
	}
	if err = json.Unmarshal(record.Input, &step.Input); err != nil {
		return step, err
	}
	step.Phase = record.Phase.String
	if step.Key != ActivationKey(step.Input.Attempt) {
		return step, fmt.Errorf("activation attempt key changed")
	}
	if record.Action != activateAction || record.ExpectedInstance != step.Input.ExpectedInstance || record.CandidateResourceID == "" {
		return step, fmt.Errorf("activation frozen identity changed")
	}
	return step, nil
}
