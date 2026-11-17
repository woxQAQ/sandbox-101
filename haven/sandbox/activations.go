package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	Inspect(context.Context, loom.Resource) (loom.Observation, error)
	Network(context.Context, loom.Resource, bool) error
}

type ActivationGuest interface {
	Info(context.Context, loom.Resource) error
	Activate(context.Context, loom.Resource, guest.Activation) error
}

type EgressBindings interface {
	Bind(context.Context, egress.BindingTarget, egress.Policy, map[string]egress.Secret) error
	Revoke(string) error
}

type ActivationGuards interface {
	VM(string) execution.Guard
	Admission() execution.Guard
	Configuration(string) execution.Guard
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
	Guards    ActivationGuards
	Lifetime  *execution.Lifetime
	Registry  *ephemeral.Registry
	Proofs    machine.ResourceProofs
	Committer *operation.Committer
}

// Activations establishes guest delivery under a frozen configuration. The
// parent runner retains its original VM guard and worker custody throughout.
type Activations struct {
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     ActivationGuards
	proofs     machine.ResourceProofs
	backend    ActivationBackend
	guest      ActivationGuest
	egress     EgressBindings
	caPEM      string
	epoch      string
	registry   *ephemeral.Registry
	sign       func(...string) string
	faultPoint func(string) error
	committer  *operation.Committer
}

func NewActivations(state *store.State, dependencies ActivationDependencies, options ActivationOptions) *Activations {
	return &Activations{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:        dependencies.Lifetime.Context(),
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		proofs:     dependencies.Proofs,
		backend:    options.Backend,
		guest:      options.Guest,
		egress:     options.Egress,
		caPEM:      options.CAPEM,
		epoch:      options.Epoch,
		registry:   dependencies.Registry,
		sign:       state.Sign,
		faultPoint: options.Fault,
		committer:  dependencies.Committer,
	}
}

func (service *Activations) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

// AdmitIn joins the initial activation and configuration fence to the caller's
// resource transaction. Secret registration and dispatch follow its commit.
func (service *Activations) AdmitIn(tx *sql.Tx, record operation.Record, resource machine.ResourceInput, claim ClaimAdmission) error {
	claim.Settings.Generation = record.ID
	input := ActivationInput{
		RequestTenantID:   resource.TenantID,
		TenantID:          resource.TenantID,
		SandboxID:         resource.SandboxID,
		ResourceOperation: record.ID,
		ResourceHash:      operation.Digest(operation.Encode(resource)),
		ExpectedInstance:  resource.Resource.Instance,
		Settings:          claim.Settings,
		ResetIdentity:     true,
	}
	payload := operation.Encode(input)
	if claim.MAC != "" {
		if err := operation.New(tx).InsertCredential(service.ctx, record.ID, claim.MAC); err != nil {
			return err
		}
	}
	if err := store.RequireOne(dbgen.New(tx).InsertSandboxSettings(service.ctx, dbgen.InsertSandboxSettingsParams{
		SandboxID: resource.SandboxID,
		Body:      operation.Encode(claim.Settings),
	})); err != nil {
		return err
	}
	if err := store.RequireOne(dbgen.New(tx).InsertConfigurationFence(service.ctx, dbgen.InsertConfigurationFenceParams{
		SandboxID:   resource.SandboxID,
		OperationID: record.ID,
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).InsertStep(service.ctx, dbgen.InsertStepParams{
		OperationID:         record.ID,
		StepKey:             "activation",
		Action:              "activate",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: resource.Resource.VMID,
		ExpectedInstance:    resource.Resource.Instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	}))
}

// Resume selects standalone activation while resource-owned activation remains
// with its resource runner. The original source determines the shared VM guard.
func (service *Activations) Resume(operationID string) (bool, error) {
	activation, err := LoadActivation(service.ctx, service.store.Database, operationID)
	if err == nil && activation.Input.ResourceOperation != operationID {
		source, lookup := machine.LoadResourceStep(service.ctx, service.store.Database, activation.Input.ResourceOperation)
		if lookup != nil {
			return false, lookup
		}
		guard := service.guards.VM(source.Candidate)
		guard.Lock()
		defer guard.Unlock()
		return true, service.Continue(operationID)
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	return false, nil
}

// blockActivation retains the configuration fence and all resource charges. A
// superseded worker records only its own failure and cannot revoke a successor.
func (service *Activations) blockActivation(step ActivationStep, source machine.ResourceStep) error {
	guard := service.guards.Configuration(step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	return service.BlockGuarded(step, source)
}

// BlockGuarded borrows the configuration guard already held by the caller.
func (service *Activations) BlockGuarded(step ActivationStep, source machine.ResourceStep) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if current, err := LoadActivation(ctx, service.store.Database, step.OperationID); err != nil {
		return err
	} else if current.Key != step.Key || current.Hash != step.Hash || operation.Terminal(current.Status) {
		return nil
	}
	var cleanupErr error
	fence, queryErr := LifecycleFence(ctx, service.store.Database, step.Input.SandboxID)
	if queryErr == nil && fence == step.OperationID {
		cleanupErr = service.backend.Network(ctx, source.Input.Resource, false)
		cleanupErr = errors.Join(cleanupErr, service.RevokeSandbox(step.Input.SandboxID))
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(ctx, nil)
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
	if err = store.RequireOne(dbgen.New(tx).FailLiveStep(ctx, dbgen.FailLiveStepParams{
		OperationID: step.OperationID,
		StepKey:     step.Key,
		InputHash:   step.Hash,
		Body:        operation.Encode("blocked"),
	})); err != nil {
		return errors.Join(cleanupErr, err)
	}
	status, body, err := operation.New(tx).ResultState(ctx, step.OperationID)
	if err != nil {
		return errors.Join(cleanupErr, err)
	}
	if !operation.Terminal(status) && !step.Input.AllowFallback {
		var result operation.Result
		if err = json.Unmarshal(body, &result); err != nil {
			return errors.Join(cleanupErr, err)
		}
		result.Error = "sandbox activation could not establish delivery"
		if err = operation.New(tx).FailResult(ctx, step.OperationID, operation.Encode(result)); err != nil {
			return errors.Join(cleanupErr, err)
		}
	}
	if queryErr == nil && fence == step.OperationID {
		record, err := LoadSandbox(ctx, tx, step.Input.SandboxID, step.Input.TenantID)
		if err != nil {
			return errors.Join(cleanupErr, err)
		}
		if record.SourceOperation == step.Input.ResourceOperation && record.Instance == step.Input.ExpectedInstance {
			record.LastError = "sandbox activation could not establish delivery"
			if err = store.RequireOne(dbgen.New(tx).UpdateSandboxBody(ctx, dbgen.UpdateSandboxBodyParams{
				Body:   operation.Encode(record),
				ID:     record.ID,
				Tenant: record.TenantID,
			})); err != nil {
				return errors.Join(cleanupErr, err)
			}
		}
	}
	if err = service.committer.Commit(tx); err != nil {
		return errors.Join(cleanupErr, err)
	}
	return errors.Join(cleanupErr, service.store.Checkpoint())
}

func (service *Activations) activationSecrets(step ActivationStep) (map[string]egress.Secret, error) {
	secrets := service.registry.OperationSecrets(step.OperationID)
	for name, descriptor := range step.Input.Settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return nil, fmt.Errorf("%w: host credentials must be supplied again after restart", errs.ErrConflict)
		}
	}
	return secrets, nil
}

func (service *Activations) bindActivation(step ActivationStep, source machine.ResourceStep) error {
	guard := service.guards.Configuration(step.Input.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	if _, err := service.checkActivation(step); err != nil {
		return err
	}
	secrets, err := service.activationSecrets(step)
	if err != nil {
		return err
	}
	if step.Input.Settings.Policy.Mode == "none" {
		return service.RevokeSandbox(step.Input.SandboxID)
	}
	if service.egress == nil {
		return fmt.Errorf("%w: egress effects required for this network mode", errs.ErrIncompatible)
	}
	// Binding fingerprints remain keyed and ephemeral. Secret values stay in the
	// shared ephemeral registry and never enter the unified metadata.
	fingerprint := service.sign("binding-v1", step.OperationID, guest.Socket(source.Input.Resource))
	bound := service.registry.Binding(step.Input.SandboxID)
	if bound == fingerprint {
		return nil
	}
	if err = service.RevokeSandbox(step.Input.SandboxID); err != nil {
		return err
	}
	if err = service.egress.Bind(service.ctx, egress.BindingTarget{
		SandboxID: step.Input.SandboxID,
		TenantID:  step.Input.TenantID,
		VSockPath: guest.Socket(source.Input.Resource),
	}, step.Input.Settings.Policy, secrets); err != nil {
		return err
	}
	service.registry.Bind(step.Input.SandboxID, fingerprint, secrets)
	return nil
}

func (service *Activations) RevokeSandbox(id string) error {
	if service.egress != nil {
		if err := service.egress.Revoke(id); err != nil {
			return err
		}
	}
	service.registry.ForgetBinding(id)
	return nil
}
