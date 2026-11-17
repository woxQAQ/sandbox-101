package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// AdmitStop is an internal resource boundary for an undelivered sandbox. Running
// Activated sessions require their separate deactivation/pause workflow.
func (service *VMStops) AdmitStop(ctx context.Context, request StopRequest) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
				Record{},
			fmt.Errorf(
				"%w: required VMStops capability port unavailable",

				errs.ErrManualRecovery)
	}

	return service.AdmitForParent(ctx, request, nil)
}

func (service *VMStops) AdmitForParent(ctx context.Context, request StopRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
				Record{},
			fmt.Errorf(
				"%w: required VMStops capability port unavailable",

				errs.ErrManualRecovery)
	}

	lifecycle, allowRunning := "", false
	if authority, ok := parent.(StopParentAuthority); ok {
		lifecycle, allowRunning = authority.StopLifecycle()
	}
	var empty operation.Record
	if request.TenantID == "" || request.SandboxID == "" || request.ExpectedInstance == "" || request.Key == "" || len(request.Key) > 256 {
		return empty, fmt.Errorf("stop requires scoped sandbox, instance and key")
	}
	frozen := operation.Encode(struct {
		Action  string
		Request StopRequest
	}{"stop", request})
	hash := operation.Digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	closing := service.lifetime.Closing() || service.store.Reviewed
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	sandbox, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, sandbox.VMID, parent); err != nil {
		return empty, err
	}
	if (sandbox.State != "provisioning" && (!allowRunning || sandbox.State != "running")) || sandbox.Instance != request.ExpectedInstance {
		return empty, fmt.Errorf("stop requires undelivered current sandbox instance")
	}
	source, err := LoadResourceStep(ctx, tx, sandbox.SourceOperation)
	if err != nil {
		return empty, err
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if _, err = service.proofs.GenerationHistory(ctx, tx, source); err != nil {
		return empty, err
	}
	currentMachine, instance, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != request.ExpectedInstance || !currentMachine.Verified || !currentMachine.AgentReady || currentMachine.Deleting {
		return empty, fmt.Errorf("stop current ownership changed")
	}
	operations := operation.New(tx)
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID: sandbox.ID,
			VMID:      sandbox.VMID,
		},
	}
	input := StopInput{
		Request:         request,
		SourceOperation: source.OperationID,
		SourceHash:      source.Hash,
	}
	input.Lifecycle = lifecycle
	if err = AdmitResourceParent(ctx, tx, parent, accepted, "stop", sandbox.VMID, hash); err != nil {
		return empty, err
	}
	payload := operation.Encode(input)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Hash:      hash,
		Request:   frozen,
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "stop",
		Action:              "stop",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: sandbox.ID,
		ExpectedInstance:    request.ExpectedInstance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ClaimRunning(ctx, dbgen.ClaimRunningParams{
		ID:       sandbox.VMID,
		Instance: request.ExpectedInstance,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("stop_admitted"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}

func (service *VMStops) stopProof(tx *sql.Tx, step StopStep) (ResourceStep, VMRecord, error) {
	source, err := LoadResourceStep(service.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return source, VMRecord{}, err
	}
	if source.Hash != step.Input.SourceHash || source.Input.SandboxID != step.Input.Request.SandboxID || source.Input.TenantID != step.Input.Request.TenantID || source.State.Witness != step.Input.Request.ExpectedInstance {
		return source, VMRecord{}, fmt.Errorf("stop source history changed")
	}
	if err = service.proofs.VerifyGeneration(service.ctx, tx, source); err != nil {
		return source, VMRecord{}, err
	}
	currentMachine, current, owner, err := LoadVM(service.ctx, tx, source)
	if err != nil {
		return source, currentMachine, err
	}
	record, err := service.ownership.ReadSandbox(service.ctx, tx, step.Input.Request.SandboxID, step.Input.Request.TenantID)
	if err != nil {
		return source, currentMachine, err
	}
	state := "provisioning"
	if step.Input.Lifecycle != "" {
		state, err = service.ownership.VerifyStopParent(service.ctx, tx, step, source, record)
		if err != nil {
			return source, currentMachine, err
		}
	}
	if owner != step.OperationID || current != step.Input.Request.ExpectedInstance || currentMachine.Deleting || record.SourceOperation != source.OperationID || record.State != state || record.Instance != current {
		return source, currentMachine, fmt.Errorf("stop current instance or sandbox ownership changed")
	}
	projection, err := operation.New(tx).Projection(service.ctx, step.OperationID)
	if err != nil {
		return source, currentMachine, err
	}
	request, body := projection.Request, projection.Body
	hash, scope, tenant, status := projection.RequestHash, projection.ScopeKind, projection.ScopeID, projection.Status
	var frozen struct {
		Action  string
		Request StopRequest
	}
	var result operation.Result
	if err = json.Unmarshal(request, &frozen); err != nil {
		return source, currentMachine, err
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return source, currentMachine, err
	}
	if operation.Digest(request) != hash || scope != "tenant" || tenant != source.Input.TenantID || operation.Terminal(status) || frozen.Action != "stop" || frozen.Request != step.Input.Request || result.VMID != source.Candidate || result.SandboxID != source.Input.SandboxID {
		return source, currentMachine, fmt.Errorf("stop parent identity changed")
	}
	return source, currentMachine, nil
}

func (service *VMStops) runStop(step StopStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	service.admission.Lock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	var source ResourceStep
	if err == nil {
		source, _, err = service.stopProof(tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	if step.Phase != "admitted" && step.Phase != "stop_intent" {
		return fmt.Errorf("stop phase unsupported")
	}
	if step.Phase == "admitted" {
		if err = store.RequireOne(dbgen.New(service.store.Database).AdvanceLiveStep(service.ctx, dbgen.AdvanceLiveStepParams{
			OperationID:  step.OperationID,
			StepKey:      "stop",
			InputHash:    step.Hash,
			PreviousBody: operation.Encode(step.Phase),
			NewBody:      operation.Encode("stop_intent"),
		})); err != nil {
			return err
		}
		step.Phase = "stop_intent"
		if err = service.store.Checkpoint(); err != nil {
			return err
		}
	}
	r := source.Input.Resource
	if err = service.backend.Stop(service.ctx, r); err != nil {
		return err
	}
	observation, err := service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive {
		return fmt.Errorf("stop exit unconfirmed")
	}
	if err = service.backend.Network(service.ctx, r, false); err != nil {
		return err
	}
	observation, err = service.backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("stop network/exit unconfirmed")
	}
	if err = service.fault("stop_effect_stopped"); err != nil {
		return err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err = service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadStop(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || current.Phase != step.Phase || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("stop completion intent changed")
	}
	_, currentMachine, err := service.stopProof(tx, current)
	if err != nil {
		return err
	}
	currentMachine.Verified = true
	currentMachine.AgentReady = false
	currentMachine.Observation = nil
	if err = store.RequireOne(dbgen.New(tx).CompleteStop(service.ctx, dbgen.CompleteStopParams{
		ID:              source.Candidate,
		CurrentInstance: step.Input.Request.ExpectedInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
		Body: operation.Encode(currentMachine),
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "stop",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("stop_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}
