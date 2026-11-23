package machine

import (
	"context"
	"database/sql"
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
func (s *Stops) AdmitStop(ctx context.Context, request StopRequest) (operation.State, error) {
	if s.
		ownership ==
		nil {
		return operation.State{},
			fmt.Errorf(
				"%w: required Stops capability port unavailable",

				errs.ErrManualRecovery)
	}

	return s.AdmitForParent(ctx, request, nil)
}

func (s *Stops) AdmitForParent(ctx context.Context, request StopRequest, parent ResourceParent) (operation.State, error) {
	if s.
		ownership ==
		nil {
		return operation.State{},
			fmt.Errorf(
				"%w: required Stops capability port unavailable",

				errs.ErrManualRecovery)
	}

	lifecycle, allowRunning := "", false
	if authority, ok := parent.(StopParentAuthority); ok {
		lifecycle, allowRunning = authority.StopLifecycle()
	}
	var empty operation.State
	if request.TenantID == "" || request.SandboxID == "" || request.ExpectedInstance == "" || request.Key == "" || len(request.Key) > 256 {
		return empty, fmt.Errorf("stop requires scoped sandbox, instance and key")
	}
	frozen := request.operationRequest()
	s.storeWriteMu.Lock()
	defer s.storeWriteMu.Unlock()
	closing := s.lifetime.Closing() || s.store.Reviewed
	if closing {
		return empty, fmt.Errorf("haven is closing")
	}
	tx, err := s.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, frozen); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			s.dispatch(cached.ID)
		}
		return cached, err
	}
	sandbox, err := s.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
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
	if err = s.proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return empty, err
	}
	if _, err = s.proofs.GenerationHistory(ctx, tx, source); err != nil {
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
	accepted := operation.State{
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
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:     accepted,
		Kind:      "stop",
		Candidate: sandbox.VMID,
		Request:   frozen,
	}); err != nil {
		return empty, err
	}
	payload := operation.Encode(input)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   request.TenantID,
		Key:       request.Key,
		Request:   frozen,
		Status:    "pending",
		Result:    accepted.Result,
	}); err != nil {
		return empty, err
	}
	if err = operation.InsertPhaseStep(ctx, tx, operation.PhaseIntent{
		OperationID:         accepted.ID,
		StepKey:             "stop",
		Action:              "stop",
		Input:               payload,
		CandidateResourceID: sandbox.ID,
		ExpectedInstance:    request.ExpectedInstance,
		Status:              "pending",
		Phase:               "admitted",
	}); err != nil {
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
	if err = s.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = s.fault("stop_admitted"); err != nil {
		return accepted, err
	}
	s.dispatch(accepted.ID)
	return accepted, nil
}

func (s *Stops) stopProof(tx *sql.Tx, step StopStep) (ResourceStep, VMRecord, error) {
	source, err := LoadResourceStep(s.ctx, tx, step.Input.SourceOperation)
	if err != nil {
		return source, VMRecord{}, err
	}
	if source.Hash != step.Input.SourceHash || source.Input.SandboxID != step.Input.Request.SandboxID || source.Input.TenantID != step.Input.Request.TenantID || source.State.Witness != step.Input.Request.ExpectedInstance {
		return source, VMRecord{}, fmt.Errorf("stop source history changed")
	}
	if err = s.proofs.VerifyGeneration(s.ctx, tx, source); err != nil {
		return source, VMRecord{}, err
	}
	currentMachine, current, owner, err := LoadVM(s.ctx, tx, source)
	if err != nil {
		return source, currentMachine, err
	}
	record, err := s.ownership.ReadSandbox(s.ctx, tx, step.Input.Request.SandboxID, step.Input.Request.TenantID)
	if err != nil {
		return source, currentMachine, err
	}
	state := "provisioning"
	if step.Input.Lifecycle != "" {
		state, err = s.ownership.VerifyStopParent(s.ctx, tx, StopParentProof{
			Step:    step,
			Source:  source,
			Sandbox: record,
		})
		if err != nil {
			return source, currentMachine, err
		}
	}
	if owner != step.OperationID || current != step.Input.Request.ExpectedInstance || currentMachine.Deleting || record.SourceOperation != source.OperationID || record.State != state || record.Instance != current {
		return source, currentMachine, fmt.Errorf("stop current instance or sandbox ownership changed")
	}
	projection, err := operation.New(tx).Projection(s.ctx, step.OperationID)
	if err != nil {
		return source, currentMachine, err
	}
	result := projection.Result
	if !projection.Request.Equal(step.Input.Request.operationRequest()) || projection.ScopeKind != "tenant" || projection.ScopeID != source.Input.TenantID || operation.Terminal(projection.Status) || result.VMID != source.Candidate || result.SandboxID != source.Input.SandboxID {
		return source, currentMachine, fmt.Errorf("stop parent identity changed")
	}
	return source, currentMachine, nil
}

func (s *Stops) runStop(step StopStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	s.storeWriteMu.Lock()
	tx, err := s.store.Database.BeginTx(s.ctx, nil)
	var source ResourceStep
	if err == nil {
		source, _, err = s.stopProof(tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	s.storeWriteMu.Unlock()
	if err != nil {
		return err
	}
	if step.Phase != "admitted" && step.Phase != "stop_intent" {
		return fmt.Errorf("stop phase unsupported")
	}
	if step.Phase == "admitted" {
		if err = operation.AdvancePhase(s.ctx, s.store.Database, operation.PhaseChange{
			OperationID:    step.OperationID,
			StepKey:        "stop",
			InputHash:      step.Hash,
			PreviousStatus: step.Status,
			PreviousPhase:  step.Phase,
			NextStatus:     "running",
			NextPhase:      "stop_intent",
		}); err != nil {
			return err
		}
		step.Phase = "stop_intent"
		if err = s.store.Checkpoint(); err != nil {
			return err
		}
	}
	r := source.Input.Resource
	if err = s.backend.Stop(s.ctx, r); err != nil {
		return err
	}
	observation, err := s.backend.Inspect(s.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive {
		return fmt.Errorf("stop exit unconfirmed")
	}
	if err = s.backend.SetNetworkEnabled(s.ctx, r, false); err != nil {
		return err
	}
	observation, err = s.backend.Inspect(s.ctx, r)
	if err != nil {
		return err
	}
	if observation.Alive || observation.Network {
		return fmt.Errorf("stop network/exit unconfirmed")
	}
	if err = s.fault("stop_effect_stopped"); err != nil {
		return err
	}
	s.storeWriteMu.Lock()
	defer s.storeWriteMu.Unlock()
	tx, err = s.store.Database.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := LoadStop(s.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || current.Phase != step.Phase || !reflect.DeepEqual(current.Input, step.Input) {
		return fmt.Errorf("stop completion intent changed")
	}
	_, _, err = s.stopProof(tx, current)
	if err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteStop(s.ctx, dbgen.CompleteStopParams{
		ID:              source.Candidate,
		CurrentInstance: step.Input.Request.ExpectedInstance,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = operation.AdvancePhase(s.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "stop",
		InputHash:      step.Hash,
		PreviousStatus: "running",
		PreviousPhase:  step.Phase,
		NextStatus:     "succeeded",
		NextPhase:      "complete",
	}); err != nil {
		return err
	}
	if err = operations.Complete(s.ctx, step.OperationID); err != nil {
		return err
	}
	if err = s.fault("stop_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return s.store.Checkpoint()
}
