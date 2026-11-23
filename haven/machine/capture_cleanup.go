package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// CaptureCleanup retires raw VMM output only after its original publication and
// stop proofs establish that the retained generation no longer runs.
type CaptureCleanup struct {
	store        store.Workflow
	ctx          context.Context
	storeWriteMu execution.Guard
	steps        vmSteps[CaptureCleanupStep]
	gate         execution.AdmissionGate
	workers      *execution.Workers
	inspector    InstanceInspector
	ownership    SandboxOwnershipReader
	faultPoint   func(string) error
}

type CaptureCleanupDependencies struct {
	Gate      execution.AdmissionGate
	VMGuards  VMGuards
	Workers   *execution.Workers
	Inspector InstanceInspector
	Ownership SandboxOwnershipReader
}

func NewCaptureCleanup(state *store.State, dependencies CaptureCleanupDependencies, fault func(string) error) *CaptureCleanup {
	return &CaptureCleanup{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Workers.Context(),
		storeWriteMu: dependencies.VMGuards.StoreWriteMu,
		steps:        newVMSteps(state.Database(), dependencies.VMGuards.LifecycleLocks, LoadCaptureCleanup),
		gate:         dependencies.Gate,
		workers:      dependencies.Workers,
		inspector:    dependencies.Inspector,
		ownership:    dependencies.Ownership,
		faultPoint:   fault,
	}
}

func (cc *CaptureCleanup) fault(point string) error {
	if cc.faultPoint != nil {
		return cc.faultPoint(point)
	}
	return nil
}

func (cc *CaptureCleanup) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(cc.ctx, cc.inspector, generations)
}

func (cc *CaptureCleanup) dispatch(id string) {
	if cc.store.Reviewed {
		return
	}
	cc.workers.Start(id, func() error {
		handled, err := cc.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("raw cleanup lacks matching immutable step")
		}
		return nil
	})
}

func (cc *CaptureCleanup) Resume(id string) (bool, error) {
	if cc.ownership == nil {
		return false, fmt.Errorf("%w: required CaptureCleanup capability port unavailable", errs.ErrManualRecovery)
	}

	if cc.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume raw cleanup", errs.ErrManualRecovery)
	}
	return cc.steps.resume(cc.ctx, id, cc.runCaptureCleanup)
}

func ValidateCaptureCleanup(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadCaptureCleanup(ctx, database, id)
	return err
}

type (
	CaptureCleanupRequest struct {
		TenantID           string `json:"tenant_id"`
		Key                string `json:"key"`
		CaptureOperationID string `json:"capture_operation_id"`
		StopOperationID    string `json:"stop_operation_id"`
	}
	captureCleanupInput struct {
		Request CaptureCleanupRequest
		Capture CaptureStep
		Stop    StopStep
	}
	CaptureCleanupStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       captureCleanupInput
	}
)

func (step CaptureCleanupStep) sourceVMID() string {
	return step.Input.Capture.sourceVMID()
}

func LoadCaptureCleanup(ctx context.Context, q store.DBTX, id string) (CaptureCleanupStep, error) {
	step := CaptureCleanupStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "capture_cleanup",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input := record.Input
	candidate, expected, action := record.CandidateResourceID, record.ExpectedInstance, record.Action
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("raw capture cleanup checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	step.Phase = record.Phase.String
	if action != "capture_cleanup" || candidate != step.sourceVMID() || expected != step.Input.Capture.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("raw capture cleanup identity changed")
	}
	return step, nil
}

func (cc *CaptureCleanup) Admit(ctx context.Context, request CaptureCleanupRequest, parent ResourceParent) (operation.State, error) {
	if cc.
		ownership ==
		nil {
		return operation.State{},
			fmt.Errorf(
				"%w: required CaptureCleanup capability port unavailable",

				errs.ErrManualRecovery,
			)
	}

	var empty operation.State
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.CaptureOperationID, "op-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: raw capture cleanup requires original capture and stop proof", errs.ErrInvalid)
	}
	frozen := request.operationRequest()
	cc.storeWriteMu.Lock()
	defer cc.storeWriteMu.Unlock()
	if err := cc.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := cc.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).LookupForReplay(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, frozen); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			cc.dispatch(cached.ID)
		}
		return cached, err
	}
	capture, err := LoadCapture(ctx, tx, request.CaptureOperationID)
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, capture.sourceVMID(), parent); err != nil {
		return empty, err
	}
	if authority, ok := parent.(CaptureCleanupParentAuthority); ok {
		if err = authority.VerifyCaptureCleanup(ctx, tx, capture, request.StopOperationID); err != nil {
			return empty, err
		}
	}
	stopped, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if capture.Input.Request.TenantID != request.TenantID || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != capture.Input.Source.OperationID || stopped.Input.SourceHash != capture.Input.Source.Hash || stopped.Input.Request.ExpectedInstance != capture.Input.Request.ExpectedInstance || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != capture.Input.Request.SandboxID {
		return empty, fmt.Errorf("%w: raw capture stop proof differs from captured generation", errs.ErrConflict)
	}
	if err = CaptureCleanupProof(ctx, tx, capture, CaptureCleanupTarget{
		VMID:     capture.sourceVMID(),
		Instance: capture.Input.Request.ExpectedInstance,
		Status:   "pending",
	}); err != nil {
		return empty, err
	}
	source, err := LoadResourceStep(ctx, tx, capture.Input.Source.OperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(source, capture.Input.Source) {
		return empty, fmt.Errorf("%w: raw capture source history changed", errs.ErrConflict)
	}
	currentMachine, instance, owner, err := LoadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != source.State.Witness || currentMachine.Deleting || currentMachine.AgentReady || !currentMachine.Verified {
		return empty, fmt.Errorf("%w: raw capture source is not stopped and unowned", errs.ErrConflict)
	}
	sandbox, err := cc.ownership.ReadSandbox(ctx, tx, capture.Input.Request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if sandbox.SourceOperation != source.OperationID || sandbox.Instance != source.State.Witness || sandbox.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw capture source ownership changed", errs.ErrConflict)
	}
	operations := operation.New(tx)
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operation.Result{
			SandboxID:  sandbox.ID,
			VMID:       source.Candidate,
			SnapshotID: capture.Input.Snapshot.ID,
		},
	}
	input := captureCleanupInput{
		Request: request,
		Capture: capture,
		Stop:    stopped,
	}
	payload := operation.Encode(input)
	if err = AdmitResourceParent(ctx, tx, parent, ResourceAdmission{
		Child:     accepted,
		Kind:      "capture_cleanup",
		Candidate: source.Candidate,
		Request:   frozen,
	}); err != nil {
		return empty, err
	}
	if err = operations.RecordAdmission(ctx, operation.Admission{
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
		StepKey:             "capture_cleanup",
		Action:              "capture_cleanup",
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Phase:               "admitted",
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).TransferPendingCleanupResource(ctx, dbgen.TransferPendingCleanupResourceParams{
		ID:           "capture:" + capture.OperationID,
		OperationID:  capture.OperationID,
		ResourceKind: "capture",
		ResourceID:   source.Candidate,
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "raw:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: "capture_raw",
		ResourceID:   source.Candidate,
		StepKey:      "capture_cleanup",
		WitnessHash:  operation.Digest(payload),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ClaimStopped(ctx, dbgen.ClaimStoppedParams{
		ID:       source.Candidate,
		Instance: source.State.Witness,
		Owner: sql.NullString{
			String: accepted.ID,
			Valid:  true,
		},
	})); err != nil {
		return empty, err
	}
	if err = cc.fault("capture_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = cc.store.Checkpoint(); err != nil {
		return accepted, err
	}
	cc.dispatch(accepted.ID)
	return accepted, nil
}

func (cc *CaptureCleanup) captureRawProof(tx *sql.Tx, step CaptureCleanupStep) error {
	current, err := LoadCaptureCleanup(cc.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("raw capture cleanup intent changed")
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	if err = CaptureCleanupProof(cc.ctx, tx, capture, CaptureCleanupTarget{
		VMID:     source.Candidate,
		Instance: source.State.Witness,
		Status:   "transferred",
	}); err != nil {
		return err
	}
	stopped, err := LoadStop(cc.ctx, tx, step.Input.Stop.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stopped, step.Input.Stop) {
		return fmt.Errorf("raw capture original stop proof changed")
	}
	vm, instance, owner, err := LoadVM(cc.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != step.OperationID || instance != source.State.Witness || vm.Deleting || vm.AgentReady {
		return fmt.Errorf("raw capture current stopped ownership changed")
	}
	count, err := quota.CountResourceReference(cc.ctx, tx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("raw capture responsibility missing: %v", err)
	}
	projection, err := operation.New(tx).LoadProjectionSource(cc.ctx, step.OperationID)
	if err != nil {
		return err
	}
	result := projection.Result
	if operation.Terminal(projection.Status) || projection.ScopeKind != "tenant" || projection.ScopeID != source.Input.TenantID || !projection.Request.Equal(step.Input.Request.operationRequest()) || result.SandboxID != source.Input.SandboxID || result.VMID != source.Candidate || result.SnapshotID != capture.Input.Snapshot.ID {
		return fmt.Errorf("raw capture operation authority changed")
	}
	return nil
}

func (cc *CaptureCleanup) checkCaptureRaw(step CaptureCleanupStep) error {
	cc.storeWriteMu.Lock()
	defer cc.storeWriteMu.Unlock()
	tx, err := cc.store.Database.BeginTx(cc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return cc.captureRawProof(tx, step)
}

func (cc *CaptureCleanup) captureRawReceipt(step *CaptureCleanupStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	capture := step.Input.Capture
	raw := capture.State.Boundary.Path
	intent, err := store.LoadDeletionReceipt(cc.ctx, cc.store.Database, step.OperationID, "capture_cleanup", raw)

	if err == nil {
		if step.Phase != "deleting" {
			return intent, fmt.Errorf("raw capture receipt outside deletion phase")
		}
		return intent, loom.VerifyPublishedCaptureRawDeletion(intent, raw, capture.State.Boundary)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("raw capture durable receipt missing: %w", err)
	}
	intent, err = loom.InspectPublishedCaptureRawDeletion(cc.ctx, cc.store.Directory, loom.CaptureRawDeletionTarget{
		Resource:    capture.Input.Source.Input.Resource,
		OperationID: capture.OperationID,
		Boundary:    capture.State.Boundary,
		Digests:     capture.State.Files,
	})
	if err != nil {
		return intent, err
	}
	cc.storeWriteMu.Lock()
	defer cc.storeWriteMu.Unlock()
	tx, err := cc.store.Database.BeginTx(cc.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = cc.captureRawProof(tx, *step); err != nil {
		return intent, err
	}

	if err = store.RecordDeletionReceipt(cc.ctx, tx, step.OperationID, "capture_cleanup", intent); err != nil {
		return intent, err
	}
	if err = operation.AdvancePhase(cc.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "capture_cleanup",
		InputHash:      step.Hash,
		PreviousStatus: "pending",
		PreviousPhase:  step.Phase,
		NextStatus:     "running",
		NextPhase:      "deleting",
	}); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = cc.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, cc.fault("capture_cleanup_receipt")
}

func (cc *CaptureCleanup) runCaptureCleanup(step CaptureCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := cc.checkCaptureRaw(step); err != nil {
		return err
	}
	source := step.Input.Capture.Input.Source
	if err := cc.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	intent, err := cc.captureRawReceipt(&step)
	if err != nil {
		return err
	}
	paths, err := loom.ResumeDeletion(cc.ctx, cc.store.Directory, intent)
	if err != nil {
		return err
	}
	if err = loom.CheckLocalReaders(paths); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(intent); err != nil {
		return err
	}
	if err = cc.checkCaptureRaw(step); err != nil {
		return err
	}
	for _, path := range paths {
		if err = loom.RemoveDeletionFile(cc.ctx, cc.store.Directory, intent, path); err != nil {
			return err
		}
		if err = cc.fault("capture_cleanup_unlinked"); err != nil {
			return err
		}
	}
	if err = loom.RemoveDeletionDirectories(cc.ctx, cc.store.Directory, intent); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(intent); err != nil {
		return err
	}
	if err = cc.fault("capture_cleanup_files_removed"); err != nil {
		return err
	}
	if err = cc.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	return cc.completeCaptureRaw(step, intent)
}

func (cc *CaptureCleanup) completeCaptureRaw(step CaptureCleanupStep, intent loom.DeletionIntent) error {
	cc.storeWriteMu.Lock()
	defer cc.storeWriteMu.Unlock()
	tx, err := cc.store.Database.BeginTx(cc.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = cc.captureRawProof(tx, step); err != nil {
		return err
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	if err = quota.RemoveResourceReference(cc.ctx, tx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	if err = quota.RemoveResourceReference(cc.ctx, tx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(cc.ctx, dbgen.CompleteTransferredCleanupParams{
		ID:          "capture:" + capture.OperationID,
		OperationID: capture.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(cc.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "raw:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseStoppedOwnership(cc.ctx, dbgen.ReleaseStoppedOwnershipParams{
		ID:              source.Candidate,
		CurrentInstance: source.State.Witness,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = store.RetireDeletionReceipt(cc.ctx, tx, step.OperationID, "capture_cleanup", intent); err != nil {
		return err
	}
	if err = operation.AdvancePhase(cc.ctx, tx, operation.PhaseChange{
		OperationID:    step.OperationID,
		StepKey:        "capture_cleanup",
		InputHash:      step.Hash,
		PreviousStatus: "running",
		PreviousPhase:  step.Phase,
		NextStatus:     "succeeded",
		NextPhase:      "complete",
	}); err != nil {
		return err
	}
	if err = operations.MarkSucceeded(cc.ctx, step.OperationID); err != nil {
		return err
	}
	if err = cc.fault("capture_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return cc.store.Checkpoint()
}

// CaptureCleanupParentAuthority verifies a frozen rollback's raw capture and
// original stop child before resource cleanup admission. It performs no effects.
type CaptureCleanupParentAuthority interface {
	// VerifyCaptureCleanup checks the parent's authority over this capture and stop in the caller's transaction.
	VerifyCaptureCleanup(ctx context.Context, tx *sql.Tx, capture CaptureStep, stopOperationID string) error
}

// AllocationCleanupFence keeps a detached pool member unavailable to every new
// resource workflow while its independent collector retains responsibility.
func AllocationCleanupFence(ctx context.Context, database store.DBTX, vm string, parent ResourceParent) error {
	pending, err := dbgen.New(database).CountPendingCleanupResource(ctx, dbgen.CountPendingCleanupResourceParams{
		ResourceKind: "direct_policy",
		ResourceID:   vm,
	})
	if err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: allocation has unresolved direct policy retirement", errs.ErrConflict)
	}
	owner, err := dbgen.New(database).PendingCleanupResourceOwner(ctx, dbgen.PendingCleanupResourceOwnerParams{
		ResourceKind: "supply_cleanup",
		ResourceID:   vm,
	})

	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	collector, ok := parent.(SupplyCleanupAuthority)
	if !ok {
		return fmt.Errorf("%w: allocation is fenced by independent supply cleanup", errs.ErrConflict)
	}
	collectorID, valid := collector.SupplyCleanupOperation()
	if !valid || collectorID != owner {
		return fmt.Errorf("%w: allocation is fenced by independent supply cleanup", errs.ErrConflict)
	}
	return nil
}
