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
	store      store.Workflow
	ctx        context.Context
	admission  execution.Guard
	guards     VMGuards
	gate       execution.AdmissionGate
	workers    *execution.Workers
	inspector  InstanceInspector
	ownership  SandboxOwnershipReader
	faultPoint func(string) error
}

type CaptureCleanupDependencies struct {
	Gate      execution.AdmissionGate
	Guards    VMGuards
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
		ctx:        dependencies.Workers.Context(),
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		gate:       dependencies.Gate,
		workers:    dependencies.Workers,
		inspector:  dependencies.Inspector,
		ownership:  dependencies.Ownership,
		faultPoint: fault,
	}
}

func (service *CaptureCleanup) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *CaptureCleanup) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(service.ctx, service.inspector, generations)
}

func (service *CaptureCleanup) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("raw cleanup lacks matching immutable step")
		}
		return nil
	})
}

func (service *CaptureCleanup) Resume(id string) (bool, error) {
	if service.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required CaptureCleanup capability port unavailable",
				errs.
					ErrManualRecovery)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume raw cleanup", errs.ErrManualRecovery)
	}
	step, err := LoadCaptureCleanup(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.VM(step.Input.Capture.Input.Source.Candidate)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadCaptureCleanup(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runCaptureCleanup(step)
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
	input, body := record.Input, record.Body
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
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "capture_cleanup" || candidate != step.Input.Capture.Input.Source.Candidate || expected != step.Input.Capture.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("raw capture cleanup identity changed")
	}
	return step, nil
}

func (service *CaptureCleanup) Admit(ctx context.Context, request CaptureCleanupRequest, parent ResourceParent) (operation.Record, error) {
	if service.
		ownership ==
		nil {
		return operation.
				Record{},
			fmt.Errorf(
				"%w: required CaptureCleanup capability port unavailable",

				errs.ErrManualRecovery,
			)
	}

	var empty operation.Record
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.CaptureOperationID, "op-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: raw capture cleanup requires original capture and stop proof", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request CaptureCleanupRequest
	}{"capture_cleanup", request})
	hash := operation.Digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := operation.New(tx).Cached(ctx, operation.Scope{
		Kind: "tenant",
		ID:   request.TenantID,
	}, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	capture, err := LoadCapture(ctx, tx, request.CaptureOperationID)
	if err != nil {
		return empty, err
	}
	if err = AllocationCleanupFence(ctx, tx, capture.Input.Source.Candidate, parent); err != nil {
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
		VMID:     capture.Input.Source.Candidate,
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
	sandbox, err := service.ownership.ReadSandbox(ctx, tx, capture.Input.Request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if sandbox.SourceOperation != source.OperationID || sandbox.Instance != source.State.Witness || sandbox.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw capture source ownership changed", errs.ErrConflict)
	}
	operations := operation.New(tx)
	accepted := operation.Record{
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
		Child:       accepted,
		Kind:        "capture_cleanup",
		Candidate:   source.Candidate,
		RequestHash: hash,
	}); err != nil {
		return empty, err
	}
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
		StepKey:             "capture_cleanup",
		Action:              "capture_cleanup",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: source.Candidate,
		ExpectedInstance:    source.State.Witness,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	})); err != nil {
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
		Body:         payload,
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
	if err = service.fault("capture_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}

func (service *CaptureCleanup) captureRawProof(tx *sql.Tx, step CaptureCleanupStep) error {
	current, err := LoadCaptureCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("raw capture cleanup intent changed")
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	if err = CaptureCleanupProof(service.ctx, tx, capture, CaptureCleanupTarget{
		VMID:     source.Candidate,
		Instance: source.State.Witness,
		Status:   "transferred",
	}); err != nil {
		return err
	}
	stopped, err := LoadStop(service.ctx, tx, step.Input.Stop.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stopped, step.Input.Stop) {
		return fmt.Errorf("raw capture original stop proof changed")
	}
	vm, instance, owner, err := LoadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != step.OperationID || instance != source.State.Witness || vm.Deleting || vm.AgentReady {
		return fmt.Errorf("raw capture current stopped ownership changed")
	}
	count, err := quota.New(tx).CountResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	})
	if err != nil || count != 1 {
		return fmt.Errorf("raw capture responsibility missing: %v", err)
	}
	projection, err := operation.New(tx).Projection(service.ctx, step.OperationID)
	if err != nil {
		return err
	}
	body, request := projection.Body, projection.Request
	status, scope, tenant, hash := projection.Status, projection.ScopeKind, projection.ScopeID, projection.RequestHash
	var result operation.Result
	var frozen struct {
		Action  string
		Request CaptureCleanupRequest
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if err = json.Unmarshal(request, &frozen); err != nil {
		return err
	}
	if operation.Terminal(status) || scope != "tenant" || tenant != source.Input.TenantID || operation.Digest(request) != hash || frozen.Action != "capture_cleanup" || frozen.Request != step.Input.Request || result.SandboxID != source.Input.SandboxID || result.VMID != source.Candidate || result.SnapshotID != capture.Input.Snapshot.ID {
		return fmt.Errorf("raw capture operation authority changed")
	}
	return nil
}

func (service *CaptureCleanup) checkCaptureRaw(step CaptureCleanupStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return service.captureRawProof(tx, step)
}

func (service *CaptureCleanup) captureRawReceipt(step *CaptureCleanupStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	capture := step.Input.Capture
	raw := capture.State.Boundary.Path
	body, err := dbgen.New(service.store.Database).GetDeletionReceipt(service.ctx, dbgen.GetDeletionReceiptParams{
		Path:        raw,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
	})

	if err == nil {
		if step.Phase != "deleting" {
			return intent, fmt.Errorf("raw capture receipt outside deletion phase")
		}
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		return intent, loom.VerifyPublishedCaptureRawDeletion(intent, raw, capture.State.Boundary)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("raw capture durable receipt missing: %w", err)
	}
	intent, err = loom.InspectPublishedCaptureRawDeletion(service.ctx, service.store.Directory, loom.CaptureRawDeletionTarget{
		Resource:    capture.Input.Source.Input.Resource,
		OperationID: capture.OperationID,
		Boundary:    capture.State.Boundary,
		Digests:     capture.State.Files,
	})
	if err != nil {
		return intent, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureRawProof(tx, *step); err != nil {
		return intent, err
	}

	if err = store.RequireOne(dbgen.New(tx).InsertDeletionReceipt(service.ctx, dbgen.InsertDeletionReceiptParams{
		Path:        raw,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
		Body:        operation.Encode(intent),
	})); err != nil {
		return intent, err
	}
	if err = store.RequireOne(dbgen.New(tx).BeginPendingStep(service.ctx, dbgen.BeginPendingStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NextBody:     operation.Encode("deleting"),
	})); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.Checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("capture_cleanup_receipt")
}

func (service *CaptureCleanup) runCaptureCleanup(step CaptureCleanupStep) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	if err := service.checkCaptureRaw(step); err != nil {
		return err
	}
	source := step.Input.Capture.Input.Source
	if err := service.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	intent, err := service.captureRawReceipt(&step)
	if err != nil {
		return err
	}
	paths, err := loom.ResumeDeletion(service.ctx, service.store.Directory, intent)
	if err != nil {
		return err
	}
	if err = loom.CheckLocalReaders(paths); err != nil {
		return err
	}
	identities := make([]loom.FileIdentity, 0, len(intent.Nodes))
	for _, identity := range intent.Nodes {
		identities = append(identities, identity)
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.checkCaptureRaw(step); err != nil {
		return err
	}
	for _, path := range paths {
		if err = loom.RemoveDeletionFile(service.ctx, service.store.Directory, intent, path); err != nil {
			return err
		}
		if err = service.fault("capture_cleanup_unlinked"); err != nil {
			return err
		}
	}
	if err = loom.RemoveDeletionDirectories(service.ctx, service.store.Directory, intent); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.fault("capture_cleanup_files_removed"); err != nil {
		return err
	}
	if err = service.inspectStoppedGenerations([]ResourceStep{
		source,
	}); err != nil {
		return err
	}
	return service.completeCaptureRaw(step, intent)
}

func (service *CaptureCleanup) completeCaptureRaw(step CaptureCleanupStep, intent loom.DeletionIntent) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.Database.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureRawProof(tx, step); err != nil {
		return err
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	responsibilities := quota.New(tx)
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "vm",
		ID:         source.Candidate,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	if err = responsibilities.RemoveResourceReference(service.ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         source.Input.Resource.Config.Image,
		HolderKind: "capture",
		HolderID:   capture.OperationID,
	}); err != nil {
		return err
	}
	operations := operation.New(tx)
	if err = store.RequireOne(dbgen.New(tx).CompleteTransferredCleanup(service.ctx, dbgen.CompleteTransferredCleanupParams{
		ID:          "capture:" + capture.OperationID,
		OperationID: capture.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompletePendingCleanup(service.ctx, dbgen.CompletePendingCleanupParams{
		ID:          "raw:" + step.OperationID,
		OperationID: step.OperationID,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).ReleaseStoppedOwnership(service.ctx, dbgen.ReleaseStoppedOwnershipParams{
		ID:              source.Candidate,
		CurrentInstance: source.State.Witness,
		OperationID: sql.NullString{
			String: step.OperationID,
			Valid:  true,
		},
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).DeleteMatchingDeletionReceipt(service.ctx, dbgen.DeleteMatchingDeletionReceiptParams{
		Path:        intent.Path,
		OperationID: step.OperationID,
		StepKey:     "capture_cleanup",
		Body:        operation.Encode(intent),
	})); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteRunningStep(service.ctx, dbgen.CompleteRunningStepParams{
		OperationID:  step.OperationID,
		StepKey:      "capture_cleanup",
		InputHash:    step.Hash,
		PreviousBody: operation.Encode(step.Phase),
		NewBody:      operation.Encode("complete"),
	})); err != nil {
		return err
	}
	if err = operations.Complete(service.ctx, step.OperationID); err != nil {
		return err
	}
	if err = service.fault("capture_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.Checkpoint()
}

// CaptureCleanupParentAuthority verifies a frozen rollback's raw capture and
// original stop child before resource cleanup admission. It performs no effects.
type CaptureCleanupParentAuthority interface {
	VerifyCaptureCleanup(context.Context, *sql.Tx, CaptureStep, string) error
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
