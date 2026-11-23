package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// CaptureDiscard reclaims a failed capture while retaining the original VM and
// disk. Frozen stop, generation and funding proofs authorize each handoff.
type CaptureDiscard struct {
	store        store.Workflow
	ctx          context.Context
	storeWriteMu execution.Guard
	steps        vmSteps[CaptureDiscardStep]
	lifetime     *execution.Lifetime
	workers      *execution.Workers
	proofs       ResourceProofs
	inspector    InstanceInspector
	ownership    CaptureDiscardOwnershipReader
	faultPoint   func(string) error
}

type CaptureDiscardDependencies struct {
	VMGuards  VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Proofs    ResourceProofs
	Inspector InstanceInspector
	Ownership CaptureDiscardOwnershipReader
}

func NewCaptureDiscard(state *store.State, dependencies CaptureDiscardDependencies, fault func(string) error) *CaptureDiscard {
	return &CaptureDiscard{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Workers.Context(),
		storeWriteMu: dependencies.VMGuards.StoreWriteMu,
		steps:        newVMSteps(state.Database(), dependencies.VMGuards.LifecycleLocks, LoadCaptureDiscard),
		lifetime:     dependencies.Lifetime,
		workers:      dependencies.Workers,
		proofs:       dependencies.Proofs,
		inspector:    dependencies.Inspector,
		ownership:    dependencies.Ownership,
		faultPoint:   fault,
	}
}

func (cd *CaptureDiscard) fault(point string) error {
	if cd.faultPoint != nil {
		return cd.faultPoint(point)
	}
	return nil
}

func (cd *CaptureDiscard) inspectStoppedGenerations(generations []ResourceStep) error {
	return InspectStoppedGenerations(cd.ctx, cd.inspector, generations)
}

func (cd *CaptureDiscard) dispatch(id string) {
	if cd.store.Reviewed {
		return
	}
	cd.workers.Start(id, func() error {
		handled, err := cd.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("capture discard lacks matching immutable step")
		}
		return nil
	})
}

func (cd *CaptureDiscard) Resume(id string) (bool, error) {
	if cd.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required CaptureDiscard capability port unavailable",
				errs.
					ErrManualRecovery)
	}

	if cd.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume capture discard", errs.ErrManualRecovery)
	}
	return cd.steps.resume(cd.ctx, id, cd.runCaptureDiscard)
}

func ValidateCaptureDiscard(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadCaptureDiscard(ctx, database, id)
	return err
}

type CaptureDiscardRequest struct {
	TenantID          string `json:"tenant_id"`
	Key               string `json:"key"`
	SandboxID         string `json:"sandbox_id"`
	FailedOperationID string `json:"failed_operation_id"`
	ExpectedInstance  string `json:"expected_instance"`
	StopOperationID   string `json:"stop_operation_id"`
}

type (
	CaptureDiscardInput struct {
		Lifecycle string `json:"lifecycle,omitempty"`
		Request   CaptureDiscardRequest
		Source    CaptureStep
		StopHash  string
	}
	CaptureDiscardStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       CaptureDiscardInput
	}
)

func (step CaptureDiscardStep) sourceVMID() string {
	return step.Input.Source.sourceVMID()
}

func LoadCaptureDiscard(ctx context.Context, q store.DBTX, id string) (CaptureDiscardStep, error) {
	step := CaptureDiscardStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "capture_discard",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input := record.Input
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("capture discard input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	step.Phase = record.Phase.String
	if action != "capture_discard" || candidate != step.Input.Source.Input.Snapshot.ID || expected != step.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("capture discard frozen identity changed")
	}
	return step, nil
}

// captureDiscardAdmissionProof requires the original failed capture to retain
// custody. The discard operation and its transferred holders do not exist yet.
func (cd *CaptureDiscard) captureDiscardAdmissionProof(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep) error {
	if _, err := cd.verifyCaptureDiscardSource(ctx, tx, step, ""); err != nil {
		return err
	}
	return verifyCaptureDiscardCustody(ctx, tx, step, captureDiscardCustody{
		Deleting:      0,
		CleanupStatus: "blocked",
		Holder: quota.Holder{
			Kind: "engine_dependency",
			ID:   step.Input.Source.OperationID + ":capture",
		},
	})
}

// captureDiscardProof requires admitted ownership and the exact transferred
// cleanup responsibility before effects or completion can proceed.
func (cd *CaptureDiscard) captureDiscardProof(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep) error {
	record, err := cd.verifyCaptureDiscardSource(ctx, tx, step, step.OperationID)
	if err != nil {
		return err
	}
	if step.Input.Lifecycle != "" {
		if err = cd.ownership.VerifyCaptureDiscardLink(ctx, tx, step); err != nil {
			return err
		}
	}
	if err = verifyCaptureDiscardCustody(ctx, tx, step, captureDiscardCustody{
		Deleting:      1,
		CleanupStatus: "transferred",
		Holder: quota.Holder{
			Kind: "cleanup",
			ID:   step.OperationID + ":capture_discard",
		},
	}); err != nil {
		return err
	}
	input, request := step.Input, step.Input.Request
	snapshot := input.Source.Input.Snapshot
	resource := input.Source.Input.Source
	count, err := dbgen.New(tx).MatchingCleanupCount(ctx, dbgen.MatchingCleanupCountParams{
		ID:           "capture_discard:" + step.OperationID,
		OperationID:  step.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   snapshot.ID,
		Status:       "pending",
		WitnessHash:  operation.Digest(operation.Encode(input)),
	})
	if err != nil || count != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	// Decode the discard result independently; missing fields cannot inherit
	// identity from the original failed capture result.
	projection, err := operation.New(tx).LoadProjectionSource(ctx, step.OperationID)
	if err != nil {
		return err
	}
	result := projection.Result
	if !projection.Request.Equal(request.operationRequest()) || operation.Terminal(projection.Status) || projection.ScopeKind != "tenant" || projection.ScopeID != request.TenantID || result.SnapshotID != snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != record.ID {
		return fmt.Errorf("capture discard parent identity changed")
	}
	return nil
}

// captureDiscardCustody is the exact responsibility expected at one durable
// boundary. These facts are matched directly against storage, without selecting
// execution paths or inferring a holder from current catalog state.
type captureDiscardCustody struct {
	Deleting      int64
	CleanupStatus string
	Holder        quota.Holder
}

func verifyCaptureDiscardCustody(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep, expected captureDiscardCustody) error {
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	snapshot := source.Input.Snapshot
	publication, err := dbgen.New(tx).GetCapture(ctx, dbgen.GetCaptureParams{
		ID:         snapshot.ID,
		SourceVmID: resource.Candidate,
	})
	if err != nil {
		return err
	}
	var record SnapshotRecord
	if record, err = snapshotRecord(ctx, tx, publication); err != nil {
		return err
	}
	if publication.Published != 0 || publication.Deleting != expected.Deleting || !reflect.DeepEqual(record, snapshot) {
		return fmt.Errorf("capture discard unpublished catalog changed")
	}
	body, err := dbgen.New(tx).CleanupBody(ctx, dbgen.CleanupBodyParams{
		ID:           "capture:" + source.OperationID,
		OperationID:  source.OperationID,
		ResourceKind: "snapshot",
		ResourceID:   snapshot.ID,
		Status:       expected.CleanupStatus,
	})
	if err != nil {
		return err
	}
	if body != source.Hash {
		return fmt.Errorf("capture discard original cleanup changed")
	}
	shape := resource.Input.Resource.Config.Shape
	if count, err := quota.CountMatchingReservation(ctx, tx, quota.Reservation{
		Owner: "snapshot:" + snapshot.ID,
		Bytes: (2*shape.MemoryMiB + shape.DiskMiB + 224) << 20,
	}); err != nil || count != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	checks := []struct {
		count func() (int64, error)
		want  int64
	}{
		{func() (int64, error) {
			return quota.CountMatchingCost(ctx, tx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        request.TenantID,
					Kind:            "pending_storage",
					Key:             source.OperationID + ":capture",
					SourceOperation: source.OperationID,
				},
				StorageMiB: shape.DiskMiB + shape.MemoryMiB,
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountTenantCostHolders(ctx, tx, request.TenantID, "pending_storage", source.OperationID+":capture")
		}, 1},
		{func() (int64, error) {
			return quota.CountCostReference(ctx, tx, quota.CostReference{
				TenantID:   request.TenantID,
				Kind:       "pending_storage",
				Key:        source.OperationID + ":capture",
				HolderKind: expected.Holder.Kind,
				HolderID:   expected.Holder.ID,
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "vm",
				ID:         resource.Candidate,
				HolderKind: "capture",
				HolderID:   source.OperationID,
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountResourceReference(ctx, tx, quota.ResourceReference{
				Kind:       "image",
				ID:         resource.Input.Resource.Config.Image,
				HolderKind: "capture",
				HolderID:   source.OperationID,
			})
		}, 1},
		{func() (int64, error) {
			return quota.CountResourceHolders(ctx, tx, "snapshot", snapshot.ID)
		}, 0},
	}
	for _, check := range checks {
		if count, err := check.count(); err != nil || count != check.want {
			return fmt.Errorf("capture discard responsibility changed: %v", err)
		}
	}
	owners, err := dbgen.New(tx).CountCaptureOwner(ctx, dbgen.CountCaptureOwnerParams{
		SnapshotID:        snapshot.ID,
		TenantID:          request.TenantID,
		SourceOperationID: source.OperationID,
	})
	if err != nil || owners != 1 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	publications, err := dbgen.New(tx).CountSnapshotPublication(ctx, snapshot.ID)
	if err != nil || publications != 0 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	leases, err := CountSnapshotSourceLeases(ctx, tx, snapshot.ID)
	if err != nil || leases != 0 {
		return fmt.Errorf("capture discard responsibility changed: %v", err)
	}
	return nil
}
