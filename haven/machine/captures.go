package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/store"
)

type CaptureBackend interface {
	InstanceInspector

	loom.SnapshotCompatibility
	// Pause suspends guest execution before capture.
	Pause(ctx context.Context, resource loom.Resource) error
	// Resume continues guest execution after a pause.
	Resume(ctx context.Context, resource loom.Resource) error
	// Capture writes the paused VM state to the supplied directory.
	Capture(ctx context.Context, resource loom.Resource, directory string) error
}

// Captures freezes source custody and peak responsibility before pausing, then
// resumes the source before completing durable snapshot publication.
type Captures struct {
	store        store.Workflow
	ctx          context.Context
	storeWriteMu execution.Guard
	steps        vmSteps[CaptureStep]
	captureSlot  chan struct{}
	lifetime     *execution.Lifetime
	workers      *execution.Workers
	backend      CaptureBackend
	ownership    SandboxOwnershipReader
	safetyBytes  int64
	faultPoint   func(string) error
}

type CapturesDependencies struct {
	VMGuards  VMGuards
	Lifetime  *execution.Lifetime
	Workers   *execution.Workers
	Backend   CaptureBackend
	Ownership SandboxOwnershipReader
}

type CapturesOptions struct {
	SafetyBytes int64
	Fault       func(string) error
}

func NewCaptures(state *store.State, dependencies CapturesDependencies, options CapturesOptions) *Captures {
	return &Captures{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          dependencies.Workers.Context(),
		storeWriteMu: dependencies.VMGuards.StoreWriteMu,
		steps:        newVMSteps(state.Database(), dependencies.VMGuards.LifecycleLocks, LoadCapture),
		captureSlot:  make(chan struct{}, 1),
		lifetime:     dependencies.Lifetime,
		workers:      dependencies.Workers,
		backend:      dependencies.Backend,
		ownership:    dependencies.Ownership,
		safetyBytes:  options.SafetyBytes,
		faultPoint:   options.Fault,
	}
}

func (c *Captures) fault(point string) error {
	if c.faultPoint != nil {
		return c.faultPoint(point)
	}
	return nil
}

func (c *Captures) validateStep(step ResourceStep) error {
	return ValidateResourceStep(c.store.Directory, step)
}

func (c *Captures) dispatch(id string) {
	if c.store.Reviewed {
		return
	}
	c.workers.Start(id, func() error {
		handled, err := c.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("capture lacks matching immutable step")
		}
		return nil
	})
}

func (c *Captures) Resume(id string) (bool, error) {
	if c.
		ownership ==
		nil {
		return false,

			fmt.Errorf("%w: required Captures capability port unavailable",
				errs.ErrManualRecovery,
			)
	}

	if c.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume capture", errs.ErrManualRecovery)
	}
	return c.steps.resume(c.ctx, id, c.runCapture)
}

type CaptureRequest struct {
	TenantID         string `json:"tenant_id"`
	Key              string `json:"key"`
	SandboxID        string `json:"sandbox_id"`
	ExpectedInstance string `json:"expected_instance"`
}

type CaptureInput struct {
	Request      CaptureRequest    `json:"request"`
	Source       ResourceStep      `json:"source"`
	Sandbox      ownership.Sandbox `json:"sandbox"`
	Snapshot     SnapshotRecord    `json:"snapshot"`
	Backend      string            `json:"backend"`
	Architecture string            `json:"architecture"`
	CPU          string            `json:"cpu"`
}

type CaptureState struct {
	Phase           string                `json:"phase"`
	Boundary        *loom.CaptureBoundary `json:"boundary,omitempty"`
	ResumeRequired  bool                  `json:"resume_required"`
	Captured        bool                  `json:"captured"`
	Materialized    bool                  `json:"materialized"`
	Files           map[string]string     `json:"files,omitempty"`
	ArtifactsSynced bool                  `json:"artifacts_synced"`
	Error           string                `json:"error,omitempty"`
}

type CaptureStep struct {
	OperationID string
	Hash        string
	Status      string
	Input       CaptureInput
	State       CaptureState
}

func (step CaptureStep) sourceVMID() string {
	return step.Input.Source.Candidate
}

func LoadCapture(ctx context.Context, query store.DBTX, operationID string) (CaptureStep, error) {
	var step CaptureStep
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		step, err = readCapture(ctx, tx, operationID)
		return err
	})
	return step, err
}

func readCapture(ctx context.Context, query *sql.Tx, operationID string) (CaptureStep, error) {
	step := CaptureStep{
		OperationID: operationID,
	}
	record, err := dbgen.New(query).GetStep(ctx, dbgen.GetStepParams{
		OperationID: operationID,
		StepKey:     "capture",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	input := record.Input
	action, candidate, instance := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(input) != step.Hash {
		return step, fmt.Errorf("capture input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	queries := dbgen.New(query)
	progress, err := queries.ReadCaptureProgress(ctx, operationID)
	if err != nil {
		return step, fmt.Errorf("cannot read capture progress for %s: %v", operationID, err)
	}
	step.State = CaptureState{
		Phase:           record.Phase.String,
		Error:           record.StepError,
		ResumeRequired:  progress.ResumeRequired,
		Captured:        progress.Captured,
		Materialized:    progress.Materialized,
		ArtifactsSynced: progress.ArtifactsSynced,
	}
	b, err := queries.GetCaptureBoundary(ctx, operationID)
	if err == nil {
		step.State.Boundary = &loom.CaptureBoundary{
			Path:        b.Path,
			Parent:      fileIdentity(b.ParentDevice, b.ParentInode, b.ParentMode),
			Publication: fileIdentity(b.PublicationDevice, b.PublicationInode, b.PublicationMode),
		}
	} else if err != sql.ErrNoRows {
		return step, err
	}
	files, err := queries.ListCaptureFiles(ctx, operationID)
	if err != nil {
		return step, err
	}
	if len(files) > 0 {
		step.State.Files = make(map[string]string, len(files))
		for _, file := range files {
			step.State.Files[file.Path] = file.Digest
		}
	}
	if action != "snapshot" || candidate != step.Input.Snapshot.ID || instance != step.Input.Request.ExpectedInstance || !loom.ValidResourceID(candidate, "snap-") {
		return step, fmt.Errorf("capture identity changed")
	}
	return step, nil
}
