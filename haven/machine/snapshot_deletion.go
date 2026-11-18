package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type SnapshotDeletionGuards interface {
	Admission() execution.Guard
	Snapshot(string) execution.Guard
}

// SnapshotDeletion coordinates one immutable deletion intent and its retained
// responsibility. The shared execution and persistence owners outlive this service.
type SnapshotDeletion struct {
	store      store.Workflow
	proofs     SnapshotProofs
	usage      SnapshotUsageReader
	ctx        context.Context
	admission  execution.Guard
	guards     SnapshotDeletionGuards
	lifetime   *execution.Lifetime
	workers    *execution.Workers
	faultPoint func(string) error
}

type SnapshotDeletionDependencies struct {
	Proofs   SnapshotProofs
	Guards   SnapshotDeletionGuards
	Lifetime *execution.Lifetime
	Workers  *execution.Workers
	Usage    SnapshotUsageReader
}

func NewSnapshotDeletion(state *store.State, dependencies SnapshotDeletionDependencies, fault func(string) error) *SnapshotDeletion {
	return &SnapshotDeletion{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		proofs:     dependencies.Proofs,
		usage:      dependencies.Usage,
		ctx:        dependencies.Workers.Context(),
		admission:  dependencies.Guards.Admission(),
		guards:     dependencies.Guards,
		lifetime:   dependencies.Lifetime,
		workers:    dependencies.Workers,
		faultPoint: fault,
	}
}

func (sd *SnapshotDeletion) fault(point string) error {
	if sd.faultPoint != nil {
		return sd.faultPoint(point)
	}
	return nil
}

func (sd *SnapshotDeletion) dispatch(id string) {
	if sd.store.Reviewed {
		return
	}
	sd.workers.Start(id, func() error {
		handled, err := sd.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("snapshot deletion lacks matching immutable step")
		}
		return nil
	})
}

func (sd *SnapshotDeletion) Resume(id string) (bool, error) {
	if sd.
		usage == nil {
		return false,
			fmt.
				Errorf("%w: required SnapshotDeletion capability port unavailable",
					errs.ErrManualRecovery,
				)
	}

	if sd.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume snapshot deletion", errs.ErrManualRecovery)
	}
	step, err := LoadSnapshotDelete(sd.ctx, sd.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := sd.guards.Snapshot(step.Input.Request.SnapshotID)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadSnapshotDelete(sd.ctx, sd.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, sd.runSnapshotDelete(step)
}

func ValidateSnapshotDelete(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadSnapshotDelete(ctx, database, id)
	return err
}

type SnapshotDeleteRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}
type (
	snapshotDeleteInput struct {
		Request SnapshotDeleteRequest
		Source  SnapshotSource
	}
	SnapshotDeleteStep struct {
		OperationID string
		Hash        string
		Status      string
		Phase       string
		Input       snapshotDeleteInput
	}
)

func LoadSnapshotDelete(ctx context.Context, q store.DBTX, id string) (SnapshotDeleteStep, error) {
	step := SnapshotDeleteStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "snapshot_delete",
	})
	if err != nil {
		record = dbgen.GetStepRow{}
	}
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if err != nil {
		return step, err
	}
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("snapshot delete input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "snapshot_delete" || expected != "" || candidate != step.Input.Request.SnapshotID || candidate != step.Input.Source.Snapshot.ID {
		return step, fmt.Errorf("snapshot delete frozen identity changed")
	}
	return step, nil
}
