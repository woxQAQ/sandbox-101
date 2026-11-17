package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
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
