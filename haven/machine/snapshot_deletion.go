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

func NewSnapshotDeletion(state *store.State, proofs SnapshotProofs, guards SnapshotDeletionGuards, lifetime *execution.Lifetime, workers *execution.Workers, fault func(string) error, usage SnapshotUsageReader) *SnapshotDeletion {
	return &SnapshotDeletion{
		store: store.Workflow{
			Database:   state.Database(),
			Directory:  state.Directory(),
			Reviewed:   state.Reviewed(),
			Checkpoint: state.Checkpoint,
		},
		proofs:     proofs,
		usage:      usage,
		ctx:        workers.Context(),
		admission:  guards.Admission(),
		guards:     guards,
		lifetime:   lifetime,
		workers:    workers,
		faultPoint: fault,
	}
}

func (service *SnapshotDeletion) fault(point string) error {
	if service.faultPoint != nil {
		return service.faultPoint(point)
	}
	return nil
}

func (service *SnapshotDeletion) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		handled, err := service.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("snapshot deletion lacks matching immutable step")
		}
		return nil
	})
}

func (service *SnapshotDeletion) Resume(id string) (bool, error) {
	if service.
		usage == nil {
		return false,
			fmt.
				Errorf("%w: required SnapshotDeletion capability port unavailable",
					errs.ErrManualRecovery,
				)
	}

	if service.store.Reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume snapshot deletion", errs.ErrManualRecovery)
	}
	step, err := LoadSnapshotDelete(service.ctx, service.store.Database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := service.guards.Snapshot(step.Input.Request.SnapshotID)
	guard.Lock()
	defer guard.Unlock()
	step, err = LoadSnapshotDelete(service.ctx, service.store.Database, id)
	if err != nil {
		return true, err
	}
	return true, service.runSnapshotDelete(step)
}

func ValidateSnapshotDelete(ctx context.Context, database store.DBTX, id string) error {
	_, err := LoadSnapshotDelete(ctx, database, id)
	return err
}
