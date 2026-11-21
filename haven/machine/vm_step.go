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

type vmStep interface {
	// sourceVMID identifies the logical VM in the frozen source proof.
	sourceVMID() string
}

// vmSteps associates a recovery route's frozen step loader with the shared VM domain.
type vmSteps[S vmStep] struct {
	database         store.DBTX
	vmLifecycleLocks *execution.KeyedLocks[string]
	load             func(context.Context, store.DBTX, string) (S, error)
}

func newVMSteps[S vmStep](database store.DBTX, vmLifecycleLocks *execution.KeyedLocks[string], load func(context.Context, store.DBTX, string) (S, error)) vmSteps[S] {
	return vmSteps[S]{
		database:         database,
		vmLifecycleLocks: vmLifecycleLocks,
		load:             load,
	}
}

// The first read routes recovery. Only the step reloaded under the VM lock may
// advance effects. A missing second read is an error on an already matched route.
func (steps vmSteps[S]) resume(ctx context.Context, id string, run func(S) error) (bool, error) {
	original, err := steps.load(ctx, steps.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	vmID := original.sourceVMID()
	return true, steps.vmLifecycleLocks.With(vmID, func() error {
		current, err := steps.load(ctx, steps.database, id)
		if err != nil {
			return err
		}
		if current.sourceVMID() != vmID {
			return fmt.Errorf("%w: source VM changed for operation %s", errs.ErrManualRecovery, id)
		}
		return run(current)
	})
}
