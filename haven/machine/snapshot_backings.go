package machine

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

// Backings enumerates explicit retained users. No name or current VM query
// substitutes for their immutable material and lease handoff proof.
func (proofs SnapshotProofs) Backings(ctx context.Context, tx *sql.Tx, source SnapshotSource) ([]ResourceStep, error) {
	holders, err := quota.New(tx).ResourceHolderIDs(ctx, "snapshot", source.Snapshot.ID, "backing")
	if err != nil {
		return nil, err
	}
	var result []ResourceStep
	for _, holder := range holders {
		id, err := dbgen.New(tx).RestoreBackingOperation(ctx, dbgen.RestoreBackingOperationParams{
			HolderID:   holder,
			SnapshotID: source.Snapshot.ID,
		})
		if err != nil {
			return nil, err
		}
		step, err := LoadResourceStep(ctx, tx, id)
		if err != nil {
			return nil, err
		}
		if err = ValidateResourceStep(proofs.directory, step); err != nil {
			return nil, err
		}
		if step.Input.Restore == nil || !reflect.DeepEqual(*step.Input.Restore, source) || step.Input.BackingHolder != holder || !step.State.BackingReady {
			return nil, fmt.Errorf("snapshot retained backing history changed")
		}
		if err = proofs.RestoreRelations(ctx, tx, step); err != nil {
			return nil, err
		}
		result = append(result, step)
	}
	return result, nil
}

func BackingRoots(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListBackingPaths(ctx)
}
