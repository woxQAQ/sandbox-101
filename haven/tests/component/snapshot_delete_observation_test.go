package component

import (
	"context"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

type SnapshotDeleteRequest = machine.SnapshotDeleteRequest

type snapshotDeleteStep = machine.SnapshotDeleteStep

func loadSnapshotDelete(ctx context.Context, database store.DBTX, id string) (snapshotDeleteStep, error) {
	return machine.LoadSnapshotDelete(ctx, database, id)
}
