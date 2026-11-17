package machine

import (
	"context"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Lease pins a source publication for one frozen operation step.
type Lease struct {
	SnapshotID  string
	OperationID string
	StepKey     string
}

func SourceLeases(ctx context.Context, query store.DBTX) ([]Lease, error) {
	rows, err := dbgen.New(query).ListSourceLeases(ctx)
	if err != nil {
		return nil, err
	}
	var leases []Lease
	for _, row := range rows {
		leases = append(leases, Lease{
			SnapshotID:  row.SnapshotID,
			OperationID: row.OperationID,
			StepKey:     row.StepKey,
		})
	}
	return leases, nil
}

func BackingRoots(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListBackingPaths(ctx)
}
