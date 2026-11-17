package machine

import (
	"context"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Repository reads current ownership and retained generation facts through the
// caller's database or transaction. Generated SQL types stay inside this package.

func LoadOwnership(ctx context.Context, query store.DBTX, id string) (Current, error) {
	row, err := dbgen.New(query).GetCurrent(ctx, id)
	if err != nil {
		return Current{}, err
	}
	return Current{
		Instance: row.CurrentInstance,
		State:    row.State,
		Owner:    row.OperationID.String,
		Owned:    row.OperationID.Valid,
		Body:     row.Body,
	}, nil
}

// Instance requires the original instance, VM and operation identities together.
func InstanceHistory(ctx context.Context, query store.DBTX, instance, vmID, operationID string) ([]byte, error) {
	return dbgen.New(query).GetInstance(ctx, dbgen.GetInstanceParams{
		ID:          instance,
		VmID:        vmID,
		OperationID: operationID,
	})
}

// Version reads the retained version named by the original operation and step.
// Missing history remains an error; the current VM row is never substituted.
func VersionHistory(ctx context.Context, query store.DBTX, operationID, vmID, stepKey string) ([]byte, error) {
	return dbgen.New(query).GetVersion(ctx, dbgen.GetVersionParams{
		ID:          operationID,
		VmID:        vmID,
		OperationID: operationID,
		StepKey:     stepKey,
	})
}
