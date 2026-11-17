package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// VMRoot retains the catalog identity even when the serialized resource has
// changed. Inconsistent roots still protect their observed paths from collection.
type VMRoot struct {
	ID         string
	Directory  string
	Consistent bool
}

// VMRoots returns the decoded prefix with an error when later metadata is
// unreadable. The caller can preserve that prefix in its diagnostic inventory.
func VMRoots(ctx context.Context, query store.DBTX, directory string) ([]VMRoot, error) {
	rows, err := dbgen.New(query).ListCurrentBodies(ctx)
	if err != nil {
		return nil, err
	}
	var roots []VMRoot
	for _, row := range rows {
		var record VMRecord
		if err := json.Unmarshal(row.Body, &record); err != nil {
			return roots, err
		}
		roots = append(roots, VMRoot{
			ID:         row.ID,
			Directory:  record.Resource.Dir,
			Consistent: record.Resource.VMID == row.ID && record.Resource.Dir == filepath.Join(directory, "vms", row.ID),
		})
	}
	return roots, nil
}

func CurrentVMCount(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountCurrent(ctx, id)
}

func ClaimRestoreGeneration(ctx context.Context, query store.DBTX, id, instance, owner string, body []byte) error {
	return store.RequireOne(dbgen.New(query).ClaimStoppedBody(ctx, dbgen.ClaimStoppedBodyParams{
		ID:       id,
		Instance: instance,
		Owner: sql.NullString{
			String: owner,
			Valid:  true,
		},
		Body: body,
	}))
}
