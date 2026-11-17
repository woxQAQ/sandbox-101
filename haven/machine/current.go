package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
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

// RestoreGenerationClaim joins the stopped predecessor identity to the exact
// replacement record. The VM key and serialized body come from that same record.
type RestoreGenerationClaim struct {
	ExpectedInstance string
	OperationID      string
	VM               VMRecord
}

func ClaimRestoreGeneration(ctx context.Context, query store.DBTX, claim RestoreGenerationClaim) error {
	return store.RequireOne(dbgen.New(query).ClaimStoppedBody(ctx, dbgen.ClaimStoppedBodyParams{
		ID:       claim.VM.Resource.VMID,
		Instance: claim.ExpectedInstance,
		Owner: sql.NullString{
			String: claim.OperationID,
			Valid:  true,
		},
		Body: operation.Encode(claim.VM),
	}))
}

// CurrentOwnership separates the mutable VM row's ownership from its persisted body.
// The body is decoded by the workflow that verifies the frozen resource input;
// this row never supplies a missing historical witness.
type CurrentOwnership struct {
	Instance string
	State    string
	Owner    string
	Owned    bool // Preserves SQL NULL independently of the owner string.
	Body     []byte
}
