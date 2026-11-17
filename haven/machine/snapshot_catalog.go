package machine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// SnapshotRoot keeps the catalog key distinct from decoded ownership and hashes
// the original bytes. Collection eligibility still requires historical proof.
type SnapshotRoot struct {
	ID        string
	Record    SnapshotRecord
	Hash      string
	Published bool
	Deleting  bool
}

func LoadOwnedSnapshot(ctx context.Context, query store.DBTX, tenant, id string) (SnapshotRecord, error) {
	var record SnapshotRecord
	body, err := dbgen.New(query).GetOwnedBody(ctx, dbgen.GetOwnedBodyParams{
		ID:       id,
		TenantID: tenant,
	})
	if err != nil {
		return record, err
	}
	if err = json.Unmarshal(body, &record); err != nil {
		return record, err
	}
	if record.ID != id || record.TenantID != tenant {
		return record, fmt.Errorf("snapshot provenance inconsistent")
	}
	return record, nil
}

func SnapshotRoots(ctx context.Context, query store.DBTX) ([]SnapshotRoot, error) {
	rows, err := dbgen.New(query).ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	var roots []SnapshotRoot
	for _, row := range rows {
		var record SnapshotRecord
		if err := json.Unmarshal(row.Body, &record); err != nil {
			return roots, err
		}
		roots = append(roots, SnapshotRoot{
			ID:        row.ID,
			Record:    record,
			Hash:      operation.Digest(row.Body),
			Published: row.Published == 1,
			Deleting:  row.Deleting != 0,
		})
	}
	return roots, nil
}

func SnapshotReaders(ctx context.Context, query store.DBTX, id string) (int64, error) {
	return dbgen.New(query).CountSnapshotLeases(ctx, id)
}
