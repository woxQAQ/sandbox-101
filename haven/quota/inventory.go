package quota

import (
	"context"

	"github.com/woxQAQ/haven/quota/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func ResourceReferences(ctx context.Context, db store.DBTX) ([]ResourceReference, error) {
	rows, err := dbgen.New(db).ListResourceReferences(ctx)
	if err != nil {
		return nil, err
	}
	var references []ResourceReference
	for _, row := range rows {
		references = append(references, ResourceReference{
			Kind:       row.ResourceKind,
			ID:         row.ResourceID,
			HolderKind: row.HolderKind,
			HolderID:   row.HolderID,
		})
	}
	return references, nil
}

// ActiveSnapshotReferences excludes backing and the publication's own checkpoint
// holder. The caller separately proves publication history and outstanding leases.
func ActiveSnapshotReferences(ctx context.Context, db store.DBTX, id string) (int64, error) {
	return dbgen.New(db).CountActiveSnapshotReferences(ctx, dbgen.CountActiveSnapshotReferencesParams{
		ResourceID: id,
		HolderID:   id,
	})
}
