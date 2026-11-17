package quota

import (
	"context"

	"github.com/woxQAQ/haven/quota/internal/dbgen"
)

func (r *Repository) ResourceReferences(ctx context.Context) ([]ResourceReference, error) {
	rows, err := r.queries.ListResourceReferences(ctx)
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
func (r *Repository) ActiveSnapshotReferences(ctx context.Context, id string) (int64, error) {
	return r.queries.CountActiveSnapshotReferences(ctx, dbgen.CountActiveSnapshotReferencesParams{
		ResourceID: id,
		HolderID:   id,
	})
}
