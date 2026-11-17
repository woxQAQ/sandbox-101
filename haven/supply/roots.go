package supply

import (
	"context"

	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

// UnresolvedTemplates verifies every catalog root against its frozen publisher.
// The caller supplies the same snapshot used for all other GC root families.
func UnresolvedTemplates(ctx context.Context, query store.DBTX) ([]string, error) {
	rows, err := dbgen.New(query).ListTemplateIDs(ctx)
	if err != nil {
		return nil, err
	}
	var unresolved []string
	catalog := NewTemplateCatalog(query)
	for _, row := range rows {
		if _, err = catalog.Owned(ctx, row.Tenant, row.ID); err != nil {
			unresolved = append(unresolved, row.ID)
		}
	}
	return unresolved, nil
}
