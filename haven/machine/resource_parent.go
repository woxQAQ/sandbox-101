package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/operation"
)

// ResourceParent joins a control workflow to a resource admission transaction.
// Implementations verify their frozen intent; Loom never receives this port.
type ResourceParent interface {
	AdmitResource(context.Context, *sql.Tx, operation.Record, string, string, string) error
}

func AdmitResourceParent(ctx context.Context, tx *sql.Tx, parent ResourceParent, child operation.Record, kind, candidate, hash string) error {
	if parent == nil {
		return nil
	}
	return parent.AdmitResource(ctx, tx, child, kind, candidate, hash)
}
