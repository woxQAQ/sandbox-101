package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/operation"
)

// ResourceAdmission carries one child's frozen identity through the parent
// transaction. Its fields are required facts, not optional execution modes.
type ResourceAdmission struct {
	Child       operation.Record
	Kind        string
	Candidate   string
	RequestHash string
}

// ResourceParent joins a control workflow to a resource admission transaction.
// Implementations verify their frozen intent; Loom never receives this port.
type ResourceParent interface {
	AdmitResource(context.Context, *sql.Tx, ResourceAdmission) error
}

func AdmitResourceParent(ctx context.Context, tx *sql.Tx, parent ResourceParent, request ResourceAdmission) error {
	if parent == nil {
		return nil
	}
	return parent.AdmitResource(ctx, tx, request)
}
