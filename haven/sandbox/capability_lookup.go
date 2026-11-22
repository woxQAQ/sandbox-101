package sandbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type CapabilityLookup struct {
	queries *dbgen.Queries
}

func NewCapabilityLookup(db store.DBTX) *CapabilityLookup {
	return &CapabilityLookup{
		queries: dbgen.New(db),
	}
}

// FindSandboxPrincipal matches a token against the persisted tenant and sandbox IDs.
func (r *CapabilityLookup) FindSandboxPrincipal(ctx context.Context, includeRetired bool, matches func(tenantID, sandboxID string) bool) (authorization.Principal, error) {
	subjects, err := r.queries.ListCapabilitySubjects(ctx, includeRetired)
	if err != nil {
		return authorization.Principal{}, err
	}
	for _, subject := range subjects {
		if subject.ID == "" || subject.Tenant == "" {
			return authorization.Principal{}, fmt.Errorf("%w: incomplete sandbox identity", errs.ErrManualRecovery)
		}
		if matches(subject.Tenant, subject.ID) {
			return authorization.Principal{
				TenantID:  subject.Tenant,
				SandboxID: subject.ID,
			}, nil
		}
	}
	return authorization.Principal{}, sql.ErrNoRows
}
