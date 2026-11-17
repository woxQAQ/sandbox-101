package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// RunningSubject identifies a running sandbox with persisted settings. It does
// not establish runtime readiness or a current proxy binding.
type RunningSubject struct {
	ID       string
	TenantID string
}

func BoundSubjects(ctx context.Context, query store.DBTX) ([]RunningSubject, error) {
	rows, err := dbgen.New(query).ListRunningSubjects(ctx)
	if err != nil {
		return nil, err
	}
	var subjects []RunningSubject
	for _, row := range rows {
		subjects = append(subjects, RunningSubject{
			ID:       row.ID,
			TenantID: row.Tenant,
		})
	}
	return subjects, nil
}
