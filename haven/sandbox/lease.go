package sandbox

import (
	"context"
	"encoding/json"

	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// LeaseCandidate is a running or paused sandbox with its persisted lease policy.
// Expiration must still pass the original deadline and configuration fence.
type LeaseCandidate struct {
	ID       string
	TenantID string
	State    string
	Settings Settings
}

func LeaseCandidates(ctx context.Context, query store.DBTX) ([]LeaseCandidate, error) {
	rows, err := dbgen.New(query).ListLeaseCandidates(ctx)
	if err != nil {
		return nil, err
	}
	var candidates []LeaseCandidate
	for _, row := range rows {
		candidate := LeaseCandidate{
			ID:       row.ID,
			TenantID: row.Tenant,
			State:    row.State,
		}
		if err = json.Unmarshal(row.Body, &candidate.Settings); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}
