package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/woxQAQ/haven/ownership"
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

// FindCapability includes retired identities only for historical operation
// authorization. Decoding stops at the matching capability, preserving the
// original ordered lookup's failure boundary.
func (r *CapabilityLookup) FindCapability(ctx context.Context, retired bool, matches func(ownership.Sandbox) bool) (ownership.Sandbox, error) {
	var bodies [][]byte
	var err error
	if retired {
		bodies, err = r.queries.ListAllCapabilities(ctx)
	} else {
		bodies, err = r.queries.ListActiveCapabilities(ctx)
	}
	if err != nil {
		return ownership.Sandbox{}, err
	}
	for _, body := range bodies {
		var record ownership.Sandbox
		if err := json.Unmarshal(body, &record); err != nil {
			return ownership.Sandbox{}, err
		}
		if matches(record) {
			return record, nil
		}
	}
	return ownership.Sandbox{}, sql.ErrNoRows
}
