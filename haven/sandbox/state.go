// Package sandbox owns stable sandbox identity, settings and catalog facts.
package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type Settings struct {
	Env         map[string]string                  `json:"env"`
	Policy      egress.Policy                      `json:"policy"`
	Secrets     map[string]authorization.Injection `json:"secrets,omitempty"`
	TTLSeconds  int64                              `json:"ttl_seconds"`
	OnExpire    string                             `json:"on_expire"`
	AutoRenew   bool                               `json:"auto_renew"`
	Deadline    time.Time                          `json:"deadline"`
	PausedUntil time.Time                          `json:"paused_until"`
	Generation  string                             `json:"generation"`
	Instance    string                             `json:"instance"`
	ActiveEpoch string                             `json:"active_epoch,omitempty"`
}

type CapabilityLookup struct {
	queries *dbgen.Queries
}

func NewCapabilityLookup(db store.DBTX) *CapabilityLookup {
	return &CapabilityLookup{
		queries: dbgen.New(db),
	}
}

func LoadSandbox(ctx context.Context, query store.DBTX, id, tenant string) (ownership.Sandbox, error) {
	row, err := dbgen.New(query).GetSandbox(ctx, dbgen.GetSandboxParams{
		ID:     id,
		Tenant: tenant,
	})
	if err != nil {
		return ownership.Sandbox{}, err
	}
	var record ownership.Sandbox
	if err = json.Unmarshal(row.Body, &record); err != nil {
		return record, err
	}
	if record.ID != id || record.TenantID != tenant || record.State != row.State {
		return record, fmt.Errorf("sandbox ownership metadata inconsistent")
	}
	return record, nil
}

func sandboxOwner(ctx context.Context, query store.DBTX, id string) (string, error) {
	return dbgen.New(query).SandboxOwner(ctx, id)
}

func LoadSettings(ctx context.Context, query store.DBTX, id string) (Settings, error) {
	body, err := dbgen.New(query).GetSettings(ctx, id)
	if err != nil {
		return Settings{}, err
	}
	var settings Settings
	err = json.Unmarshal(body, &settings)
	return settings, err
}

func LifecycleFence(ctx context.Context, query store.DBTX, id string) (string, error) {
	return dbgen.New(query).GetFence(ctx, id)
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
