package sandbox

import (
	"context"
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
