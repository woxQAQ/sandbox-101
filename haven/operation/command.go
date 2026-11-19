package operation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type EnvVar struct {
	Value  string            `json:"value"`
	Guest  *bool             `json:"guest,omitempty"`
	Inject *egress.Injection `json:"inject,omitempty"`
}

func (value EnvVar) IsSecret() bool {
	return value.Guest != nil && !*value.Guest
}

// Command retains the sandbox API's intent fields. Only sanitized values may
// enter a durable request; a node-keyed MAC compares complete secret-bearing
// retries without retaining their values or a guessable credential digest.
type Command struct {
	ObservedGeneration string            `json:"observed_generation,omitempty"`
	Template           *TemplateSpec     `json:"template,omitempty"`
	Action             string            `json:"action"`
	SandboxID          string            `json:"sandbox_id,omitempty"`
	CheckpointID       string            `json:"checkpoint_id,omitempty"`
	TemplateID         string            `json:"template_id,omitempty"`
	TTLSeconds         int64             `json:"ttl_seconds,omitempty"`
	OnExpire           string            `json:"on_expire,omitempty"`
	AutoRenew          bool              `json:"auto_renew,omitempty"`
	LeaseDeadline      time.Time         `json:"lease_deadline"`
	Env                map[string]EnvVar `json:"env,omitempty"`
	Policy             egress.Policy     `json:"policy"`
	Count              int               `json:"count,omitempty"`
}

func SanitizedCommand(command Command) Command {
	result := command
	result.Env = map[string]EnvVar{}
	for key, value := range command.Env {
		if value.IsSecret() {
			value.Value = ""
		}
		if value.Inject != nil {
			copy := *value.Inject
			copy.Hosts = append([]string(nil), copy.Hosts...)
			value.Inject = &copy
		}
		result.Env[key] = value
	}
	return result
}

// Template is the accepted template specification used in frozen commands.
// The host shape and JSON field order are retained independently of wire DTOs.
type TemplateSpec struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Image      string     `json:"image"`
	Shape      loom.Shape `json:"shape"`
	SnapshotID string     `json:"snapshot_id,omitempty"`
	State      string     `json:"state"`
	TargetWarm int        `json:"target_warm"`
	Warmup     string     `json:"warmup,omitempty"`
}

func (r *Repository) Finish(ctx context.Context, id, status string, body []byte) error {
	if err := store.RequireOne(r.queries.FinishLiveOperation(ctx, dbgen.FinishLiveOperationParams{
		ID:     id,
		Status: status,
		Body:   body,
	})); err != nil {
		return err
	}
	if Terminal(status) {
		return r.finishClock(ctx, id)
	}
	return nil
}

// LiveTenantRequest verifies the persisted bytes, scope and lifecycle before
// returning the input for action-specific decoding and the current result.
func (r *Repository) LiveTenantRequest(ctx context.Context, id, tenant string) ([]byte, Result, error) {
	var result Result
	row, err := r.queries.GetRequestState(ctx, id)
	if err != nil {
		return nil, result, err
	}
	if Digest(row.Request) != row.RequestHash {
		return nil, result, fmt.Errorf("operation request checksum mismatch")
	}
	if row.ScopeKind != "tenant" || row.ScopeID != tenant || Terminal(row.Status) {
		return nil, result, fmt.Errorf("operation tenant or lifecycle changed")
	}
	if err := json.Unmarshal(row.Body, &result); err != nil {
		return nil, result, err
	}
	return row.Request, result, nil
}

func (r *Repository) RunResult(ctx context.Context, id string, body []byte) error {
	return store.RequireOne(r.queries.RunLiveOperationResult(ctx, dbgen.RunLiveOperationResultParams{
		Body: body,
		ID:   id,
	}))
}
