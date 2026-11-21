package operation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Intent includes the generation observed by host-initiated configuration changes.
// Its JSON field order is part of the input to the durable request MAC.
type Intent struct {
	ObservedGeneration string                  `json:"observed_generation,omitempty"`
	Template           *types.Template         `json:"template,omitempty"`
	Action             string                  `json:"action"`
	SandboxID          string                  `json:"sandbox_id,omitempty"`
	CheckpointID       string                  `json:"checkpoint_id,omitempty"`
	TemplateID         string                  `json:"template_id,omitempty"`
	TTLSeconds         int64                   `json:"ttl_seconds,omitempty"`
	OnExpire           string                  `json:"on_expire,omitempty"`
	AutoRenew          bool                    `json:"auto_renew,omitempty"`
	LeaseDeadline      time.Time               `json:"lease_deadline"`
	Env                map[string]types.EnvVar `json:"env,omitempty"`
	Policy             types.Policy            `json:"policy"`
	Count              int                     `json:"count,omitempty"`
}

// NewIntent copies the public fields into the stable representation used for replay.
func NewIntent(command types.Command) Intent {
	return Intent{
		Template:      command.Template,
		Action:        command.Action,
		SandboxID:     command.SandboxID,
		CheckpointID:  command.CheckpointID,
		TemplateID:    command.TemplateID,
		TTLSeconds:    command.TTLSeconds,
		OnExpire:      command.OnExpire,
		AutoRenew:     command.AutoRenew,
		LeaseDeadline: command.LeaseDeadline,
		Env:           command.Env,
		Policy:        command.Policy,
		Count:         command.Count,
	}
}

func SanitizedCommand(command Intent) Intent {
	result := command
	result.Env = map[string]types.EnvVar{}
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
