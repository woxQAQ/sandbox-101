package operation

import (
	"context"
	"fmt"
	"time"

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

// CommandRequest records the sanitized business intent accepted for replay.
func CommandRequest(command Intent) Request {
	return Request{
		Action:  command.Action,
		Command: &command,
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

// LiveTenantRequestAndResult returns the admitted parameters and result in one snapshot.
func (r *Repository) LiveTenantRequestAndResult(ctx context.Context, id, tenant string) (Request, Result, error) {
	var result Result
	row, err := r.LoadProjectionSource(ctx, id)
	if err != nil {
		return Request{}, result, err
	}
	if row.ScopeKind != ScopeTenant || row.ScopeID != tenant || Terminal(row.Status) {
		return Request{}, result, fmt.Errorf("operation tenant or lifecycle changed")
	}
	return row.Request, row.Result, nil
}

func (r *Repository) MarkRunningWithResult(ctx context.Context, id string, result Result) error {
	return r.updateResult(ctx, id, "running", result)
}
