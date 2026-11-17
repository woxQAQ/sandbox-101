package operation

import (
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
)

type EnvVar struct {
	Value  string                   `json:"value"`
	Guest  *bool                    `json:"guest,omitempty"`
	Inject *authorization.Injection `json:"inject,omitempty"`
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
