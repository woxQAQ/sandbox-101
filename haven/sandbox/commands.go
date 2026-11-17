package sandbox

import (
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

// SandboxCommands validates host command policy and keeps complete credentials
// bound to the original authenticated role while persistence receives redaction.
type SandboxCommands struct {
	Identity *operation.CommandIdentity
	access   *access.Registry
	maxTTL   time.Duration
}

func NewSandboxCommands(maxTTL time.Duration, identity *operation.CommandIdentity, registry *access.Registry) *SandboxCommands {
	return &SandboxCommands{
		Identity: identity,
		access:   registry,
		maxTTL:   maxTTL,
	}
}

func (service *SandboxCommands) Prepare(command operation.Command) (*ClaimAdmission, error) {
	env, secrets, descriptors, err := SplitEnv(command.Env)
	if err != nil {
		return nil, err
	}
	policy := command.Policy
	if policy.Mode == "" {
		policy.Mode = "relay"
	}
	if policy.Mode != "none" && policy.Mode != "relay" && policy.Mode != "direct" {
		return nil, fmt.Errorf("%w: invalid network mode", errs.ErrInvalid)
	}
	if len(secrets) > 0 && policy.Mode != "relay" {
		return nil, fmt.Errorf("%w: host credentials require relay mode", errs.ErrInvalid)
	}
	if policy.Mode == "direct" {
		if _, err = machine.DirectNetworkPolicy(policy); err != nil {
			return nil, err
		}
	}
	ttl := command.TTLSeconds
	if ttl == 0 {
		ttl = 300
	}
	if ttl < 1 || ttl > int64(service.maxTTL.Seconds()) {
		return nil, fmt.Errorf("%w: lease duration exceeds host bound", errs.ErrInvalid)
	}
	onExpire := command.OnExpire
	if onExpire == "" {
		onExpire = "kill"
	}
	if onExpire != "kill" && onExpire != "pause" {
		return nil, fmt.Errorf("%w: on_expire must be kill or pause", errs.ErrInvalid)
	}
	return &ClaimAdmission{
		Command: operation.SanitizedCommand(command),
		Settings: Settings{
			Env:        env,
			Policy:     policy,
			Secrets:    descriptors,
			TTLSeconds: ttl,
			OnExpire:   onExpire,
			AutoRenew:  command.AutoRenew,
		},
		Secrets: secrets,
	}, nil
}

func (service *SandboxCommands) CredentialsMissing(id string, settings Settings) bool {
	if len(settings.Secrets) == 0 {
		return false
	}
	secrets := service.access.SandboxSecrets(id)
	for name, descriptor := range settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return true
		}
	}
	return false
}
