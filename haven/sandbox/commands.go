package sandbox

import (
	"reflect"
	"strings"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

// SandboxCommands validates host command policy and keeps complete credentials
// bound to the original authenticated role while persistence receives redaction.
type SandboxCommands struct {
	Identity *operation.CommandIdentity
	registry *ephemeral.Registry
	maxTTL   time.Duration
}

func NewSandboxCommands(maxTTL time.Duration, identity *operation.CommandIdentity, registry *ephemeral.Registry) *SandboxCommands {
	return &SandboxCommands{
		Identity: identity,
		registry: registry,
		maxTTL:   maxTTL,
	}
}

func (sc *SandboxCommands) Prepare(command operation.Command) (*ClaimAdmission, error) {
	env, secrets, descriptors, err := SplitEnv(command.Env)
	if err != nil {
		return nil, err
	}
	policy := command.Policy
	if policy.Mode == "" {
		policy.Mode = "proxy"
	}
	if policy.Mode != "none" && policy.Mode != "proxy" && policy.Mode != "direct" {
		return nil, errs.InvalidInput("invalid network mode")
	}
	if len(secrets) > 0 && policy.Mode != "proxy" {
		return nil, errs.InvalidInput("host credentials require proxy mode")
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
	if ttl < 1 || ttl > int64(sc.maxTTL.Seconds()) {
		return nil, errs.InvalidInput("lease duration exceeds host bound")
	}
	onExpire := command.OnExpire
	if onExpire == "" {
		onExpire = "kill"
	}
	if onExpire != "kill" && onExpire != "pause" {
		return nil, errs.InvalidInput("on_expire must be kill or pause")
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

func (sc *SandboxCommands) CredentialsMissing(id string, settings Settings) bool {
	if len(settings.Secrets) == 0 {
		return false
	}
	secrets := sc.registry.SandboxSecrets(id)
	for name, descriptor := range settings.Secrets {
		secret, ok := secrets[name]
		if !ok || secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return true
		}
	}
	return false
}

func SplitEnv(env map[string]operation.EnvVar) (map[string]string, map[string]egress.Secret, map[string]egress.Injection, error) {
	visible := map[string]string{}
	secrets := map[string]egress.Secret{}
	descriptors := map[string]egress.Injection{}
	for key, value := range env {
		if key == "" || len(key) > 256 || strings.ContainsAny(key, "=\x00\n") || strings.ContainsRune(value.Value, 0) {
			return nil, nil, nil, errs.InvalidInput("invalid environment variable")
		}
		if value.IsSecret() {
			if value.Inject == nil || len(value.Inject.Hosts) == 0 || value.Inject.Header == "" || value.Value == "" {
				return nil, nil, nil, errs.InvalidInput("host credential requires authorized injection")
			}
			descriptors[key] = *value.Inject
			secrets[key] = egress.Secret{
				Value:       value.Value,
				Hosts:       append([]string(nil), value.Inject.Hosts...),
				Header:      value.Inject.Header,
				Placeholder: value.Inject.Placeholder,
			}
			if value.Inject.Placeholder != "" {
				visible[key] = value.Inject.Placeholder
			}
		} else {
			visible[key] = value.Value
		}
	}
	return visible, secrets, descriptors, nil
}
