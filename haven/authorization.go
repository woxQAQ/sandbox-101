package haven

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func (state *store) sign(parts ...string) string {
	mac := hmac.New(sha256.New, state.key)
	for _, part := range parts {
		_, _ = fmt.Fprintf(mac, "%d:", len(part))
		_, _ = mac.Write([]byte(part))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

// commandMAC binds complete secret-bearing input to the authenticated role and
// sandbox scope. A tenant named root cannot replay an administrator's intent.
func (state *store) commandMAC(principal Principal, command Command) string {
	role := "tenant"
	if principal.Root {
		role = "root"
	} else if principal.SandboxID != "" {
		role = "sandbox"
	}
	return role + ":" + state.sign("sandbox-command-v1", string(encode(principal)), string(encode(command)))
}

func tokenHash(token string) string {
	return digest([]byte(token))
}

func (service *Service) Authenticate(ctx context.Context, token string) (Principal, error) {
	return service.authenticateToken(ctx, token, false)
}

// AuthenticateOperation permits a retired sandbox capability only for reading
// its own operation history. It grants no resource or data admission.
func (service *Service) AuthenticateOperation(ctx context.Context, token string) (Principal, error) {
	return service.authenticateToken(ctx, token, true)
}

func (service *Service) authenticateToken(ctx context.Context, token string, retired bool) (Principal, error) {
	if token == "" {
		return Principal{}, fmt.Errorf("%w: not found", ErrNotFound)
	}
	if service.opts.RootToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(service.opts.RootToken)) == 1 {
		return Principal{
			TenantID: "root",
			Root:     true,
		}, nil
	}
	var tenant string
	err := service.store.db.QueryRowContext(ctx, "SELECT id FROM tenants WHERE token_hash=?", tokenHash(token)).Scan(&tenant)
	if err == nil {
		return Principal{
			TenantID: tenant,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Principal{}, err
	}
	query := "SELECT body FROM sandboxes WHERE state<>'terminated'"
	if retired {
		query = "SELECT body FROM sandboxes"
	}
	rows, err := service.store.db.QueryContext(ctx, query)
	if err != nil {
		return Principal{}, err
	}
	defer func() {
		_ = rows.Close()
	}()
	for rows.Next() {
		var body []byte
		var sandbox sandboxRecord
		if err = rows.Scan(&body); err != nil {
			return Principal{}, err
		}
		if err = json.Unmarshal(body, &sandbox); err != nil {
			return Principal{}, err
		}
		if subtle.ConstantTimeCompare([]byte(token), []byte(service.sandboxToken(sandbox))) == 1 {
			return Principal{
				TenantID:  sandbox.TenantID,
				SandboxID: sandbox.ID,
			}, nil
		}
	}
	if err = rows.Err(); err != nil {
		return Principal{}, err
	}
	return Principal{}, fmt.Errorf("%w: not found", ErrNotFound)
}

func (service *Service) sandboxToken(sandbox sandboxRecord) string {
	return service.store.sign("sandbox-token-v1", sandbox.TenantID, sandbox.ID)
}

func visibleSandbox(principal Principal, sandbox sandboxRecord) bool {
	return (principal.Root || principal.TenantID == sandbox.TenantID) && (principal.SandboxID == "" || principal.SandboxID == sandbox.ID)
}

func splitEnv(env map[string]EnvVar) (map[string]string, map[string]egress.Secret, map[string]Injection, error) {
	visible := map[string]string{}
	secrets := map[string]egress.Secret{}
	descriptors := map[string]Injection{}
	for key, value := range env {
		if key == "" || len(key) > 256 || strings.ContainsAny(key, "=\x00\n") || strings.ContainsRune(value.Value, 0) {
			return nil, nil, nil, fmt.Errorf("%w: invalid environment variable", ErrInvalid)
		}
		if value.isSecret() {
			if value.Inject == nil || len(value.Inject.Hosts) == 0 || value.Inject.Header == "" || value.Value == "" {
				return nil, nil, nil, fmt.Errorf("%w: host credential requires authorized injection", ErrInvalid)
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

func sanitizedCommand(command Command) Command {
	result := command
	result.Env = map[string]EnvVar{}
	for key, value := range command.Env {
		if value.isSecret() {
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

func directNetworkPolicy(policy egress.Policy) (*loom.NetworkPolicy, error) {
	if policy.Mode != "direct" || len(policy.AllowedHosts) != 0 {
		return nil, fmt.Errorf("%w: direct mode requires IPv4 CIDRs without hostname rules", ErrInvalid)
	}
	limit := policy.MaxConnections
	if limit == 0 {
		limit = 256
	}
	if limit < 1 || limit > 65535 || len(policy.AllowedCIDRs) > 256 {
		return nil, fmt.Errorf("%w: invalid direct connection budget", ErrInvalid)
	}
	seen := map[netip.Prefix]bool{}
	for _, raw := range policy.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() || prefix.String() != raw || seen[prefix] {
			return nil, fmt.Errorf("%w: direct mode requires unique canonical IPv4 CIDRs", ErrInvalid)
		}
		seen[prefix] = true
	}
	result := &loom.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   append([]string(nil), policy.AllowedCIDRs...),
		MaxConnections: limit,
	}
	return result, host.ValidateNetworkPolicy(result)
}

func (service *Service) prepareClaim(command Command) (*claimAdmission, error) {
	env, secrets, descriptors, err := splitEnv(command.Env)
	if err != nil {
		return nil, err
	}
	policy := command.Policy
	if policy.Mode == "" {
		policy.Mode = "relay"
	}
	if policy.Mode != "none" && policy.Mode != "relay" && policy.Mode != "direct" {
		return nil, fmt.Errorf("%w: invalid network mode", ErrInvalid)
	}
	if len(secrets) > 0 && policy.Mode != "relay" {
		return nil, fmt.Errorf("%w: host credentials require relay mode", ErrInvalid)
	}
	if policy.Mode == "direct" {
		if _, err = directNetworkPolicy(policy); err != nil {
			return nil, err
		}
	}
	ttl := command.TTLSeconds
	if ttl == 0 {
		ttl = 300
	}
	if ttl < 1 || ttl > int64(service.opts.MaxTTL.Seconds()) {
		return nil, fmt.Errorf("%w: lease duration exceeds host bound", ErrInvalid)
	}
	onExpire := command.OnExpire
	if onExpire == "" {
		onExpire = "kill"
	}
	if onExpire != "kill" && onExpire != "pause" {
		return nil, fmt.Errorf("%w: on_expire must be kill or pause", ErrInvalid)
	}
	return &claimAdmission{
		Command: sanitizedCommand(command),
		Settings: sandboxSettings{
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

func (service *Service) credentialsMatch(operation string, admission *claimAdmission) error {
	var expected string
	if err := service.store.db.QueryRowContext(service.ctx, "SELECT request_mac FROM operation_credentials WHERE operation_id=?", operation).Scan(&expected); err != nil {
		return err
	}
	if !hmac.Equal([]byte(expected), []byte(admission.MAC)) {
		return fmt.Errorf("%w: idempotency key has different parameters", ErrConflict)
	}
	return nil
}
