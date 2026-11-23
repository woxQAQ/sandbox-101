package sandbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

type Settings struct {
	Env         map[string]string           `json:"env"`
	Policy      egress.Policy               `json:"policy"`
	Secrets     map[string]egress.Injection `json:"secrets,omitempty"`
	TTLSeconds  int64                       `json:"ttl_seconds"`
	OnExpire    string                      `json:"on_expire"`
	AutoRenew   bool                        `json:"auto_renew"`
	Deadline    time.Time                   `json:"deadline"`
	PausedUntil time.Time                   `json:"paused_until"`
	Generation  string                      `json:"generation"`
	Instance    string                      `json:"instance"`
	ActiveEpoch string                      `json:"active_epoch,omitempty"`
}

func (s Settings) hasCredentials(secrets map[string]egress.Secret) bool {
	for name, descriptor := range s.Secrets {
		secret := secrets[name]
		if secret.Value == "" || secret.Header != descriptor.Header || secret.Placeholder != descriptor.Placeholder || !reflect.DeepEqual(secret.Hosts, descriptor.Hosts) {
			return false
		}
	}
	return true
}

func sandboxOwner(ctx context.Context, query store.DBTX, id string) (string, error) {
	return dbgen.New(query).SandboxOwner(ctx, id)
}

func LoadSettings(ctx context.Context, query store.DBTX, id string) (Settings, error) {
	var result Settings
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		var err error
		result, err = readSettings(ctx, tx, id)
		return err
	})
	return result, err
}

func readSettings(ctx context.Context, tx *sql.Tx, id string) (Settings, error) {
	q := dbgen.New(tx)
	row, err := q.GetSettings(ctx, id)
	if err != nil {
		return Settings{}, err
	}
	s := Settings{
		Generation:  row.Generation,
		Instance:    row.Instance,
		ActiveEpoch: row.ActiveEpoch,
		TTLSeconds:  row.TtlSeconds,
		OnExpire:    row.OnExpire,
		AutoRenew:   row.AutoRenew,
		Policy: egress.Policy{
			Mode:           row.PolicyMode,
			MaxConnections: int(row.MaxConnections),
		},
	}
	if s.Deadline, err = time.Parse(time.RFC3339Nano, row.Deadline); err != nil {
		return Settings{}, err
	}
	if s.PausedUntil, err = time.Parse(time.RFC3339Nano, row.PausedUntil); err != nil {
		return Settings{}, err
	}
	env, err := q.ReadEnvironment(ctx, id)
	if err != nil {
		return Settings{}, err
	}
	if row.EnvPresent {
		s.Env = make(map[string]string, len(env))
	} else if len(env) > 0 {
		return Settings{}, fmt.Errorf("sandbox environment ownership changed")
	}
	for _, entry := range env {
		s.Env[entry.Name] = entry.Value
	}
	rules, err := q.ReadPolicyRules(ctx, id)
	if err != nil {
		return Settings{}, err
	}
	for _, rule := range rules {
		switch rule.Kind {
		case "host":
			s.Policy.AllowedHosts = append(s.Policy.AllowedHosts, rule.Value)
		case "cidr":
			s.Policy.AllowedCIDRs = append(s.Policy.AllowedCIDRs, rule.Value)
		default:
			return Settings{}, fmt.Errorf("invalid policy rule kind %q", rule.Kind)
		}
	}
	descriptors, err := q.ReadSecretDescriptors(ctx, id)
	if err != nil {
		return Settings{}, err
	}
	if len(descriptors) > 0 {
		s.Secrets = map[string]egress.Injection{}
	}
	for _, entry := range descriptors {
		d := s.Secrets[entry.Name]
		d.Header, d.Placeholder = entry.Header, entry.Placeholder
		if entry.Host.Valid {
			d.Hosts = append(d.Hosts, entry.Host.String)
		}
		s.Secrets[entry.Name] = d
	}
	return s, nil
}

func settingsRow(id string, s Settings) dbgen.InsertSandboxSettingsParams {
	return dbgen.InsertSandboxSettingsParams{
		SandboxID:      id,
		Generation:     s.Generation,
		Instance:       s.Instance,
		ActiveEpoch:    s.ActiveEpoch,
		TtlSeconds:     s.TTLSeconds,
		OnExpire:       s.OnExpire,
		AutoRenew:      s.AutoRenew,
		Deadline:       s.Deadline.Format(time.RFC3339Nano),
		PausedUntil:    s.PausedUntil.Format(time.RFC3339Nano),
		PolicyMode:     s.Policy.Mode,
		MaxConnections: int64(s.Policy.MaxConnections),
		EnvPresent:     s.Env != nil,
	}
}

func insertSettings(ctx context.Context, tx *sql.Tx, id string, s Settings) error {
	q := dbgen.New(tx)
	if err := store.RequireOne(q.InsertSandboxSettings(ctx, settingsRow(id, s))); err != nil {
		return err
	}
	return writeSettingsChildren(ctx, q, id, s)
}

func writeSettingsChildren(ctx context.Context, q *dbgen.Queries, id string, s Settings) error {
	for name, value := range s.Env {
		if err := store.RequireOne(q.InsertEnvironment(ctx, dbgen.InsertEnvironmentParams{
			SandboxID: id,
			Name:      name,
			Value:     value,
		})); err != nil {
			return err
		}
	}
	for kind, values := range map[string][]string{
		"host": s.Policy.AllowedHosts,
		"cidr": s.Policy.AllowedCIDRs,
	} {
		for index, value := range values {
			if err := store.RequireOne(q.InsertPolicyRule(ctx, dbgen.InsertPolicyRuleParams{
				SandboxID: id,
				Kind:      kind,
				Position:  int64(index),
				Value:     value,
			})); err != nil {
				return err
			}
		}
	}
	for name, d := range s.Secrets {
		if err := store.RequireOne(q.InsertSecretDescriptor(ctx, dbgen.InsertSecretDescriptorParams{
			SandboxID:   id,
			Name:        name,
			Header:      d.Header,
			Placeholder: d.Placeholder,
		})); err != nil {
			return err
		}
		for index, host := range d.Hosts {
			if err := store.RequireOne(q.InsertSecretHost(ctx, dbgen.InsertSecretHostParams{
				SandboxID: id,
				Name:      name,
				Position:  int64(index),
				Host:      host,
			})); err != nil {
				return err
			}
		}
	}
	return nil
}

func matchSettings(ctx context.Context, tx *sql.Tx, id string, expected Settings) error {
	current, err := readSettings(ctx, tx, id)
	if err != nil {
		return err
	}
	if !bytes.Equal(operation.Encode(current), operation.Encode(expected)) {
		return fmt.Errorf("sandbox %s settings changed", id)
	}
	return nil
}

func retireSettingsChildren(ctx context.Context, q *dbgen.Queries, id string) error {
	if err := q.ClearEnvironment(ctx, id); err != nil {
		return err
	}
	if err := q.ClearPolicyRules(ctx, id); err != nil {
		return err
	}
	if err := q.ClearSecretHosts(ctx, id); err != nil {
		return err
	}
	return q.ClearSecretDescriptors(ctx, id)
}

func replaceSettings(ctx context.Context, tx *sql.Tx, id string, before, after Settings) error {
	if err := matchSettings(ctx, tx, id, before); err != nil {
		return err
	}
	q := dbgen.New(tx)
	row := settingsRow(id, after)
	if !reflect.DeepEqual(before.Env, after.Env) || !reflect.DeepEqual(before.Policy, after.Policy) || !reflect.DeepEqual(before.Secrets, after.Secrets) {
		if err := retireSettingsChildren(ctx, q, id); err != nil {
			return err
		}
		if err := writeSettingsChildren(ctx, q, id, after); err != nil {
			return err
		}
	}
	if err := store.RequireOne(q.ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:      id,
		Generation:     row.Generation,
		Instance:       row.Instance,
		ActiveEpoch:    row.ActiveEpoch,
		TtlSeconds:     row.TtlSeconds,
		OnExpire:       row.OnExpire,
		AutoRenew:      row.AutoRenew,
		Deadline:       row.Deadline,
		PausedUntil:    row.PausedUntil,
		PolicyMode:     row.PolicyMode,
		MaxConnections: row.MaxConnections,
		EnvPresent:     row.EnvPresent,
	})); err != nil {
		return err
	}
	return matchSettings(ctx, tx, id, after)
}

// LifecycleFence returns the operation ID that currently blocks data and configuration
// admission for this sandbox. sql.ErrNoRows means no operation holds the fence.
func LifecycleFence(ctx context.Context, query store.DBTX, id string) (string, error) {
	return dbgen.New(query).GetFence(ctx, id)
}
