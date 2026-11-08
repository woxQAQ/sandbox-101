package sandboxd

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

// activationFence requires configure to own its fence and every other
// activation to observe no fence. Callers guard publication with configuration:ID.
func (s *Service) activationFence(o Operation, id string) error {
	fence, err := configFence(s.ctx, s.store.db, id)
	if err != nil {
		return err
	}
	if (o.Command.Action == "configure" && fence != o.ID) ||
		(o.Command.Action != "configure" && fence != "") {
		return api.Fail(
			api.Conflict,
			"sandbox configuration operation is fenced or superseded",
		)
	}
	return nil
}

func (s *Service) configure(o *Operation, b *Sandbox) (err error) {
	defer func() {
		s.mu.Lock()
		delete(s.opSecrets, o.ID)
		s.mu.Unlock()
	}()
	configuration := s.lock("configuration:" + b.ID)
	if err = s.activationFence(*o, b.ID); err != nil {
		configuration()
		return err
	}
	// Once an owned configure starts, every error closes its data paths.
	// Read the durable configuration for diagnostics so a failed save never
	// writes the rejected policy back through LastError handling.
	defer func() {
		if err != nil {
			s.closeConnections(b.ID)
			err = errors.Join(err, s.revoke(b.ID))
			if saved, e := getSandbox(s.ctx, s.store.db, b.ID); e == nil {
				saved.LastError = err.Error()
				_ = saveSandbox(s.ctx, s.store.db, saved)
			}
		}
	}()
	if b.State != "running" && b.State != "paused" {
		configuration()
		return api.Fail(
			api.Conflict,
			"configuration requires running or paused sandbox",
		)
	}
	s.closeConnections(b.ID)
	if err = s.revoke(b.ID); err != nil {
		configuration()
		return err
	}
	s.mu.Lock()
	s.secrets[b.ID] = s.opSecrets[o.ID]
	s.mu.Unlock()
	b.Env = map[string]string{}
	b.RequiresSecrets = false
	for k, v := range o.Command.Env {
		if v.isSecret() {
			b.RequiresSecrets = true
			if v.Inject != nil && v.Inject.Placeholder != "" {
				b.Env[k] = v.Inject.Placeholder
			}
		} else {
			b.Env[k] = v.Value
		}
	}
	if o.Command.Policy.Mode != "" {
		b.Policy = o.Command.Policy
	}
	if b.State == "running" {
		err = saveSandbox(s.ctx, s.store.db, *b)
	}
	configuration()
	if err != nil {
		return err
	}
	if b.State == "running" {
		if err = s.activate(*o, b, false); err != nil {
			return err
		}
	}
	configuration = s.lock("configuration:" + b.ID)
	defer configuration()
	if err = s.activationFence(*o, b.ID); err != nil {
		return err
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = saveSandbox(s.ctx, tx, *b); err != nil {
		return err
	}
	if err = clearConfigFence(s.ctx, tx, b.ID, o.ID); err != nil {
		return err
	}
	completed := *o
	completed.Status, completed.Error = "succeeded", nil
	completed.FinishedAt = time.Now().UTC()
	completed.Result.Sandbox = b
	if err = saveOp(s.ctx, tx, completed); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*o = completed
	return nil
}

func splitEnv(
	env map[string]EnvVar,
) (map[string]string, map[string]egress.Secret, error) {
	visibleEnv := map[string]string{}
	secrets := map[string]egress.Secret{}
	for k, v := range env {
		if k == "" || strings.ContainsAny(k, "=\x00\n") || len(k) > 256 ||
			strings.ContainsRune(v.Value, 0) {
			return nil, nil, api.Fail(
				api.Invalid,
				"invalid environment variable",
			)
		}
		if v.isSecret() {
			if v.Inject == nil || len(v.Inject.Hosts) == 0 ||
				v.Inject.Header == "" ||
				v.Value == "" {
				return nil, nil, api.Fail(
					api.Invalid,
					"host secret requires value, authorized hosts and header",
				)
			}
			secrets[k] = egress.Secret{
				Value:       v.Value,
				Hosts:       v.Inject.Hosts,
				Header:      v.Inject.Header,
				Placeholder: v.Inject.Placeholder,
			}
			if v.Inject.Placeholder != "" {
				visibleEnv[k] = v.Inject.Placeholder
			}
		} else {
			visibleEnv[k] = v.Value
		}
	}
	return visibleEnv, secrets, nil
}

func sanitized(c Command) Command {
	c.Env = cloneEnv(c.Env)
	for k, v := range c.Env {
		if v.isSecret() {
			v.Value = ""
			c.Env[k] = v
		}
	}
	return c
}

func cloneEnv(e map[string]EnvVar) map[string]EnvVar {
	out := map[string]EnvVar{}
	maps.Copy(out, e)
	return out
}

// recordConfigFence atomically admits a configure intent. A newer explicit
// configure replaces the fence; an older operation can never remove it.
// Only identities are stored here, never previous or new parameters.
func recordConfigFence(
	ctx context.Context,
	e executor,
	sandboxID, operationID string,
) error {
	_, err := e.ExecContext(
		ctx,
		"INSERT INTO configuration_fences(sandbox_id,operation_id) VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id",
		sandboxID,
		operationID,
	)
	return err
}

// configFence returns the operation owning the configuration boundary. A
// failed query is never reported as the absence of a fence.
func configFence(
	ctx context.Context,
	e executor,
	sandboxID string,
) (string, error) {
	var operation string
	err := e.QueryRowContext(
		ctx,
		"SELECT operation_id FROM configuration_fences WHERE sandbox_id=?",
		sandboxID,
	).Scan(&operation)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return operation, err
}

// clearConfigFence removes only the fence owned by this operation.
func clearConfigFence(
	ctx context.Context,
	e executor,
	sandboxID, operationID string,
) error {
	res, err := e.ExecContext(
		ctx,
		"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?",
		sandboxID,
		operationID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return api.Fail(api.Conflict, "configuration operation was superseded")
	}
	return err
}

// Injection restricts a host secret to authorized hosts and an HTTP header.
// Placeholder may be exposed to the guest in place of the secret value.
type Injection struct {
	Hosts       []string `json:"hosts"`
	Header      string   `json:"header"`
	Placeholder string   `json:"placeholder,omitempty"`
}

// EnvVar supplies a guest environment value, or a host secret when Guest is false.
// Host secret values require Injection and are cleared in durable commands.
type EnvVar struct {
	Value  string     `json:"value"`
	Guest  *bool      `json:"guest,omitempty"`
	Inject *Injection `json:"inject,omitempty"`
}

func (v EnvVar) isSecret() bool { return v.Guest != nil && !*v.Guest }
