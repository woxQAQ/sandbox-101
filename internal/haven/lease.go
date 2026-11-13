package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) AdmitRenew(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if command.Action != "renew" || command.SandboxID == "" || principal.TenantID == "" || key == "" || len(key) > 256 || (principal.SandboxID != "" && principal.SandboxID != command.SandboxID) {
		return empty, loom.Fail(loom.Invalid, "renew requires scoped sandbox and key")
	}
	requestGuard := service.vmLock("request:" + string(encode([]string{principal.TenantID, key})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	mac := service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{MAC: mac}); found || err != nil {
		return operation, err
	}
	guard := service.vmLock("configuration:" + command.SandboxID)
	guard.Lock()
	defer guard.Unlock()
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	sandbox, err := ownedSandbox(ctx, tx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}
	settings, err := loadSettings(ctx, tx, sandbox.ID)
	if err != nil {
		return empty, err
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&fence); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return empty, err
		}
		return empty, loom.Fail(loom.Conflict, "renew cannot pass an admitted configuration fence")
	}
	now := time.Now().UTC()
	next := settings
	switch sandbox.State {
	case "running":
		if settings.Deadline.IsZero() || !now.Before(settings.Deadline) {
			return empty, loom.Fail(loom.Conflict, "sandbox lease expired")
		}
		ttl := command.TTLSeconds
		if ttl == 0 {
			ttl = settings.TTLSeconds
		}
		if ttl < 1 || ttl > int64(service.opts.MaxTTL.Seconds()) {
			return empty, loom.Fail(loom.Invalid, "renew duration exceeds host bound")
		}
		next.TTLSeconds = ttl
		next.Deadline = now.Add(time.Duration(ttl) * time.Second)
	case "paused":
		if settings.PausedUntil.IsZero() || !now.Before(settings.PausedUntil) {
			return empty, loom.Fail(loom.Conflict, "paused sandbox lease expired")
		}
		ttl := command.TTLSeconds
		if ttl == 0 {
			ttl = int64(service.opts.PausedTTL.Seconds())
		}
		if ttl < 1 || ttl > int64(service.opts.PausedTTL.Seconds()) {
			return empty, loom.Fail(loom.Invalid, "paused renew duration exceeds host bound")
		}
		next.PausedUntil = now.Add(time.Duration(ttl) * time.Second)
	default:
		return empty, loom.Fail(loom.Conflict, "sandbox state cannot renew")
	}
	operation := Operation{ID: newID("op-"), Key: key, Status: "succeeded", Result: operationResult{SandboxID: sandbox.ID, VMID: sandbox.VMID}}
	frozen := encode(struct {
		Action  string
		Request Command
	}{"renew", sanitizedCommand(command)})
	input := encode(struct {
		Sandbox       sandboxRecord
		Before, After sandboxSettings
	}{sandbox, settings, next})
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{operation.ID, "tenant", principal.TenantID, key, digest(frozen), frozen, "succeeded", encode(operation.Result)}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{operation.ID, mac}},
		{"INSERT INTO operation_steps VALUES(?,'lease','renew',?,?,?,?,'succeeded',?)", []any{operation.ID, digest(input), input, sandbox.ID, sandbox.Instance, encode("complete")}},
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{encode(next), sandbox.ID, encode(settings)}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("renew_before_commit"); err != nil {
		return empty, err
	}
	if err = commitPublications(tx); err != nil {
		return empty, err
	}
	return operation, service.store.checkpoint()
}

// Reap submits normal durable workflows with the exact observed lease deadline.
// Renewal winning the configuration guard invalidates that expiration intent.
func (service *Service) Reap(ctx context.Context) error {
	rows, err := service.store.db.QueryContext(ctx, "SELECT s.id,s.tenant,s.state,c.body FROM sandboxes s JOIN sandbox_settings c ON c.sandbox_id=s.id WHERE s.state IN ('running','paused') ORDER BY s.id")
	if err != nil {
		return err
	}
	type expiration struct {
		id, tenant, action string
		deadline           time.Time
	}
	var pending []expiration
	now := time.Now().UTC()
	for rows.Next() {
		var id, tenant, state string
		var body []byte
		if err = rows.Scan(&id, &tenant, &state, &body); err != nil {
			break
		}
		var settings sandboxSettings
		if err = json.Unmarshal(body, &settings); err != nil {
			break
		}
		deadline := settings.Deadline
		action := "release"
		if state == "paused" {
			deadline = settings.PausedUntil
		} else if settings.OnExpire == "pause" {
			action = "pause"
		}
		if !deadline.IsZero() && !now.Before(deadline) {
			pending = append(pending, expiration{id, tenant, action, deadline})
		}
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	var failures []error
	for _, item := range pending {
		_, err := service.AdmitLifecycle(ctx, Principal{TenantID: item.tenant}, "expire:"+item.id+":"+item.deadline.Format(time.RFC3339Nano), Command{Action: item.action, SandboxID: item.id, LeaseDeadline: item.deadline})
		if err != nil {
			failures = append(failures, fmt.Errorf("expire %s: %w", item.id, err))
		}
	}
	return errors.Join(failures...)
}
