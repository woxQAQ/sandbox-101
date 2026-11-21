package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// LeaseCandidate is a running or paused sandbox with its persisted lease policy.
// Expiration must still pass the original deadline and configuration fence.
type LeaseCandidate struct {
	ID       string
	TenantID string
	State    string
	Settings Settings
}

func LeaseCandidates(ctx context.Context, query store.DBTX) ([]LeaseCandidate, error) {
	rows, err := dbgen.New(query).ListLeaseCandidates(ctx)
	if err != nil {
		return nil, err
	}
	var candidates []LeaseCandidate
	for _, row := range rows {
		candidate := LeaseCandidate{
			ID:       row.ID,
			TenantID: row.Tenant,
			State:    row.State,
		}
		if err = json.Unmarshal(row.Body, &candidate.Settings); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

// RenewActivityLease extends the verified configuration under the caller's
// continuous configuration guard. Admission, binding checks and checkpointing
// remain with the data caller; the original settings body is the CAS witness.
func RenewActivityLease(ctx context.Context, query store.DBTX, id string, previous Settings) (time.Time, error) {
	next := previous
	next.Deadline = time.Now().UTC().Add(time.Duration(next.TTLSeconds) * time.Second)
	err := store.RequireOne(dbgen.New(query).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    id,
		PreviousBody: operation.Encode(previous),
		NextBody:     operation.Encode(next),
	}))
	return next.Deadline, err
}

func (l *Lifecycle) AdmitRenew(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	var empty operation.State
	if command.Action != "renew" || command.SandboxID == "" || principal.TenantID == "" || key == "" || len(key) > 256 || (principal.SandboxID != "" && principal.SandboxID != command.SandboxID) {
		return empty, fmt.Errorf("%w: renew requires scoped sandbox and key", errs.ErrInvalid)
	}
	releaseKey := l.tenantIdempotencyLocks.Lock(principal.TenantID, key)
	defer releaseKey()
	mac := l.identity.CommandMAC(principal, command)
	previous, found, err := l.identity.Existing(ctx, principal, key, operation.CommandCredentials{
		MAC: mac,
	})
	if err != nil {
		return previous, err
	}
	if found {
		if !operation.Terminal(previous.Status) {
			return previous, fmt.Errorf("%w: renewal operation %s has non-terminal status %q", errs.ErrManualRecovery, previous.ID, previous.Status)
		}
		return previous, nil
	}
	unlock := l.sandboxConfigLocks.Lock(command.SandboxID)
	defer unlock()
	l.storeWriteMu.Lock()
	defer l.storeWriteMu.Unlock()
	if err := l.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := l.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := NewCatalog(tx).OwnedRecord(ctx, principal, command.SandboxID)
	if err != nil {
		return empty, err
	}

	settings, err := LoadSettings(ctx, tx, current.ID)
	if err != nil {
		return empty, err
	}
	if _, err = LifecycleFence(ctx, tx, current.ID); !errors.Is(err, sql.ErrNoRows) {
		if err != nil {
			return empty, err
		}
		return empty, fmt.Errorf("%w: renew cannot pass an admitted configuration fence", errs.ErrConflict)
	}
	now := time.Now().UTC()
	next := settings
	switch current.State {
	case "running":
		if settings.Deadline.IsZero() || !now.Before(settings.Deadline) {
			return empty, fmt.Errorf("%w: sandbox lease expired", errs.ErrConflict)
		}
		ttl := command.TTLSeconds
		if ttl == 0 {
			ttl = settings.TTLSeconds
		}
		if ttl < 1 || ttl > int64(l.opts.MaxTTL.Seconds()) {
			return empty, errs.InvalidInput("renew duration exceeds host bound")
		}
		next.TTLSeconds = ttl
		next.Deadline = now.Add(time.Duration(ttl) * time.Second)
	case "paused":
		if settings.PausedUntil.IsZero() || !now.Before(settings.PausedUntil) {
			return empty, fmt.Errorf("%w: paused sandbox lease expired", errs.ErrConflict)
		}
		ttl := command.TTLSeconds
		if ttl == 0 {
			ttl = int64(l.opts.PausedTTL.Seconds())
		}
		if ttl < 1 || ttl > int64(l.opts.PausedTTL.Seconds()) {
			return empty, errs.InvalidInput("paused renew duration exceeds host bound")
		}
		next.PausedUntil = now.Add(time.Duration(ttl) * time.Second)
	default:
		return empty, fmt.Errorf("%w: sandbox state cannot renew", errs.ErrConflict)
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "succeeded",
		Result: operation.Result{
			SandboxID: current.ID,
			VMID:      current.VMID,
		},
	}
	frozen := operation.Encode(struct {
		Action  string
		Request operation.Intent
	}{"renew", operation.SanitizedCommand(command)})
	input := operation.Encode(struct {
		Sandbox ownership.Sandbox
		Before  Settings
		After   Settings
	}{current, settings, next})
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "tenant",
		ScopeID:   principal.TenantID,
		Key:       key,
		Hash:      operation.Digest(frozen),
		Request:   frozen,
		Status:    "succeeded",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = operations.InsertCredential(ctx, accepted.ID, mac); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "lease",
		Action:              "renew",
		InputHash:           operation.Digest(input),
		Input:               input,
		CandidateResourceID: current.ID,
		ExpectedInstance:    current.Instance,
		Status:              "succeeded",
		Body:                operation.Encode("complete"),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).ReplaceSettings(ctx, dbgen.ReplaceSettingsParams{
		SandboxID:    current.ID,
		PreviousBody: operation.Encode(settings),
		NextBody:     operation.Encode(next),
	})); err != nil {
		return empty, err
	}
	if err = l.fault("renew_before_commit"); err != nil {
		return empty, err
	}
	if err = l.committer.Complete(tx, operation.Outcome{
		ID:     accepted.ID,
		Result: accepted.Result,
		Frozen: true,
	}); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	return accepted, l.store.Checkpoint()
}

// Reap submits normal durable workflows with the exact observed lease deadline.
// Renewal winning the configuration guard invalidates that expiration intent.
func (l *Lifecycle) Reap(ctx context.Context) error {
	candidates, err := LeaseCandidates(ctx, l.store.Database)
	if err != nil {
		return err
	}
	type expiration struct {
		id       string
		tenant   string
		action   string
		deadline time.Time
	}
	var pending []expiration
	now := time.Now().UTC()
	for _, candidate := range candidates {
		deadline := candidate.Settings.Deadline
		action := "release"
		if candidate.State == "paused" {
			deadline = candidate.Settings.PausedUntil
		} else if candidate.Settings.OnExpire == "pause" {
			action = "pause"
		}
		if !deadline.IsZero() && !now.Before(deadline) {
			pending = append(pending, expiration{candidate.ID, candidate.TenantID, action, deadline})
		}
	}
	var failures []error
	for _, item := range pending {
		_, err := l.AdmitLifecycle(ctx, authorization.Principal{
			TenantID: item.tenant,
		}, "expire:"+item.id+":"+item.deadline.Format(time.RFC3339Nano), operation.Intent{
			Action:        item.action,
			SandboxID:     item.id,
			LeaseDeadline: item.deadline,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("expire %s: %w", item.id, err))
		}
	}
	return errors.Join(failures...)
}
