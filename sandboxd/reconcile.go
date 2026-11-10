package sandboxd

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) recoverOperations() error {
	rows, err := s.store.db.QueryContext(
		s.ctx,
		"SELECT body FROM operations WHERE status IN ('pending','running') ORDER BY id",
	)
	if err != nil {
		return err
	}
	var ops []Operation
	for rows.Next() {
		var raw []byte
		var o Operation
		if err = rows.Scan(&raw); err != nil {
			return errors.Join(err, rows.Close())
		}
		if err = json.Unmarshal(raw, &o); err != nil {
			return errors.Join(err, rows.Close())
		}
		ops = append(ops, o)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for _, o := range ops {
		s.start(o)
	}
	return nil
}

func (s *Service) maintain() {
	t := time.NewTicker(s.opts.Interval)
	defer t.Stop()
	for {
		if s.ctx.Err() != nil {
			return
		}
		s.reconcile()
		s.reap()
		s.pools()
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (s *Service) reap() {
	all, err := s.store.sandboxes(s.ctx)
	if err != nil {
		return
	}
	now := time.Now()
	for _, b := range all {
		var action, key string
		var deadline time.Time
		if b.State == "running" && !now.Before(b.Deadline) {
			deadline = b.Deadline
			action = "release"
			if b.OnExpire == "pause" {
				action = "pause"
			}
			key = "reap:" + b.ID + ":" + b.Deadline.UTC().
				Format(time.RFC3339Nano) +
				":" + newID(
				"",
			)
		}
		if b.State == "paused" && !now.Before(b.PausedUntil) {
			deadline = b.PausedUntil
			action = "release"
			key = "paused-expiry:" + b.ID + ":" + b.SnapshotID
		}
		// Terminated resources belong to their durable compensation intent.
		// A fresh release would replace its receipt and retry sequence.
		if action != "" {
			_, _ = s.Submit(
				s.ctx,
				Principal{TenantID: b.TenantID},
				key,
				Command{
					Action:        action,
					SandboxID:     b.ID,
					LeaseDeadline: deadline,
				},
			)
		}
	}
}

func (s *Service) reconcile() {
	vms, inventoryErr := s.opts.Engine.ListVMs(s.ctx)
	// An unavailable inventory never prevents revocation of our own proxies.
	byID := map[string]api.VM{}
	for _, v := range vms {
		byID[v.ID] = v
	}
	all, err := s.store.sandboxes(s.ctx)
	if err != nil {
		return
	}
	for _, record := range all {
		if record.State != "running" {
			continue
		}
		unlock := s.lock(record.ID)
		b, e := getSandbox(s.ctx, s.store.db, record.ID)
		if e != nil || b.State != "running" {
			unlock()
			continue
		}
		v, ok := byID[b.VMID]
		fence, fenceErr := configFence(s.ctx, s.store.db, b.ID)
		if fenceErr != nil || fence != "" {
			// Proxy ownership is local and does not depend on VM verification.
			// NIC mutation still requires the exact verified instance.
			s.closeConnections(b.ID)
			if s.opts.Egress != nil {
				_ = s.revoke(b.ID)
			}
			if !ok || v.Instance != b.Instance {
				v, e = s.opts.Engine.GetVM(s.ctx, b.VMID)
				ok = e == nil
			}
			if ok && v.Verified && v.Instance == b.Instance {
				_, _ = api.Wait(s.ctx, s.opts.Engine, api.Request{
					Key:              "config-fence-off:" + s.epoch + ":" + b.ID + ":" + v.Instance,
					Action:           "network",
					VMID:             v.ID,
					ExpectedInstance: v.Instance,
					NetworkEnabled:   false,
				})
			}
			unlock()
			continue
		}
		if inventoryErr != nil {
			unlock()
			continue
		}
		// Inventory could predate a just-finished resume; confirm the current instance under lock.
		if !ok || v.Instance != b.Instance {
			v, e = s.opts.Engine.GetVM(s.ctx, b.VMID)
			if e != nil && !isEngineCode(e, api.NotFound) {
				unlock()
				continue
			}
			ok = e == nil
		}
		if !ok || (v.Verified && v.Phase == "stopped") ||
			v.Instance != b.Instance {
			s.closeConnections(b.ID)
			if s.opts.Egress != nil {
				_ = s.revoke(b.ID)
			}
			b.State = "failed"
			b.LastError = "engine VM exited or changed instance; memory state is unavailable"
			_ = saveSandbox(s.ctx, s.store.db, b)
			unlock()
			continue
		}
		if !v.Verified {
			unlock()
			continue
		}
		if s.credentialsMissing(b) {
			s.closeConnections(b.ID)
			if s.opts.Egress != nil {
				_ = s.revoke(b.ID)
			}
			key := "secret-loss:" + s.epoch + ":" + b.ID + ":" + v.Instance
			_, _ = api.Wait(
				s.ctx,
				s.opts.Engine,
				api.Request{
					Key:              key,
					Action:           "network",
					VMID:             v.ID,
					ExpectedInstance: v.Instance,
					NetworkEnabled:   false,
				},
			)
			b.LastError = "host credentials must be supplied again after daemon restart"
			_ = saveSandbox(s.ctx, s.store.db, b)
		} else if s.opts.Egress != nil && b.Policy.Mode != "none" {
			s.mu.Lock()
			bound := s.bindings[b.ID] != ""
			s.mu.Unlock()
			if !bound {
				// A new daemon owns a fresh proxy CA; install it in the retained guest
				// before reopening admission, without resetting identity or lease time.
				o := Operation{
					ID: "rebind:" + s.epoch + ":" + b.ID + ":" + b.Instance,
				}
				if e = s.activate(o, &b, false); e != nil {
					s.closeConnections(b.ID)
					b.LastError = "egress policy could not be restored"
					_ = saveSandbox(s.ctx, s.store.db, b)
				}
			} else {
				// Validate the effective fingerprint through the shared bind
				// path without resetting the guest or replaying activation.
				configuration := s.lock("configuration:" + b.ID)
				if e = s.activationFence(Operation{}, b.ID); e == nil {
					e = s.bind(s.ctx, b, v.VSockPath)
				}
				configuration()
				if e != nil {
					s.closeConnections(b.ID)
					_ = s.revoke(b.ID)
					b.LastError = "egress policy could not be restored"
					_ = saveSandbox(s.ctx, s.store.db, b)
				}
			}
		} else if s.opts.Egress != nil {
			// None owns no proxy binding. The manager closes only its own
			// residual proxy streams; healthy workload relays stay connected.
			// Revoke even when local fingerprint bookkeeping is absent.
			if e = s.revoke(b.ID); e != nil {
				s.closeConnections(b.ID)
				b.LastError = "egress policy could not be revoked"
				_ = saveSandbox(s.ctx, s.store.db, b)
			}
		}
		unlock()
	}
	if err := s.recoverResources(); err != nil && s.ctx.Err() == nil {
		_ = s.store.audit(
			s.ctx,
			map[string]string{
				"event": "resource_recovery_failed",
				"error": err.Error(),
			},
		)
	}
}
