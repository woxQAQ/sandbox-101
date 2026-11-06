package sandboxd

import (
	"context"
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
	vms, err := s.opts.Engine.ListVMs(s.ctx)
	if err != nil {
		return
	} // An unavailable query is never an empty inventory.
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
		// Inventory could predate a just-finished resume; confirm the current instance under lock.
		if !ok || v.Instance != b.Instance {
			v, e = s.opts.Engine.GetVM(s.ctx, b.VMID)
			if e != nil && !isCode(e, api.NotFound) {
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
		fence, fenceErr := configFence(s.ctx, s.store.db, b.ID)
		if fenceErr != nil || fence != "" {
			// A fenced (or unverifiable) configuration boundary is never
			// reactivated by maintenance. Close the data path by proven
			// instance only; the fence owner keeps the diagnostics.
			s.closeConnections(b.ID)
			if s.opts.Egress != nil {
				_ = s.revoke(b.ID)
			}
			_, _ = api.Wait(
				s.ctx,
				s.opts.Engine,
				api.Request{
					Key: "config-fence-off:" + s.epoch + ":" +
						b.ID + ":" + v.Instance,
					Action:           "network",
					VMID:             v.ID,
					ExpectedInstance: v.Instance,
					NetworkEnabled:   false,
				},
			)
			unlock()
			continue
		}
		if s.CredentialsMissing(b) {
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
			}
		} else if s.opts.Egress != nil {
			// A none policy never retains a residual proxy binding.
			s.mu.Lock()
			bound := s.bindings[b.ID] != ""
			s.mu.Unlock()
			if bound {
				s.closeConnections(b.ID)
				_ = s.revoke(b.ID)
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

func (s *Service) pools() {
	templates, err := s.store.templates(s.ctx)
	if err != nil {
		return
	}
	for _, t := range templates {
		if t.State != "ready" || t.SnapshotID == "" ||
			requireSnapshotOwner(
				s.ctx,
				s.store.db,
				t.SnapshotID,
				t.TenantID,
			) != nil {
			continue
		}
		rows, e := s.store.db.QueryContext(
			s.ctx,
			"SELECT vm_id,snapshot_id FROM warm WHERE template_id=?",
			t.ID,
		)
		if e != nil {
			continue
		}
		type member struct{ vm, snap string }
		var members []member
		for rows.Next() {
			var m member
			if e = rows.Scan(&m.vm, &m.snap); e != nil {
				break
			}
			members = append(members, m)
		}
		re := errors.Join(rows.Err(), rows.Close())
		if e != nil || re != nil {
			continue
		}
		healthy := 0
		for _, m := range members {
			v, e := s.opts.Engine.GetVM(s.ctx, m.vm)
			if e != nil && !isCode(e, api.NotFound) {
				continue
			}
			if m.snap == t.SnapshotID && e == nil && v.Verified &&
				v.Phase == "running" &&
				v.AgentReady &&
				!v.NetworkActual {
				probe, cancel := context.WithTimeout(s.ctx, time.Second)
				e = s.opts.Agent.Info(probe, v)
				cancel()
				if e == nil && healthy < t.TargetWarm {
					healthy++
					continue
				}
			}
			// Transfer out of the pool and delete under the same guard as claim.
			unlock := s.lock("pool:" + t.ID)
			ownership := s.lockResourceOwnership()
			res, e := s.store.db.ExecContext(
				s.ctx,
				"DELETE FROM warm WHERE vm_id=?",
				m.vm,
			)
			n := int64(0)
			if e == nil {
				n, _ = res.RowsAffected()
			}
			if n == 1 {
				_, _ = api.Wait(
					s.ctx,
					s.opts.Engine,
					api.Request{
						Key:    "warm-evict:" + m.vm,
						Action: "delete",
						VMID:   m.vm,
					},
				)
			}
			ownership()
			unlock()
		}
		if healthy >= t.TargetWarm {
			continue
		}
		s.mu.Lock()
		draining := s.draining
		s.mu.Unlock()
		if draining {
			continue
		}
		var pending int
		if e = s.store.db.QueryRowContext(s.ctx, "SELECT count(*) FROM operations WHERE status IN ('pending','running') AND json_extract(body,'$.command.action')='refill' AND json_extract(body,'$.command.template_id')=?", t.ID).
			Scan(&pending); e != nil {
			continue
		}
		backoff, e := s.refillBackoff(t.ID, time.Now())
		if e != nil || backoff {
			continue
		}
		for n := healthy + pending; n < t.TargetWarm; n++ {
			o := Operation{
				ID:       newID("op_"),
				Key:      newID("refill_"),
				TenantID: t.TenantID,
				Command: Command{
					Action:     "refill",
					TemplateID: t.ID,
					Template:   &t,
				},
				Status:    "pending",
				CreatedAt: time.Now().UTC(),
			}
			o.Hash = s.store.sign(o.ID)
			if e = saveOp(s.ctx, s.store.db, o); e != nil {
				break
			}
			s.start(o)
		}
	}
}

func (s *Service) refill(o *Operation) error {
	t := *o.Command.Template
	if err := requireSnapshotOwner(
		s.ctx,
		s.store.db,
		t.SnapshotID,
		o.TenantID,
	); err != nil {
		return err
	}
	v, err := s.engine(
		*o,
		"warm-clone",
		api.Request{Action: "clone", SnapshotID: t.SnapshotID},
	)
	if err != nil {
		return err
	}
	vm, err := s.opts.Engine.GetVM(s.ctx, v.VMID)
	if err != nil {
		return err
	}
	if vm.NetworkActual {
		return api.Fail(api.Conflict, "warm instance network is enabled")
	}
	if err = s.opts.Agent.Info(s.ctx, vm); err == nil {
		err = s.opts.Agent.Activate(
			s.ctx,
			vm,
			Activation{
				SandboxID:     "warm-" + vm.ID,
				Hostname:      "sandbox-warm",
				ResetIdentity: true,
				Env:           map[string]string{},
				Network:       "none",
			},
		)
	}
	if err != nil {
		_, cleanup := s.engine(
			*o,
			"warm-reject",
			api.Request{Action: "delete", VMID: vm.ID},
		)
		if cleanup != nil {
			return errors.Join(err, cleanup)
		}
		return err
	}
	unlock := s.lock("template:" + t.ID)
	defer unlock()
	current, err := s.store.template(s.ctx, t.ID)
	if err != nil {
		return err
	}
	if current.SnapshotID != t.SnapshotID {
		_, err = s.engine(
			*o,
			"warm-stale",
			api.Request{Action: "delete", VMID: vm.ID},
		)
		return err
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		t.SnapshotID,
		o.TenantID,
	); err != nil {
		return err
	}
	_, err = s.store.db.ExecContext(
		s.ctx,
		"INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?) ON CONFLICT(vm_id) DO NOTHING",
		vm.ID,
		t.ID,
		t.SnapshotID,
		time.Now().UnixNano(),
	)
	return err
}
