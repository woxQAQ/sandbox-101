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
			`SELECT vm_id,snapshot_id FROM warm WHERE template_id=? ORDER BY created_at,vm_id`,
			t.ID,
		)
		if e != nil {
			continue
		}
		type member struct{ vm, sid string }
		var members []member
		for rows.Next() {
			var m member
			if e = rows.Scan(&m.vm, &m.sid); e != nil {
				break
			}
			members = append(members, m)
		}
		if re := errors.Join(rows.Err(), rows.Close()); e != nil || re != nil {
			continue
		}
		healthy := 0
		for _, m := range members {
			var raw []byte
			if e = s.store.db.QueryRowContext(s.ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='warm' AND json_extract(body,'$.allocation_group.identity.vm')=?`, m.vm).
				Scan(&raw); e != nil {
				continue
			}
			var group resourceRecovery
			if e = json.Unmarshal(raw, &group); e != nil {
				continue
			}
			v, lookup := s.opts.Engine.GetVM(s.ctx, m.vm)
			if lookup != nil && !isCode(lookup, api.NotFound) {
				continue
			}
			if lookup == nil && validAllocationIdentity(group, v) == nil &&
				m.sid == t.SnapshotID &&
				v.Phase == "running" &&
				v.AgentReady &&
				!v.NetworkActual &&
				healthy < t.TargetWarm {
				probe, cancel := context.WithTimeout(s.ctx, time.Second)
				e = s.opts.Agent.Info(probe, v)
				cancel()
				if e == nil {
					healthy++
					continue
				}
			}
			pool := s.lock("pool:" + t.ID)
			if isCode(lookup, api.NotFound) {
				_ = s.retireAllocationAbsent(group.ID)
				pool()
				continue
			}
			collector, e := s.handoffAllocationCleanup(group.ID, v, true)
			pool()
			if e == nil {
				_ = s.consumeAllocationCleanup(collector)
			}
		}
		backoff, e := s.refillBackoff(t.ID, time.Now())
		if e != nil || backoff {
			continue
		}
		for n := 0; n < t.TargetWarm; n++ {
			o, e := s.admitRefill(t)
			if e != nil || o == nil {
				break
			}
			s.start(*o)
		}
	}
}

func (s *Service) refill(o *Operation) error {
	t := *o.Command.Template
	if err := requireSnapshotOwner(
		s.ctx,
		s.store.db,
		t.SnapshotID,
		o.ChargeTenantID,
	); err != nil {
		return err
	}
	proof, err := s.engine(
		*o,
		"warm-clone",
		api.Request{Action: "clone", SnapshotID: t.SnapshotID},
	)
	if err != nil {
		return err
	}
	group, err := s.loadRecovery(allocationGroupID(*o))
	if err != nil {
		return err
	}
	vm, err := s.opts.Engine.GetVM(s.ctx, proof.VMID)
	if err != nil {
		return err
	}
	if err = validAllocationIdentity(group, vm); err != nil {
		return err
	}
	if vm.Phase != "running" || !vm.AgentReady || vm.NetworkActual {
		return api.Fail(api.Conflict, "warm instance is not isolated and ready")
	}
	if err = s.opts.Agent.Info(s.ctx, vm); err != nil {
		return err
	}
	if err = s.opts.Agent.Activate(
		s.ctx,
		vm,
		Activation{
			SandboxID:     "warm-" + vm.ID,
			Hostname:      "sandbox-warm",
			ResetIdentity: true,
			Env:           map[string]string{},
			Network:       "none",
		},
	); err != nil {
		return err
	}
	// Recheck after guest IO before acquiring publication locks.
	currentVM, err := s.opts.Engine.GetVM(s.ctx, vm.ID)
	if err != nil {
		return err
	}
	if err = validAllocationIdentity(group, currentVM); err != nil {
		return err
	}
	template := s.lock("template:" + t.ID)
	defer template()
	pool := s.lock("pool:" + t.ID)
	defer pool()
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getTemplateTx(s.ctx, tx, t.ID)
	if err != nil {
		return err
	}
	if current.TenantID != t.TenantID || current.SnapshotID != t.SnapshotID ||
		current.Shape != t.Shape ||
		current.State != "ready" ||
		current.TargetWarm == 0 {
		return api.Fail(api.Conflict, "warm publication was superseded")
	}
	var ready int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM warm WHERE template_id=? AND snapshot_id=? AND vm_id!=?`, t.ID, t.SnapshotID, vm.ID).
		Scan(&ready); err != nil {
		return err
	}
	if ready >= current.TargetWarm {
		return api.Fail(api.Conflict, "warm pool is full")
	}
	group, err = loadRecoveryTx(s.ctx, tx, group.ID)
	if err != nil {
		return err
	}
	if err = validAllocationIdentity(group, currentVM); err != nil {
		return err
	}
	if err = requireSnapshotOwner(
		s.ctx,
		tx,
		t.SnapshotID,
		t.TenantID,
	); err != nil {
		return err
	}
	if err = putQuotaRef(
		s.ctx,
		tx,
		t.TenantID,
		"vm_disk",
		vm.ID,
		quotaRef{Kind: "active_use", ID: "warm:" + vm.ID},
	); err != nil {
		return err
	}
	if err = putQuotaRef(
		s.ctx,
		tx,
		t.TenantID,
		"snapshot",
		t.SnapshotID,
		quotaRef{Kind: "active_use", ID: "backing:" + vm.ID},
	); err != nil {
		return err
	}
	result, err := tx.ExecContext(
		s.ctx,
		`INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?) ON CONFLICT(vm_id) DO UPDATE SET template_id=excluded.template_id,snapshot_id=excluded.snapshot_id`,
		vm.ID,
		t.ID,
		t.SnapshotID,
		time.Now().UnixNano(),
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	group.Allocations[0].Resolution.State = "active"
	group.Allocations[0].Resolution.Destination = "warm:" + vm.ID
	group.Done = true
	if err = writeRecovery(s.ctx, tx, group); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
		t.TenantID,
		"operation:"+o.ID,
	); err != nil {
		return err
	}
	return tx.Commit()
}
