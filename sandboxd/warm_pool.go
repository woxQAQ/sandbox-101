package sandboxd

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Reconstruct the streak from durable terminal intents so a restart cannot reset
// a capacity failure into a burst of clone attempts. A successful refill resets it.
func (s *Service) refillBackoff(template string, now time.Time) (bool, error) {
	rows, err := s.store.db.QueryContext(s.ctx, `SELECT body FROM operations
 WHERE status IN ('failed','succeeded') AND json_extract(body,'$.command.action')='refill'
 AND json_extract(body,'$.command.template_id')=? ORDER BY rowid DESC LIMIT 10`, template)
	if err != nil {
		return true, err
	}
	defer func() { _ = rows.Close() }()
	failures := 0
	var latest time.Time
	for rows.Next() {
		var raw []byte
		var o Operation
		if err = rows.Scan(&raw); err != nil {
			return true, err
		}
		if err = json.Unmarshal(raw, &o); err != nil {
			return true, err
		}
		if o.Status == "succeeded" {
			break
		}
		if failures == 0 {
			latest = o.FinishedAt
			if latest.IsZero() {
				latest = o.CreatedAt
			}
		}
		failures++
	}
	if err = rows.Err(); err != nil {
		return true, err
	}
	if failures == 0 {
		return false, nil
	}
	delay := time.Second << min(failures-1, 8)
	return now.Before(latest.Add(delay)), nil
}

func (s *Service) poolOccupationTx(
	ctx context.Context,
	e executor,
	t Template,
) (int, error) {
	var count int
	err := e.QueryRowContext(ctx, `SELECT count(*) FROM resource_recovery r WHERE json_extract(r.body,'$.kind')='allocation_group' AND json_extract(r.body,'$.allocation_group.subject.kind')='warm' AND json_extract(r.body,'$.allocation_group.template.id')=? AND EXISTS(SELECT 1 FROM json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.resolution.state') IN ('unresolved','result_known','cleanup') OR (json_extract(a.value,'$.resolution.state')='active' AND json_extract(a.value,'$.resolution.destination') LIKE 'warm:%'))`, t.ID).
		Scan(&count)
	return count, err
}

func (s *Service) admitRefill(t Template) (*Operation, error) {
	pool := s.lock("pool:" + t.ID)
	defer pool()
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	current, err := getTemplateTx(s.ctx, tx, t.ID)
	if err != nil {
		return nil, err
	}
	if current.State != "ready" || current.TenantID != t.TenantID ||
		current.Shape != t.Shape ||
		current.SnapshotID != t.SnapshotID {
		return nil, nil
	}
	occupied, err := s.poolOccupationTx(s.ctx, tx, current)
	if err != nil {
		return nil, err
	}
	if occupied >= current.TargetWarm {
		return nil, nil
	}
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		return nil, nil
	}
	o := Operation{
		ID:             newID("op_"),
		Key:            newID("refill_"),
		TenantID:       t.TenantID,
		ChargeTenantID: t.TenantID,
		Command: Command{
			Action:     "refill",
			TemplateID: t.ID,
			Template:   &current,
		},
		Status:    "pending",
		CreatedAt: time.Now().UTC(),
	}
	o.hash = s.store.sign(o.ID)
	if err = s.quota(
		s.ctx,
		tx,
		Principal{TenantID: t.TenantID},
		0,
		t.Shape.DiskMiB,
	); err != nil {
		if !isCode(err, ErrExhausted) && !isCode(err, ErrNotFound) {
			return nil, err
		}
		o.Status = "failed"
		o.FinishedAt = time.Now().UTC()
		o.Error = internalError(err)
		if err = saveOp(s.ctx, tx, o); err != nil {
			return nil, err
		}
		return nil, tx.Commit()
	}
	if err = requireSnapshotOwner(
		s.ctx,
		tx,
		t.SnapshotID,
		t.TenantID,
	); err != nil {
		return nil, err
	}
	r := newAllocationGroup(
		o,
		allocationSubject{Kind: "warm", ID: o.ID},
		current,
	)
	if err = addGroupAllocation(
		s.ctx,
		tx,
		&r,
		o,
		"warm-clone",
		"vm_disk",
		t.Shape.DiskMiB,
		api.Request{Action: "clone", SnapshotID: t.SnapshotID},
		true,
	); err != nil {
		return nil, err
	}
	if err = putQuotaRef(
		s.ctx,
		tx,
		t.TenantID,
		"snapshot",
		t.SnapshotID,
		quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
	); err != nil {
		return nil, err
	}
	if err = insertRecovery(s.ctx, tx, r); err != nil {
		return nil, err
	}
	if err = saveOp(s.ctx, tx, o); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &o, nil
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
			if lookup != nil && !isEngineCode(lookup, api.NotFound) {
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
			if isEngineCode(lookup, api.NotFound) {
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
		return engineFailure(err)
	}
	if err = validAllocationIdentity(group, vm); err != nil {
		return err
	}
	if vm.Phase != "running" || !vm.AgentReady || vm.NetworkActual {
		return Fail(ErrConflict, "warm instance is not isolated and ready")
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
		return engineFailure(err)
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
		return Fail(ErrConflict, "warm publication was superseded")
	}
	var ready int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM warm WHERE template_id=? AND snapshot_id=? AND vm_id!=?`, t.ID, t.SnapshotID, vm.ID).
		Scan(&ready); err != nil {
		return err
	}
	if ready >= current.TargetWarm {
		return Fail(ErrConflict, "warm pool is full")
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
