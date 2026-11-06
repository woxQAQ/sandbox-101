package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/woxQAQ/sandbox-engine/api"
)

// A quota resource is a conservative logical responsibility, independent of
// authorization, catalog visibility and engine deletion authority. A row with
// no references remains charged until reclamation is proven.
type quotaResource struct {
	Tenant  string
	Kind    string
	Key     string
	Slots   int64
	Storage int64
	Source  string
}

type quotaRef struct{ Kind, ID string }

func putQuotaResource(
	ctx context.Context,
	e executor,
	r quotaResource,
	ref quotaRef,
) error {
	if r.Tenant == "" || r.Key == "" || r.Source == "" || r.Slots < 0 ||
		r.Storage < 0 {
		return api.Fail(api.ManualRecovery, "incomplete quota responsibility")
	}
	if _, err := e.ExecContext(
		ctx,
		`INSERT INTO quota_resources(tenant_id,kind,resource_key,sandbox_slots,storage_mib,source_operation_id) VALUES(?,?,?,?,?,?) ON CONFLICT(tenant_id,kind,resource_key) DO NOTHING`,
		r.Tenant,
		r.Kind,
		r.Key,
		r.Slots,
		r.Storage,
		r.Source,
	); err != nil {
		return err
	}
	var slots, storage int64
	if err := e.QueryRowContext(ctx, `SELECT sandbox_slots,storage_mib FROM quota_resources WHERE tenant_id=? AND kind=? AND resource_key=?`, r.Tenant, r.Kind, r.Key).
		Scan(&slots, &storage); err != nil {
		return err
	}
	if slots != r.Slots || storage != r.Storage {
		return api.Fail(
			api.ManualRecovery,
			"quota resource cost conflicts with its identity",
		)
	}
	return putQuotaRef(ctx, e, r.Tenant, r.Kind, r.Key, ref)
}

func putQuotaRef(
	ctx context.Context,
	e executor,
	tenant, kind, key string,
	ref quotaRef,
) error {
	_, err := e.ExecContext(
		ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING`,
		tenant,
		kind,
		key,
		ref.Kind,
		ref.ID,
	)
	return err
}

// Quota has exactly one source: the durable resource table. Root bypasses the
// limit comparison only; the same admission transaction still records costs.
func (s *Service) quota(
	ctx context.Context,
	tx *sql.Tx,
	p Principal,
	count int,
	storage int64,
) error {
	if p.Root {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", p.TenantID).
		Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return api.Fail(api.NotFound, "not found")
		}
		return err
	}
	var t Tenant
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	var slots, used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?`, p.TenantID).
		Scan(&slots, &used); err != nil {
		return err
	}
	if slots+int64(count) > int64(t.MaxSandboxes) ||
		used+storage > t.MaxStorageMiB {
		return api.Fail(api.Exhausted, "tenant quota exceeded")
	}
	return nil
}

func reserveQuotaStep(
	ctx context.Context,
	e executor,
	o Operation,
	step, sandbox string,
	storage int64,
) error {
	return putQuotaResource(
		ctx,
		e,
		quotaResource{
			Tenant:  o.ChargeTenantID,
			Kind:    "pending_storage",
			Key:     stepKey(o, step),
			Storage: storage,
			Source:  o.ID,
		},
		quotaRef{Kind: "engine_dependency", ID: "sandbox:" + sandbox},
	)
}

func quotaSnapshot(
	ctx context.Context,
	e executor,
	o Operation,
	snapshot string,
	shape api.Shape,
	ref quotaRef,
) error {
	if snapshot == "" {
		return nil
	}
	if err := putQuotaResource(
		ctx,
		e,
		quotaResource{
			Tenant:  o.ChargeTenantID,
			Kind:    "snapshot",
			Key:     snapshot,
			Storage: shape.DiskMiB + shape.MemoryMiB,
			Source:  o.ID,
		},
		ref,
	); err != nil {
		return err
	}
	return protectQuotaSnapshotUsers(ctx, e, o.ChargeTenantID, snapshot)
}

// Catalog and warm holders protect an already billed snapshot identity. This
// creates no template, build or warm admission charge; those policies are 013.
func referenceQuotaSnapshot(
	ctx context.Context,
	e executor,
	tenant, snapshot string,
	ref quotaRef,
) error {
	_, err := e.ExecContext(
		ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT tenant_id,kind,resource_key,?,? FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=? ON CONFLICT DO NOTHING`,
		ref.Kind,
		ref.ID,
		tenant,
		snapshot,
	)
	return err
}

func protectQuotaSnapshotUsers(
	ctx context.Context,
	e executor,
	tenant, snapshot string,
) error {
	if _, err := e.ExecContext(
		ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT ?, 'snapshot', ?, 'active_use', 'template:'||id FROM templates WHERE tenant=? AND json_extract(body,'$.snapshot_id')=? ON CONFLICT DO NOTHING`,
		tenant,
		snapshot,
		tenant,
		snapshot,
	); err != nil {
		return err
	}
	_, err := e.ExecContext(
		ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT ?, 'snapshot', ?, 'active_use', 'backing:'||w.vm_id FROM warm w JOIN templates t ON t.id=w.template_id WHERE t.tenant=? AND w.snapshot_id=? ON CONFLICT DO NOTHING`,
		tenant,
		snapshot,
		tenant,
		snapshot,
	)
	return err
}

// quoteSnapshot reports only the incremental cost of a shared identity. It is
// called inside admission's write transaction before any intent is published.
func quoteSnapshot(
	ctx context.Context,
	e executor,
	tenant, snapshot string,
	shape api.Shape,
) (int64, error) {
	if snapshot == "" {
		return 0, nil
	}
	var storage int64
	err := e.QueryRowContext(ctx, `SELECT storage_mib FROM quota_resources WHERE tenant_id=? AND kind='snapshot' AND resource_key=?`, tenant, snapshot).
		Scan(&storage)
	if errors.Is(err, sql.ErrNoRows) {
		return shape.DiskMiB + shape.MemoryMiB, nil
	}
	if err != nil {
		return 0, err
	}
	if storage != shape.DiskMiB+shape.MemoryMiB {
		return 0, api.Fail(api.ManualRecovery, "snapshot quota shape mismatch")
	}
	return 0, nil
}

func quotaSlot(ctx context.Context, e executor, o Operation, b Sandbox) error {
	return putQuotaResource(
		ctx,
		e,
		quotaResource{
			Tenant: b.TenantID,
			Kind:   "sandbox_slot",
			Key:    b.ID,
			Slots:  1,
			Source: o.ID,
		},
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	)
}

// quotaAllocation describes only plan012 sandbox allocations. Template builds
// and pool admission policy remain separate work; existing warm transfers use
// the same sandbox reservation and physical identities as ordinary claims.
func (s *Service) quotaAllocation(
	o Operation,
	step string,
) (Sandbox, string, bool, error) {
	var id, kind string
	switch o.Command.Action {
	case "claim":
		if step == "provision" && len(o.IDs) == 1 {
			id = o.IDs[0]
			kind = "vm_disk"
		}
	case "fork":
		if step == "fork-snapshot" {
			id = o.Command.SandboxID
			kind = "snapshot"
		}
		for _, candidate := range o.IDs {
			if step == "fork-clone:"+candidate {
				id = candidate
				kind = "vm_disk"
			}
		}
	case "pause":
		if step == "hibernate-snapshot" {
			id = o.Command.SandboxID
			kind = "snapshot"
		}
	case "checkpoint":
		if step == "checkpoint-capture" {
			id = o.Command.SandboxID
			kind = "snapshot"
		}
	case "resume":
		if step == "resume-clone" {
			id = o.Command.SandboxID
			kind = "vm_disk"
		}
	case "rollback":
		if step == "rollback-clone" || step == "rollback-restore" {
			id = o.Command.SandboxID
			kind = "vm_disk"
		}
	}
	if kind == "" {
		return Sandbox{}, "", false, nil
	}
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		return b, kind, true, err
	}
	if o.ChargeTenantID == "" || o.ChargeTenantID != b.TenantID {
		return b, kind, true, api.Fail(
			api.ManualRecovery,
			"allocation has no trusted charge owner",
		)
	}
	return b, kind, true, nil
}

// acceptQuotaReceipt atomically replaces an unknown allocation reservation
// with its proven identity. Both failed and successful receipts may own bytes.
func (s *Service) acceptQuotaReceipt(
	o Operation,
	step string,
	request api.Request,
	receipt api.Operation,
) error {
	b, kind, tracked, err := s.quotaAllocation(o, step)
	if err != nil || !tracked {
		return err
	}
	key := receipt.VMID
	if kind == "snapshot" {
		key = receipt.SnapshotID
	}
	if step == "rollback-restore" && key != "" {
		key += ":restore:" + receipt.ID
	}
	if key == "" {
		if recoveryTerminal(receipt.Status) {
			_, err = s.store.db.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				o.ChargeTenantID,
				stepKey(o, step),
			)
			return err
		}
		return nil
	} // Unknown dispatch retains its reservation.
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var pending int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, o.ChargeTenantID, stepKey(o, step)).
		Scan(&pending); err != nil {
		return err
	}
	if recoveryTerminal(receipt.Status) {
		if _, err = tx.ExecContext(
			s.ctx,
			`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
			"allocation:"+stepKey(o, step),
		); err != nil {
			return err
		}
	}
	if pending == 0 {
		return tx.Commit()
	} // Reclaimed receipts never resurrect resources.
	storage := b.Shape.DiskMiB
	if kind == "snapshot" {
		storage += b.Shape.MemoryMiB
	}
	r := quotaResource{
		Tenant:  o.ChargeTenantID,
		Kind:    kind,
		Key:     key,
		Storage: storage,
		Source:  o.ID,
	}
	if err = putQuotaResource(
		s.ctx,
		tx,
		r,
		quotaRef{Kind: "cleanup", ID: "step:" + stepKey(o, step)},
	); err != nil {
		return err
	}
	if !recoveryTerminal(receipt.Status) {
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.Tenant,
			kind,
			key,
			quotaRef{
				Kind: "engine_dependency",
				ID:   "allocation:" + stepKey(o, step),
			},
		); err != nil {
			return err
		}
	}
	if kind == "vm_disk" {
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.Tenant,
			kind,
			key,
			quotaRef{Kind: "cleanup", ID: "vm:" + receipt.VMID},
		); err != nil {
			return err
		}
		holder := quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID}
		if b.State == "terminated" ||
			(recoveryTerminal(o.Status) && b.VMID != receipt.VMID) {
			holder.Kind = "cleanup"
		}
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.Tenant,
			kind,
			key,
			holder,
		); err != nil {
			return err
		}
		// Engine restore hardlinks the memory backing. Retain that responsibility
		// until the VM is reclaimed, even after its source catalog entry is gone.
		if request.SnapshotID != "" {
			if err = quotaSnapshot(
				s.ctx,
				tx,
				o,
				request.SnapshotID,
				b.Shape,
				quotaRef{Kind: "active_use", ID: "backing:" + receipt.VMID},
			); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
		o.ChargeTenantID,
		stepKey(o, step),
	); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) requireQuotaReservation(o Operation, step string) error {
	_, kind, tracked, err := s.quotaAllocation(o, step)
	if err != nil || !tracked {
		return err
	}
	var n int
	err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND ((kind='pending_storage' AND resource_key=?) OR (kind=? AND source_operation_id=? AND EXISTS (SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_id=?)))`, o.ChargeTenantID, stepKey(o, step), kind, o.ID, "step:"+stepKey(o, step)).
		Scan(&n)
	if err == nil && n == 0 {
		receipt, e := s.findResourceStep(o, step, "*")
		if e != nil {
			return e
		}
		if receipt != nil && recoveryTerminal(receipt.Status) {
			return nil
		}
		return api.Fail(
			api.ManualRecovery,
			"allocation has no durable reservation or receipt",
		)
	}
	if err != nil {
		return err
	}
	return err
}

func (s *Service) beginQuotaDispatch(o Operation, step string) error {
	_, err := s.store.db.ExecContext(
		s.ctx,
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) SELECT tenant_id,kind,resource_key,'engine_dependency',? FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? ON CONFLICT DO NOTHING`,
		"dispatch:"+stepKey(o, step),
		o.ChargeTenantID,
		stepKey(o, step),
	)
	return err
}

// A delete receipt proves catalog reclamation. VM backing users can retain
// the snapshot inode after catalog removal, so they keep the resource charged.
func (s *Service) reclaimQuota(kind, key string) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if kind == "vm_disk" {
		var unresolved int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs r WHERE r.kind='vm_disk' AND r.holder_kind='engine_dependency' AND (r.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs v WHERE v.tenant_id=r.tenant_id AND v.kind=r.kind AND v.resource_key=r.resource_key AND v.holder_id=?))`, key, "vm:"+key).
			Scan(&unresolved); err != nil {
			return err
		}
		if unresolved > 0 {
			return nil
		}
		if _, err = tx.ExecContext(
			s.ctx,
			`DELETE FROM quota_refs WHERE holder_id=?`,
			"backing:"+key,
		); err != nil {
			return err
		}
	}
	query := `DELETE FROM quota_resources WHERE kind=? AND resource_key=?`
	if kind == "vm_disk" {
		query = `DELETE FROM quota_resources WHERE kind=? AND (resource_key=? OR EXISTS(SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_id=?))`
	}
	if kind == "snapshot" {
		query += ` AND NOT EXISTS(SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_kind IN ('active_use','engine_dependency'))`
	}
	args := []any{kind, key}
	if kind == "vm_disk" {
		args = append(args, "vm:"+key)
	}
	if _, err = tx.ExecContext(s.ctx, query, args...); err != nil {
		return err
	}
	if err = retireQuotaSlots(s.ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

// A terminated sandbox retains its slot through unknown allocations and
// exclusive sleep cleanup. Independent live users can take over a snapshot;
// cleanup references alone never constitute such a transfer.
func retireQuotaSlots(ctx context.Context, e executor) error {
	_, err := e.ExecContext(
		ctx,
		`DELETE FROM quota_resources AS slot WHERE slot.kind='sandbox_slot'
 AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.kind=slot.kind AND r.resource_key=slot.resource_key AND r.holder_kind='cleanup')
 AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.holder_id='sandbox:'||slot.resource_key AND r.kind IN ('vm_disk','pending_storage'))
 AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.holder_id='sandbox:'||slot.resource_key AND r.kind='snapshot'
 AND NOT EXISTS(SELECT 1 FROM quota_refs live WHERE live.tenant_id=r.tenant_id AND live.kind=r.kind AND live.resource_key=r.resource_key AND live.holder_kind='active_use' AND live.holder_id!=r.holder_id))`,
	)
	return err
}

func quotaTerminate(ctx context.Context, e executor, b Sandbox) error {
	if _, err := e.ExecContext(
		ctx,
		`UPDATE OR IGNORE quota_refs SET holder_kind='cleanup' WHERE tenant_id=? AND holder_id=?`,
		b.TenantID,
		"sandbox:"+b.ID,
	); err != nil {
		return err
	}
	if _, err := e.ExecContext(
		ctx,
		`DELETE FROM quota_refs WHERE tenant_id=? AND holder_id=? AND holder_kind!='cleanup'`,
		b.TenantID,
		"sandbox:"+b.ID,
	); err != nil {
		return err
	}
	return retireQuotaSlots(ctx, e)
}

func (s *Service) saveQuotaSandbox(b Sandbox) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if b.SnapshotID != "" {
		o := Operation{ID: b.OperationID, ChargeTenantID: b.TenantID}
		if o.ID == "" {
			o.ID = "sandbox:" + b.ID
		}
		ref := quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID}
		if b.State == "terminated" {
			ref.Kind = "cleanup"
		}
		if err = requireSnapshotOwner(
			s.ctx,
			tx,
			b.SnapshotID,
			b.TenantID,
		); err != nil {
			return api.Fail(
				api.ManualRecovery,
				"snapshot responsibility has no trusted provenance",
			)
		}
		if err = quotaSnapshot(
			s.ctx,
			tx,
			o,
			b.SnapshotID,
			b.Shape,
			ref,
		); err != nil {
			return err
		}
	}
	if b.State == "terminated" {
		if err = quotaTerminate(s.ctx, tx, b); err != nil {
			return err
		}
	}
	if err = saveSandbox(s.ctx, tx, b); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) validateQuota(ctx context.Context) error {
	var bad int
	err := s.db.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM quota_resources WHERE tenant_id='' OR resource_key='' OR source_operation_id='' OR sandbox_slots<0 OR storage_mib<0 OR kind NOT IN ('sandbox_slot','vm_disk','snapshot','pending_storage') OR (kind='sandbox_slot' AND (sandbox_slots!=1 OR storage_mib!=0)) OR (kind!='sandbox_slot' AND sandbox_slots!=0))+
 (SELECT count(*) FROM quota_refs r LEFT JOIN quota_resources q USING(tenant_id,kind,resource_key) WHERE q.resource_key IS NULL OR r.holder_id='' OR r.holder_kind NOT IN ('active_use','engine_dependency','cleanup'))+
 (SELECT count(*) FROM quota_resources WHERE typeof(sandbox_slots)!='integer' OR typeof(storage_mib)!='integer')+
 (SELECT count(*) FROM sandboxes b WHERE b.state!='terminated' AND NOT EXISTS(SELECT 1 FROM quota_resources q WHERE q.tenant_id=b.tenant AND q.kind='sandbox_slot' AND q.resource_key=b.id))`).Scan(&bad)
	if err != nil {
		return err
	}
	if bad != 0 {
		return api.Fail(
			api.ManualRecovery,
			"quota ledger contains inconsistent responsibility",
		)
	}
	return nil
}

func (s *Service) finishQuotaOperation(o Operation) error {
	// Only a trusted terminal receipt without an allocated identity can cancel
	// an allocation. Absence of a receipt is not evidence of non-dispatch.
	steps := recoveryAllocationSteps(o)
	if o.Command.Action == "rollback" {
		steps = append(steps, resourceStep{"rollback-restore", "restore"})
	}
	keepDependencies := false
	for _, step := range steps {
		if o.Command.Action == "refill" && step.name == "warm-clone" {
			proof, e := s.findResourceStep(o, step.name, step.action)
			if e != nil {
				return e
			}
			if proof == nil || !recoveryTerminal(proof.Status) {
				keepDependencies = true
			} else if proof.VMID != "" && o.Command.Template != nil {
				if e = referenceQuotaSnapshot(
					s.ctx,
					s.store.db,
					o.ChargeTenantID,
					o.Command.Template.SnapshotID,
					quotaRef{Kind: "active_use", ID: "backing:" + proof.VMID},
				); e != nil {
					return e
				}
			}
		}
		_, kind, tracked, err := s.quotaAllocation(o, step.name)
		if err != nil {
			return err
		}
		if !tracked {
			continue
		}
		proof, err := s.findResourceStep(o, step.name, step.action)
		if err != nil {
			return err
		}
		if proof == nil {
			if _, err = s.store.db.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=? AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=quota_resources.tenant_id AND r.kind=quota_resources.kind AND r.resource_key=quota_resources.resource_key AND r.holder_id=?)`,
				o.ChargeTenantID,
				stepKey(o, step.name),
				"dispatch:"+stepKey(o, step.name),
			); err != nil {
				return err
			}
			continue
		}
		if !recoveryTerminal(proof.Status) {
			keepDependencies = true
			continue
		}
		if err = s.acceptQuotaReceipt(
			o,
			step.name,
			proof.Request,
			*proof,
		); err != nil {
			return err
		}
		if (kind == "vm_disk" && proof.VMID == "") ||
			(kind == "snapshot" && proof.SnapshotID == "") {
			if _, err = s.store.db.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				o.ChargeTenantID,
				stepKey(o, step.name),
			); err != nil {
				return err
			}
		}
	}
	// Dependency refs belong to the operation, not its public result. Physical
	// allocations retain their cleanup/backing/sandbox holders independently.
	if keepDependencies {
		return nil
	}
	_, err := s.store.db.ExecContext(
		s.ctx,
		`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=? AND NOT EXISTS(SELECT 1 FROM quota_resources r WHERE r.source_operation_id=? AND r.kind='pending_storage')`,
		"operation:"+o.ID,
		o.ID,
	)
	if err != nil {
		return err
	}
	_, err = s.store.db.ExecContext(
		s.ctx,
		`DELETE FROM quota_resources WHERE kind='sandbox_slot' AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=quota_resources.tenant_id AND r.kind=quota_resources.kind AND r.resource_key=quota_resources.resource_key AND r.holder_kind='cleanup') AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=quota_resources.tenant_id AND r.holder_id='sandbox:'||quota_resources.resource_key AND r.kind IN ('vm_disk','snapshot','pending_storage'))`,
	)
	return err
}

// Recheck catalog absence after references move. No-ref resources remain
// charged here; a failed query never authorizes retirement.
func (s *Service) reconcileQuota() error {
	rows, err := s.store.db.QueryContext(
		s.ctx,
		`SELECT 'snapshot',resource_key FROM quota_resources WHERE kind='snapshot' UNION SELECT 'vm_disk',substr(holder_id,9) FROM quota_refs WHERE holder_id LIKE 'backing:%' ORDER BY 1 DESC`,
	)
	if err != nil {
		return err
	}
	type proofTarget struct{ kind, key string }
	var targets []proofTarget
	for rows.Next() {
		var target proofTarget
		if err = rows.Scan(&target.kind, &target.key); err != nil {
			_ = rows.Close()
			return err
		}
		targets = append(targets, target)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, target := range targets {
		if target.kind == "vm_disk" {
			_, err = s.opts.Engine.GetVM(s.ctx, target.key)
		} else {
			_, err = s.opts.Engine.GetSnapshot(s.ctx, target.key)
		}
		if isCode(err, api.NotFound) {
			if err = s.reclaimQuota(target.kind, target.key); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) reserveWarmReplacement(o Operation, b Sandbox) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = s.quota(
		s.ctx,
		tx,
		Principal{TenantID: b.TenantID},
		0,
		b.Shape.DiskMiB,
	); err != nil {
		return err
	}
	if err = reserveQuotaStep(
		s.ctx,
		tx,
		o,
		"provision",
		b.ID,
		b.Shape.DiskMiB,
	); err != nil {
		return err
	}
	if err = saveSandbox(s.ctx, tx, b); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) admitQuotaRecovery(
	o Operation,
	b Sandbox,
	step string,
	storage int64,
) error {
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var n int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, o.ChargeTenantID, stepKey(o, step)).
		Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if err = s.quota(
			s.ctx,
			tx,
			Principal{TenantID: b.TenantID},
			0,
			storage,
		); err != nil {
			return err
		}
		if err = reserveQuotaStep(
			s.ctx,
			tx,
			o,
			step,
			b.ID,
			storage,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}
