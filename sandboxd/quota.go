package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"strings"
	"time"

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

// A destination needs the producer's exact responsibility type as well as its
// identity. EXISTS keeps coexisting cleanup and active references from counting
// the same resource twice.
func requireQuotaResource(
	ctx context.Context,
	e executor,
	r quotaResource,
	ref quotaRef,
) error {
	var count int
	if err := e.QueryRowContext(ctx, `SELECT count(*) FROM quota_resources q WHERE q.tenant_id=? AND q.kind=? AND q.resource_key=? AND q.sandbox_slots=? AND q.storage_mib=? AND q.source_operation_id=? AND EXISTS(SELECT 1 FROM quota_refs f WHERE f.tenant_id=q.tenant_id AND f.kind=q.kind AND f.resource_key=q.resource_key AND f.holder_kind=? AND f.holder_id=?)`, r.Tenant, r.Kind, r.Key, r.Slots, r.Storage, r.Source, ref.Kind, ref.ID).
		Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return api.Fail(
			api.ManualRecovery,
			"allocation lost its cost or typed destination reference",
		)
	}
	return nil
}

// Pending sandbox references move to cleanup with access revocation. Known
// allocations already carry cleanup references until a destination is delivered.
func groupAllocationRef(
	ctx context.Context,
	e executor,
	r resourceRecovery,
	pending bool,
) (quotaRef, error) {
	ref := quotaRef{Kind: "cleanup", ID: groupHolder(r)}
	if !pending {
		return ref, nil
	}
	ref.Kind = "engine_dependency"
	if r.Group.Subject.Kind == "sandbox" {
		b, err := getSandbox(ctx, e, r.Group.Subject.ID)
		if err != nil {
			return quotaRef{}, err
		}
		if b.State == "terminated" {
			ref.Kind = "cleanup"
		}
	}
	return ref, nil
}

func requireSQLWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return api.Fail(
			api.ManualRecovery,
			"required responsibility write did not affect one row",
		)
	}
	return nil
}

// Subjects identify responsibility without manufacturing a user sandbox.
type allocationSubject struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type allocationContext struct {
	Subject        allocationSubject
	ChargeTenantID string
	Shape          api.Shape
	Holder         string
	Sandbox        *Sandbox
}

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
	if err != nil {
		return err
	}
	var present int
	err = e.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?`, tenant, kind, key, ref.Kind, ref.ID).
		Scan(&present)
	if err == nil && present != 1 {
		return api.Fail(api.ManualRecovery, "quota reference was not persisted")
	}
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

// quotaAllocation supplies trusted billing inputs for a real subject.
func (s *Service) quotaAllocation(
	o Operation,
	step string,
) (allocationContext, string, bool, error) {
	if a, err := s.quotaRecoveryAllocation(o, step); err != nil {
		return allocationContext{}, "", true, err
	} else if a != nil {
		if a.ChargeTenantID != o.ChargeTenantID {
			return allocationContext{}, "", true, api.Fail(
				api.ManualRecovery,
				"recovery charge owner mismatch",
			)
		}
		subject := a.Subject
		if subject.Kind == "" {
			subject = allocationSubject{Kind: "sandbox", ID: a.SandboxID}
		}
		return allocationContext{
			Subject:        subject,
			ChargeTenantID: a.ChargeTenantID,
			Shape:          a.Shape,
			Holder:         subject.Kind + ":" + subject.ID,
		}, a.Kind, true, nil
	}
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
		return allocationContext{}, "", false, nil
	}
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		return allocationContext{}, kind, true, err
	}
	if o.ChargeTenantID == "" || o.ChargeTenantID != b.TenantID {
		return allocationContext{}, kind, true, api.Fail(
			api.ManualRecovery,
			"allocation has no trusted charge owner",
		)
	}
	return allocationContext{
		Subject:        allocationSubject{Kind: "sandbox", ID: b.ID},
		ChargeTenantID: b.TenantID,
		Shape:          b.Shape,
		Holder:         "sandbox:" + b.ID,
		Sandbox:        &b,
	}, kind, true, nil
}

// acceptQuotaReceipt atomically replaces an unknown allocation reservation
// with its proven identity. Both failed and successful receipts may own bytes.
func (s *Service) acceptQuotaReceipt(
	o Operation,
	step string,
	request api.Request,
	receipt api.Operation,
) error {
	a, err := s.quotaRecoveryAllocation(o, step)
	if err != nil {
		return err
	}
	if a != nil && receipt.Key != "" {
		if err = a.validate(receipt); err != nil {
			return err
		}
	}
	b, kind, tracked, err := s.quotaAllocation(o, step)
	if err != nil || !tracked {
		return err
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err = s.acceptQuotaReceiptTx(
		tx,
		o,
		step,
		request,
		receipt,
		b,
		kind,
		a,
	); err != nil {
		return err
	}
	if a != nil && recoveryTerminal(receipt.Status) && receipt.Key != "" {
		if err = s.recordAllocationResultTx(tx, *a, receipt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) acceptQuotaReceiptTx(
	tx *sql.Tx,
	o Operation,
	step string,
	request api.Request,
	receipt api.Operation,
	b allocationContext,
	kind string,
	a *recoveryAllocation,
) error {
	key := receipt.VMID
	if kind == "snapshot" {
		key = receipt.SnapshotID
	}
	if step == "rollback-restore" && key != "" {
		key += ":restore:" + receipt.ID
	}
	if key == "" {
		if recoveryTerminal(receipt.Status) {
			_, err := tx.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				o.ChargeTenantID,
				stepKey(o, step),
			)
			return err
		}
		return nil
	} // Unknown dispatch retains its reservation.
	var err error
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
		return nil
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
	if a != nil && kind == "snapshot" {
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.Tenant,
			kind,
			key,
			quotaRef{Kind: "cleanup", ID: "sandbox:" + a.SandboxID},
		); err != nil {
			return err
		}
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
		holder := quotaRef{Kind: "active_use", ID: b.Holder}
		if a != nil || b.Sandbox == nil || b.Sandbox.State == "terminated" ||
			(recoveryTerminal(o.Status) && b.Sandbox.VMID != receipt.VMID) {
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
	return nil
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
	if err = s.reclaimQuotaTx(tx, kind, key); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) reclaimQuotaTx(tx *sql.Tx, kind, key string) error {
	var err error
	if kind == "vm_disk" {
		var protected int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND (json_extract(body,'$.allocation_group.identity.vm')=? OR json_extract(body,'$.allocation_group.selection.identity.vm')=?) AND (json_extract(body,'$.allocation_group.gate.reason') IS NOT NULL OR json_extract(body,'$.allocation_group.selection.stage') IN ('selected','rejected') OR EXISTS(SELECT 1 FROM json_each(body,'$.allocations') a WHERE json_extract(a.value,'$.kind')='vm_disk' AND json_extract(a.value,'$.resolution.state') IN ('unresolved','result_known','cleanup')))`, key, key).
			Scan(&protected); err != nil {
			return err
		}
		if protected != 0 {
			return nil
		}
		var unresolved int
		// A late receipt still owns its frozen physical target even after release
		// clears sandbox metadata. Only its destination transaction can retire it.
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM resource_recovery r, json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.kind')='vm_disk' AND json_extract(a.value,'$.request.vm_id')=? AND json_extract(a.value,'$.resolution.state') IN ('unresolved','result_known')`, key).
			Scan(&unresolved); err != nil {
			return err
		}
		if unresolved > 0 {
			return nil
		}
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs r WHERE r.kind='vm_disk' AND r.holder_kind='engine_dependency' AND (r.resource_key=? OR EXISTS(SELECT 1 FROM quota_refs v WHERE v.tenant_id=r.tenant_id AND v.kind=r.kind AND v.resource_key=r.resource_key AND v.holder_id=?))`, key, "vm:"+key).
			Scan(&unresolved); err != nil {
			return err
		}
		if unresolved > 0 {
			return nil
		}
		if err = deleteQuotaRows(
			s.ctx,
			tx,
			`FROM quota_refs WHERE holder_id=?`,
			"backing:"+key,
		); err != nil {
			return err
		}
	}
	query := `FROM quota_resources WHERE kind=? AND resource_key=?`
	if kind == "vm_disk" {
		query = `FROM quota_resources WHERE kind=? AND (resource_key=? OR EXISTS(SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_id=?))`
	}
	if kind == "snapshot" {
		query += ` AND NOT EXISTS(SELECT 1 FROM quota_refs q WHERE q.tenant_id=quota_resources.tenant_id AND q.kind=quota_resources.kind AND q.resource_key=quota_resources.resource_key AND q.holder_kind IN ('active_use','engine_dependency'))`
	}
	args := []any{kind, key}
	if kind == "vm_disk" {
		args = append(args, "vm:"+key)
	}
	if err = deleteQuotaRows(s.ctx, tx, query, args...); err != nil {
		return err
	}
	if err = retireQuotaSlots(s.ctx, tx); err != nil {
		return err
	}
	return nil
}

// Reclamation requires every selected row to retire. A silent SQLite IGNORE
// must roll back the destination transaction just like an explicit SQL error.
func deleteQuotaRows(
	ctx context.Context,
	e executor,
	selection string,
	args ...any,
) error {
	var expected int64
	if err := e.QueryRowContext(ctx, "SELECT count(*) "+selection, args...).
		Scan(&expected); err != nil {
		return err
	}
	result, err := e.ExecContext(ctx, "DELETE "+selection, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != expected {
		return api.Fail(
			api.ManualRecovery,
			"quota responsibility was not fully retired",
		)
	}
	return nil
}

// A terminated sandbox retains its slot through unknown allocations and
// exclusive sleep cleanup. Independent live users can take over a snapshot;
// cleanup references alone never constitute such a transfer.
func retireQuotaSlots(ctx context.Context, e executor) error {
	return deleteQuotaRows(
		ctx,
		e,
		`FROM quota_resources AS slot WHERE slot.kind='sandbox_slot'
 AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.kind=slot.kind AND r.resource_key=slot.resource_key AND r.holder_kind='cleanup')
 AND NOT EXISTS(SELECT 1 FROM resource_recovery rr WHERE json_extract(rr.body,'$.kind')='allocation_group' AND json_extract(rr.body,'$.allocation_group.subject.kind')='sandbox' AND json_extract(rr.body,'$.allocation_group.subject.id')=slot.resource_key AND COALESCE(json_extract(rr.body,'$.done'),0)=0)
 AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.holder_id='sandbox:'||slot.resource_key AND r.kind IN ('vm_disk','pending_storage'))
 AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=slot.tenant_id AND r.holder_id='sandbox:'||slot.resource_key AND r.kind='snapshot'
 AND NOT EXISTS(SELECT 1 FROM quota_refs live WHERE live.tenant_id=r.tenant_id AND live.kind=r.kind AND live.resource_key=r.resource_key AND live.holder_kind='active_use' AND live.holder_id!=r.holder_id))`,
	)
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
 (SELECT count(*) FROM quota_resources WHERE tenant_id='' OR resource_key='' OR source_operation_id='' OR sandbox_slots<0 OR storage_mib<0 OR kind NOT IN ('sandbox_slot','build_slot','vm_disk','snapshot','pending_storage') OR (kind IN ('sandbox_slot','build_slot') AND (sandbox_slots!=1 OR storage_mib!=0)) OR (kind NOT IN ('sandbox_slot','build_slot') AND sandbox_slots!=0))+
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
	return s.validateAllocationGroups(ctx)
}

func (s *Service) finishQuotaOperation(o Operation) error {
	if group, err := s.findAllocationGroup(o); err != nil {
		return err
	} else if group != nil {
		return nil // The group resolver owns every terminal dependency and destination.
	}
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
		`DELETE FROM quota_resources WHERE kind='sandbox_slot' AND NOT EXISTS(SELECT 1 FROM resource_recovery rr WHERE json_extract(rr.body,'$.kind')='allocation_group' AND json_extract(rr.body,'$.allocation_group.subject.kind')='sandbox' AND json_extract(rr.body,'$.allocation_group.subject.id')=quota_resources.resource_key AND COALESCE(json_extract(rr.body,'$.done'),0)=0) AND EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=quota_resources.tenant_id AND r.kind=quota_resources.kind AND r.resource_key=quota_resources.resource_key AND r.holder_kind='cleanup') AND NOT EXISTS(SELECT 1 FROM quota_refs r WHERE r.tenant_id=quota_resources.tenant_id AND r.holder_id='sandbox:'||quota_resources.resource_key AND r.kind IN ('vm_disk','snapshot','pending_storage'))`,
	)
	return err
}

// Recheck catalog absence after references move. No-ref resources remain
// charged here; a failed query never authorizes retirement.
func (s *Service) reconcileQuota() error {
	rows, err := s.store.db.QueryContext(
		s.ctx,
		`SELECT 'snapshot',resource_key FROM quota_resources q WHERE kind='snapshot' AND NOT EXISTS(SELECT 1 FROM quota_refs f WHERE f.tenant_id=q.tenant_id AND f.kind=q.kind AND f.resource_key=q.resource_key AND f.holder_kind='active_use' AND f.holder_id LIKE 'template:%') UNION SELECT 'vm_disk',substr(holder_id,9) FROM quota_refs WHERE holder_id LIKE 'backing:%' ORDER BY 1 DESC`,
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
			var collectors int
			if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM resource_recovery WHERE json_extract(body,'$.kind')='instance_cleanup' AND json_extract(body,'$.vm_id')=? AND COALESCE(json_extract(body,'$.done'),0)=0`, target.key).
				Scan(&collectors); err != nil {
				return err
			}
			if collectors > 0 {
				continue
			}
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

func (s *Service) admitQuotaRecovery(
	r *resourceRecovery,
	o Operation,
	b Sandbox,
	step, kind string,
	request api.Request,
	storage int64,
) error {
	if o.ChargeTenantID == "" || o.ChargeTenantID != b.TenantID {
		return api.Fail(
			api.ManualRecovery,
			"recovery allocation has no trusted owner",
		)
	}
	request.Key = stepKey(o, step)
	a := recoveryAllocation{
		OperationID:    o.ID,
		TenantID:       o.TenantID,
		ChargeTenantID: o.ChargeTenantID,
		SandboxID:      b.ID,
		Shape:          b.Shape,
		Step:           step,
		Kind:           kind,
		Resolution:     allocationResolution{State: "unresolved"},
		Request:        request,
	}
	index := len(r.Allocations)
	for _, previous := range r.Allocations {
		if previous.OperationID == o.ID && previous.Step == step {
			if !bytes.Equal(encode(previous.Request), encode(request)) ||
				previous.Shape != b.Shape ||
				previous.ChargeTenantID != o.ChargeTenantID {
				return api.Fail(
					api.ManualRecovery,
					"recovery allocation intent changed",
				)
			}
			return nil
		}
	}
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
		storage,
	); err != nil {
		return err
	}
	if err = reserveQuotaStep(s.ctx, tx, o, step, b.ID, storage); err != nil {
		return err
	}
	if request.SnapshotID != "" {
		if err = referenceQuotaSnapshot(
			s.ctx,
			tx,
			b.TenantID,
			request.SnapshotID,
			quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
		); err != nil {
			return err
		}
	}
	next := *r
	next.Allocations = append([]recoveryAllocation(nil), r.Allocations...)
	if index == len(next.Allocations) {
		next.Allocations = append(next.Allocations, a)
	} else {
		next.Allocations[index] = a
	}
	result, err := tx.ExecContext(
		s.ctx,
		"UPDATE resource_recovery SET body=? WHERE id=?",
		encode(next),
		r.ID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return api.Fail(
			api.ManualRecovery,
			"recovery allocation has no durable responsibility",
		)
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*r = next
	return nil
}

// Allocation groups share the ledger with sandbox recovery, but retain their
// real subject and frozen inputs instead of borrowing a sandbox identity.
func allocationGroupID(o Operation) string {
	if o.Origin != "" {
		return "allocation:" + o.Origin
	}
	return "allocation:" + o.ID
}

func loadRecoveryTx(
	ctx context.Context,
	e executor,
	id string,
) (resourceRecovery, error) {
	var raw []byte
	var r resourceRecovery
	err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, id).
		Scan(&raw)
	if err == nil {
		err = json.Unmarshal(raw, &r)
	}
	if err == nil {
		err = validateWarmSelection(ctx, e, r)
	}
	return r, err
}

func insertRecovery(ctx context.Context, e executor, r resourceRecovery) error {
	if _, err := e.ExecContext(
		ctx,
		`INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING`,
		r.ID,
		encode(r),
	); err != nil {
		return err
	}
	var actual []byte
	if err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE id=?`, r.ID).
		Scan(&actual); err != nil {
		return err
	}
	if !bytes.Equal(actual, encode(r)) {
		return api.Fail(
			api.ManualRecovery,
			"recovery identity conflicts with durable responsibility",
		)
	}
	return nil
}

func (s *Service) findAllocationGroup(o Operation) (*resourceRecovery, error) {
	r, err := loadRecoveryTx(s.ctx, s.store.db, allocationGroupID(o))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if r.Kind != "allocation_group" || r.Group == nil {
		return nil, api.Fail(
			api.ManualRecovery,
			"allocation group is malformed",
		)
	}
	return &r, nil
}

func newAllocationGroup(
	o Operation,
	subject allocationSubject,
	t Template,
) resourceRecovery {
	return resourceRecovery{
		ID: allocationGroupID(
			o,
		),
		Source:    o.ID,
		TenantID:  o.ChargeTenantID,
		Kind:      "allocation_group",
		SandboxID: o.Command.SandboxID,
		Group: &allocationGroup{
			Subject:  subject,
			Template: t,
			Steps:    map[string]allocationStep{},
		},
	}
}

func validateGroupSource(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	var raw []byte
	if err := e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return err
	}
	var source Operation
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	if r.Group == nil || source.ID != r.Source ||
		source.ChargeTenantID != r.TenantID ||
		source.Command.Template == nil ||
		!bytes.Equal(
			encode(*source.Command.Template),
			encode(r.Group.Template),
		) {
		return api.Fail(
			api.ManualRecovery,
			"allocation group differs from original public intent",
		)
	}
	if r.Group.Subject.Kind == "sandbox" {
		found := false
		for _, id := range source.IDs {
			if id == r.Group.Subject.ID {
				found = true
			}
		}
		if source.Command.Action != "claim" || !found {
			return api.Fail(
				api.ManualRecovery,
				"allocation subject is not its original sandbox",
			)
		}
	}
	if source.Command.Action == "template_put" &&
		r.Group.Template.SnapshotID != "" {
		q := r.Group.SnapshotSource
		if q == nil || q.ID == "" || q.Owner != r.TenantID ||
			q.SnapshotID != r.Group.Template.SnapshotID ||
			q.Shape != r.Group.Template.Shape ||
			q.SourceOperation == "" ||
			q.SourceKind == "" ||
			(q.Kind != "checkpoint" && q.Kind != "sleep" && q.Kind != "group" && q.Kind != "external") ||
			(q.Config != nil && q.Config.Shape != q.Shape) ||
			(q.Kind == "external" && (q.Config == nil || q.ID != q.SnapshotID)) {
			return api.Fail(
				api.ManualRecovery,
				"template quote lost its frozen source",
			)
		}
		var owners int
		if err := e.QueryRowContext(ctx, `SELECT count(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id=? AND source_operation_id=? AND source_kind=?`, q.SnapshotID, q.Owner, q.SourceOperation, q.SourceKind).
			Scan(&owners); err != nil {
			return err
		}
		if owners != 1 {
			return api.Fail(
				api.ManualRecovery,
				"template source provenance changed",
			)
		}
	}
	return nil
}

func groupHolder(
	r resourceRecovery,
) string {
	return r.Group.Subject.Kind + ":" + r.Group.Subject.ID
}

func addGroupAllocation(
	ctx context.Context,
	tx *sql.Tx,
	r *resourceRecovery,
	o Operation,
	step, kind string,
	storage int64,
	request api.Request,
	bound bool,
) error {
	request.Key = stepKey(o, step)
	a := recoveryAllocation{
		OperationID:    o.ID,
		TenantID:       o.TenantID,
		ChargeTenantID: o.ChargeTenantID,
		Shape:          r.Group.Template.Shape,
		Subject:        r.Group.Subject,
		Step:           step,
		Kind:           kind,
		Request:        request,
		RequestBound:   bound,
		Resolution:     allocationResolution{State: "unresolved"},
	}
	if r.Group.Subject.Kind == "sandbox" {
		a.SandboxID = r.Group.Subject.ID
	}
	if err := putQuotaResource(
		ctx,
		tx,
		quotaResource{
			Tenant:  o.ChargeTenantID,
			Kind:    "pending_storage",
			Key:     request.Key,
			Storage: storage,
			Source:  o.ID,
		},
		quotaRef{Kind: "engine_dependency", ID: groupHolder(*r)},
	); err != nil {
		return err
	}
	r.Allocations = append(r.Allocations, a)
	r.Group.Steps[step] = allocationStep{Request: request, Bound: bound}
	return nil
}

func (s *Service) admitBuildTx(
	ctx context.Context,
	tx *sql.Tx,
	o Operation,
	t Template,
) error {
	r := newAllocationGroup(o, allocationSubject{Kind: "build", ID: o.ID}, t)
	if err := putQuotaResource(
		ctx,
		tx,
		quotaResource{
			Tenant: o.ChargeTenantID,
			Kind:   "build_slot",
			Key:    o.ID,
			Slots:  1,
			Source: o.ID,
		},
		quotaRef{Kind: "active_use", ID: groupHolder(r)},
	); err != nil {
		return err
	}
	request := api.Request{
		Action: "create",
		Config: &api.Config{
			Image: t.Image,
			Shape: t.Shape,
			Labels: map[string]string{
				"sandboxd":     "build",
				"operation_id": o.ID,
			},
		},
	}
	if err := addGroupAllocation(
		ctx,
		tx,
		&r,
		o,
		"build-create",
		"vm_disk",
		t.Shape.DiskMiB,
		request,
		true,
	); err != nil {
		return err
	}
	if err := addGroupAllocation(
		ctx,
		tx,
		&r,
		o,
		"build-snapshot",
		"snapshot",
		t.Shape.DiskMiB+t.Shape.MemoryMiB,
		api.Request{Action: "snapshot"},
		false,
	); err != nil {
		return err
	}
	return insertRecovery(ctx, tx, r)
}

func groupAllocation(r *resourceRecovery, step string) *recoveryAllocation {
	for i := range r.Allocations {
		if r.Allocations[i].Step == step {
			return &r.Allocations[i]
		}
	}
	return nil
}

func (s *Service) allocationEngine(
	o Operation,
	step string,
	request api.Request,
) (api.Operation, error) {
	request.Key = stepKey(o, step)
	if o.Origin != "" {
		step = request.Key
	}
	ownership := s.lockResourceOwnership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		ownership()
		return api.Operation{}, err
	}
	r, err := loadRecoveryTx(s.ctx, tx, allocationGroupID(o))
	if err == nil {
		var raw []byte
		err = tx.QueryRowContext(s.ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
			Scan(&raw)
		var current Operation
		if err == nil {
			err = json.Unmarshal(raw, &current)
		}
		if err == nil && recoveryTerminal(current.Status) {
			err = api.Fail(
				api.ManualRecovery,
				"terminal allocation intent cannot dispatch lifecycle work",
			)
		}
	}
	if err == nil && (r.Group == nil || r.Group.Gate != nil) {
		err = api.Fail(api.ManualRecovery, "allocation lifecycle is gated")
	}
	if err == nil && request.Action == "delete" {
		err = api.Fail(
			api.ManualRecovery,
			"allocation deletion requires a qualified durable destination",
		)
	}
	if err == nil &&
		(request.Action == "create" || request.Action == "clone" || request.Action == "snapshot" || request.Action == "restore") {
		a := groupAllocation(&r, step)
		if a == nil {
			err = api.Fail(
				api.ManualRecovery,
				"allocation step has no reservation descriptor",
			)
		} else {
			var pending int
			err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, r.TenantID, a.Request.Key).
				Scan(&pending)
			if err == nil && pending != 1 && a.Resolution.Proof == nil {
				err = api.Fail(
					api.ManualRecovery,
					"allocation step has no durable pending responsibility",
				)
			}
		}
	}
	var frozen allocationStep
	if err == nil {
		var exists bool
		frozen, exists = r.Group.Steps[step]
		if exists && frozen.Bound &&
			!bytes.Equal(encode(frozen.Request), encode(request)) {
			err = api.Fail(api.ManualRecovery, "allocation request changed")
		}
		if err == nil && (!exists || !frozen.Bound) {
			if request.Action == "snapshot" || request.Action == "start" {
				identity := r.Group.Identity
				if identity == nil || identity.VM != request.VMID {
					err = api.Fail(
						api.ManualRecovery,
						"step has no frozen VM identity",
					)
				}
				if err == nil && request.Action == "snapshot" &&
					(identity.Instance == "" || request.ExpectedInstance != identity.Instance) {
					err = api.Fail(
						api.ManualRecovery,
						"capture has no successful generation witness",
					)
				}
				if err == nil && request.Action == "snapshot" {
					var disks int
					err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND storage_mib=?`, r.TenantID, identity.VM, identity.Shape.DiskMiB).
						Scan(&disks)
					if err == nil && disks != 1 {
						err = api.Fail(
							api.ManualRecovery,
							"capture lost its source disk responsibility",
						)
					}
				}
			}
			frozen = allocationStep{Request: request, Bound: true}
			if a := groupAllocation(&r, step); a != nil {
				a.Request = request
				a.RequestBound = true
			}
		}
		if err == nil {
			frozen.Dispatched = true
			r.Group.Steps[step] = frozen
			err = writeRecovery(s.ctx, tx, r)
		}
	}
	if err == nil {
		err = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	ownership()
	if err != nil {
		return api.Operation{}, err
	}
	proof, engineErr := api.Wait(s.ctx, s.opts.Engine, request)
	if proof.Key == "" {
		return proof, engineErr
	}
	if err = s.adoptGroupReceipt(r.ID, step, proof); err != nil {
		return proof, errors.Join(engineErr, err)
	}
	return proof, engineErr
}

func (s *Service) adoptGroupReceipt(
	id, step string,
	proof api.Operation,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := loadRecoveryTx(s.ctx, tx, id)
	if err != nil {
		return err
	}
	if err = validateGroupSource(s.ctx, tx, r); err != nil {
		return err
	}
	frozen, ok := r.Group.Steps[step]
	if !ok || !frozen.Bound || proof.ID == "" ||
		proof.Key != frozen.Request.Key ||
		!bytes.Equal(encode(proof.Request), encode(frozen.Request)) {
		return api.Fail(
			api.ManualRecovery,
			"receipt differs from frozen group request",
		)
	}
	if frozen.Proof != nil && recoveryTerminal(frozen.Proof.Status) &&
		!bytes.Equal(encode(*frozen.Proof), encode(proof)) {
		return api.Fail(
			api.ManualRecovery,
			"terminal allocation receipt changed",
		)
	}
	frozen.Proof = &proof
	r.Group.Steps[step] = frozen
	a := groupAllocation(&r, step)
	if a != nil && !a.Resolution.delivered() {
		key := proof.VMID
		if a.Kind == "snapshot" {
			key = proof.SnapshotID
		}
		var pending int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`, r.TenantID, a.Request.Key).
			Scan(&pending); err != nil {
			return err
		}
		if key != "" && pending == 1 {
			cost := a.Shape.DiskMiB
			if a.Kind == "snapshot" {
				cost += a.Shape.MemoryMiB
			}
			if err = putQuotaResource(
				s.ctx,
				tx,
				quotaResource{
					Tenant:  r.TenantID,
					Kind:    a.Kind,
					Key:     key,
					Storage: cost,
					Source:  a.OperationID,
				},
				quotaRef{Kind: "cleanup", ID: groupHolder(r)},
			); err != nil {
				return err
			}
			if a.Kind == "vm_disk" {
				if err = putQuotaRef(
					s.ctx,
					tx,
					r.TenantID,
					a.Kind,
					key,
					quotaRef{Kind: "cleanup", ID: "vm:" + key},
				); err != nil {
					return err
				}
				if a.Request.SnapshotID != "" {
					if err = putQuotaRef(
						s.ctx,
						tx,
						r.TenantID,
						"snapshot",
						a.Request.SnapshotID,
						quotaRef{Kind: "active_use", ID: "backing:" + key},
					); err != nil {
						return err
					}
				}
			} else {
				if err = recordSnapshotOwner(
					s.ctx,
					tx,
					key,
					r.TenantID,
					a.OperationID,
					"captured",
				); err != nil {
					return err
				}
				if err = putQuotaRef(
					s.ctx,
					tx,
					r.TenantID,
					"snapshot",
					key,
					quotaRef{
						Kind: "engine_dependency",
						ID:   "operation:" + r.Source,
					},
				); err != nil {
					return err
				}
			}
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				r.TenantID,
				a.Request.Key,
			); err != nil {
				return err
			}
		}
		if key == "" && recoveryTerminal(proof.Status) {
			if err = deleteQuotaRows(
				s.ctx,
				tx,
				`FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				r.TenantID,
				a.Request.Key,
			); err != nil {
				return err
			}
			a.Resolution = allocationResolution{
				State:       "reclaimed",
				Proof:       &proof,
				Destination: "terminal-no-allocation:" + proof.ID,
			}
		} else if recoveryTerminal(proof.Status) {
			a.Resolution = allocationResolution{
				State: "result_known",
				Proof: &proof,
			}
		}
		if key != "" && a.Kind == "vm_disk" {
			if r.Group.Identity == nil {
				r.Group.Identity = &allocationIdentity{
					VM:      key,
					Owner:   r.TenantID,
					Shape:   a.Shape,
					Source:  a.OperationID,
					Receipt: proof,
				}
			}
			if r.Group.Identity.VM != key {
				return api.Fail(
					api.ManualRecovery,
					"allocation returned another VM",
				)
			}
			if recoveryTerminal(proof.Status) {
				r.Group.Identity.Receipt = proof
			}
			if proof.Status == "succeeded" && proof.Request.Action == "clone" &&
				proof.ResultInstance != "" {
				r.Group.Identity.Instance = proof.ResultInstance
				r.Group.Identity.Generation = &proof
			}
		}
	}
	if proof.Request.Action == "start" && proof.Status == "succeeded" &&
		proof.ResultInstance != "" {
		identity := r.Group.Identity
		if identity == nil || identity.VM != proof.VMID ||
			proof.Request.VMID != identity.VM {
			return api.Fail(api.ManualRecovery, "start returned another VM")
		}
		identity.Instance = proof.ResultInstance
		identity.Generation = &proof
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

// Full shape is obtained from a durable capture or import, before admission's
// write lock. Authorization is checked before reporting missing provenance.
func groupSnapshotSource(r resourceRecovery, sid string) bool {
	if r.Group == nil {
		return false
	}
	if r.Group.Template.SnapshotID == sid {
		return true
	}
	for _, a := range r.Allocations {
		if a.Kind == "snapshot" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.SnapshotID == sid {
			return true
		}
	}
	return false
}

func (s *Service) templateSource(
	ctx context.Context,
	p Principal,
	t Template,
) (*templateSnapshotSource, error) {
	ownerErr := requireSnapshotOwner(ctx, s.store.db, t.SnapshotID, p.TenantID)
	if ownerErr != nil && (!p.Root || !isCode(ownerErr, api.NotFound)) {
		return nil, ownerErr
	}
	var foreign int
	if err := s.store.db.QueryRowContext(ctx, `SELECT count(*) FROM snapshot_owners WHERE snapshot_id=? AND tenant_id!=?`, t.SnapshotID, p.TenantID).
		Scan(&foreign); err != nil {
		return nil, err
	}
	if foreign != 0 {
		return nil, api.Fail(api.NotFound, "not found")
	}
	quote := &templateSnapshotSource{
		SnapshotID: t.SnapshotID,
		Owner:      p.TenantID,
		Shape:      t.Shape,
	}
	var raw []byte
	err := s.store.db.QueryRowContext(ctx, `SELECT 'checkpoint',id,body FROM checkpoints WHERE tenant=? AND json_extract(body,'$.snapshot_id')=? UNION ALL SELECT 'sleep',id,body FROM sandboxes WHERE tenant=? AND json_extract(body,'$.snapshot_id')=? LIMIT 1`, p.TenantID, t.SnapshotID, p.TenantID, t.SnapshotID).
		Scan(&quote.Kind, &quote.ID, &raw)
	if err == nil {
		var source struct {
			Shape api.Shape `json:"shape"`
		}
		if err = json.Unmarshal(raw, &source); err != nil {
			return nil, err
		}
		if source.Shape != t.Shape {
			return nil, api.Fail(
				api.Conflict,
				"template shape differs from trusted snapshot",
			)
		}
		return quote, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	groups, err := s.resourceRecoveries()
	if err != nil {
		return nil, err
	}
	for _, r := range groups {
		if r.TenantID != p.TenantID || !groupSnapshotSource(r, t.SnapshotID) {
			continue
		}
		if r.Group.Template.Shape != t.Shape {
			return nil, api.Fail(
				api.Conflict,
				"template shape differs from frozen capture",
			)
		}
		quote.Kind = "group"
		quote.ID = r.ID
		if r.Group.SnapshotSource != nil &&
			r.Group.SnapshotSource.Config != nil {
			config := *r.Group.SnapshotSource.Config
			config.Labels = maps.Clone(config.Labels)
			quote.Config = &config
		}
		if quote.Config == nil && r.Group.Identity != nil &&
			r.Group.Identity.Receipt.Request.Config != nil {
			config := *r.Group.Identity.Receipt.Request.Config
			config.Labels = maps.Clone(config.Labels)
			quote.Config = &config
		}
		return quote, nil
	}
	if !p.Root {
		return nil, api.Fail(
			api.ManualRecovery,
			"snapshot has no trusted full shape",
		)
	}
	snap, err := s.opts.Engine.GetSnapshot(ctx, t.SnapshotID)
	if err != nil {
		return nil, err
	}
	if snap.ID != t.SnapshotID || !snap.Published || snap.Deleting ||
		snap.Config.Shape != t.Shape {
		return nil, api.Fail(
			api.Incompatible,
			"import snapshot shape or publication differs",
		)
	}
	quote.Kind = "external"
	quote.ID = snap.ID
	config := snap.Config
	config.Labels = maps.Clone(config.Labels)
	quote.Config = &config
	return quote, nil
}

func recheckTemplateSource(
	ctx context.Context,
	q executor,
	source *templateSnapshotSource,
	t Template,
	owner string,
) error {
	if source == nil || source.Owner != owner ||
		source.SnapshotID != t.SnapshotID ||
		source.Shape != t.Shape {
		return api.Fail(
			api.ManualRecovery,
			"template source quote differs from admission",
		)
	}
	if source.Config != nil && source.Config.Shape != t.Shape {
		return api.Fail(
			api.ManualRecovery,
			"template source Config differs from its shape",
		)
	}
	var raw []byte
	var err error
	switch source.Kind {
	case "checkpoint":
		err = q.QueryRowContext(ctx, `SELECT body FROM checkpoints WHERE id=? AND tenant=?`, source.ID, owner).
			Scan(&raw)
	case "sleep":
		err = q.QueryRowContext(ctx, `SELECT body FROM sandboxes WHERE id=? AND tenant=?`, source.ID, owner).
			Scan(&raw)
	case "group":
		var r resourceRecovery
		r, err = loadRecoveryTx(ctx, q, source.ID)
		if err == nil &&
			(r.TenantID != owner || !groupSnapshotSource(r, t.SnapshotID) || r.Group.Template.Shape != t.Shape) {
			err = api.Fail(api.Conflict, "frozen snapshot source changed")
		}
		if err == nil {
			err = validateGroupSource(ctx, q, r)
		}
		return err
	case "external":
		if source.ID != t.SnapshotID || source.Config == nil {
			return api.Fail(
				api.ManualRecovery,
				"external snapshot quote is incomplete",
			)
		}
		return nil
	default:
		return api.Fail(api.ManualRecovery, "unknown template source")
	}
	if errors.Is(err, sql.ErrNoRows) {
		return api.Fail(
			api.Conflict,
			"template source retired during admission",
		)
	}
	if err != nil {
		return err
	}
	var current struct {
		TenantID   string    `json:"tenant_id"`
		SnapshotID string    `json:"snapshot_id"`
		Shape      api.Shape `json:"shape"`
	}
	if err = json.Unmarshal(raw, &current); err != nil {
		return err
	}
	if current.TenantID != owner || current.SnapshotID != source.SnapshotID ||
		current.Shape != source.Shape {
		return api.Fail(
			api.Conflict,
			"template source changed during admission",
		)
	}
	return nil
}

func (s *store) validateAllocationGroups(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind') IN ('allocation_group','allocation_cleanup')`,
	)
	if err != nil {
		return err
	}
	var groups []resourceRecovery
	for rows.Next() {
		var raw []byte
		var r resourceRecovery
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &r)
		}
		if err != nil {
			_ = rows.Close()
			return err
		}
		groups = append(groups, r)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, r := range groups {
		if r.TenantID == "" || r.Source == "" {
			return api.Fail(api.ManualRecovery, "group has no owner or source")
		}
		if r.Kind == "allocation_cleanup" {
			if r.Deletion == nil || r.Deletion.Identity.Instance == "" ||
				r.ID != allocationCollectorID(
					r.TenantID,
					r.Deletion.Subject,
					r.Deletion.Identity.VM,
					r.Deletion.Identity.Instance,
				) {
				return api.Fail(
					api.ManualRecovery,
					"cleanup identity is invalid",
				)
			}
			parent, e := loadRecoveryTx(ctx, s.db, r.Deletion.Group)
			if e != nil {
				return e
			}
			if e = validateAllocationDeletion(r, parent); e != nil {
				return e
			}
			continue
		}
		g := r.Group
		if g == nil || r.ID != "allocation:"+r.Source || g.Subject.ID == "" ||
			(g.Subject.Kind != "build" && g.Subject.Kind != "warm" && g.Subject.Kind != "sandbox") ||
			g.Template.Shape.VCPUs < 1 ||
			g.Template.Shape.DiskMiB < 64 ||
			g.Template.Shape.MemoryMiB < 128 {
			return api.Fail(
				api.ManualRecovery,
				"allocation subject or shape is invalid",
			)
		}
		if g.Gate != nil &&
			(g.Gate.Reason != "generation_witness_missing" || g.Gate.Mode != "observe_absence_only" || r.Done) {
			return api.Fail(
				api.ManualRecovery,
				"invalid allocation observation gate",
			)
		}
		for _, step := range g.Steps {
			if step.Dispatched && !step.Bound {
				return api.Fail(
					api.ManualRecovery,
					"unbound allocation step was dispatched",
				)
			}
			if step.Proof != nil &&
				(!step.Dispatched || !step.Bound || step.Proof.ID == "" || step.Proof.Key != step.Request.Key || !bytes.Equal(encode(step.Proof.Request), encode(step.Request))) {
				return api.Fail(
					api.ManualRecovery,
					"step receipt differs from frozen request",
				)
			}
		}
		if err = validateGroupSource(ctx, s.db, r); err != nil {
			return err
		}
		if err = validateWarmSelection(ctx, s.db, r); err != nil {
			return err
		}
		if g.Identity != nil {
			if err = validateAllocationProvenance(r); err != nil {
				return err
			}
		}
		if g.Subject.Kind == "build" && len(r.Allocations) > 0 &&
			(!r.Done || hasUnresolvedAllocations(r)) {
			if err = requireQuotaResource(ctx, s.db, quotaResource{
				Tenant: r.TenantID, Kind: "build_slot", Key: r.Source,
				Slots: 1, Source: r.Source,
			}, quotaRef{Kind: "active_use", ID: groupHolder(r)}); err != nil {
				return err
			}
		}
		for _, a := range r.Allocations {
			if a.Subject != g.Subject || a.Shape != g.Template.Shape ||
				a.ChargeTenantID != r.TenantID ||
				a.OperationID != r.Source ||
				a.Request.Key != stepKey(a.operation(), a.Step) {
				return api.Fail(
					api.ManualRecovery,
					"allocation provenance conflicts",
				)
			}
			if !a.Resolution.delivered() &&
				a.Resolution.State != "unresolved" &&
				a.Resolution.State != "result_known" {
				return api.Fail(
					api.ManualRecovery,
					"invalid allocation resolution",
				)
			}
			if a.Resolution.Proof != nil {
				if err = a.validate(*a.Resolution.Proof); err != nil {
					return err
				}
			}
			if a.Kind == "vm_disk" && a.Request.SnapshotID != "" &&
				!a.Resolution.delivered() {
				var dependency int
				if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, r.TenantID, a.Request.SnapshotID, "operation:"+r.Source).
					Scan(&dependency); err != nil {
					return err
				}
				if dependency != 1 {
					return api.Fail(
						api.ManualRecovery,
						"allocation lost its source snapshot dependency",
					)
				}
			}
			if a.Kind == "snapshot" {
				step := g.Steps[a.Step]
				if step.Proof != nil && step.Proof.SnapshotID != "" {
					var liveParent int
					if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM operations WHERE id=? AND status IN ('pending','running')`, r.Source).
						Scan(&liveParent); err != nil {
						return err
					}
					if !a.Resolution.delivered() ||
						(a.Resolution.State == "cleanup" && liveParent != 0) {
						var dependency int
						if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, r.TenantID, step.Proof.SnapshotID, "operation:"+r.Source).
							Scan(&dependency); err != nil {
							return err
						}
						if dependency != 1 {
							return api.Fail(
								api.ManualRecovery,
								"capture lost its publication reader",
							)
						}
					}
				}
			}
			if !a.Resolution.delivered() {
				step, ok := g.Steps[a.Step]
				if !ok || step.Bound != a.RequestBound ||
					!bytes.Equal(encode(step.Request), encode(a.Request)) {
					return api.Fail(
						api.ManualRecovery,
						"allocation step binding conflicts",
					)
				}
				kind, key := a.Kind, ""
				proof := step.Proof
				if proof != nil {
					key = proof.VMID
					if kind == "snapshot" {
						key = proof.SnapshotID
					}
				}
				if key == "" {
					kind = "pending_storage"
					key = a.Request.Key
				}
				cost := a.Shape.DiskMiB
				if a.Kind == "snapshot" {
					cost += a.Shape.MemoryMiB
				}
				ref, e := groupAllocationRef(
					ctx,
					s.db,
					r,
					kind == "pending_storage",
				)
				if e != nil {
					return e
				}
				if err = requireQuotaResource(ctx, s.db, quotaResource{
					Tenant: r.TenantID, Kind: kind, Key: key,
					Storage: cost, Source: a.OperationID,
				}, ref); err != nil {
					return err
				}
			}
		}
	}
	if err = validateReadyWarmRows(ctx, s.db); err != nil {
		return err
	}
	var catalogBad int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM templates t WHERE COALESCE(json_extract(t.body,'$.snapshot_id'),'')!='' AND (NOT EXISTS(SELECT 1 FROM snapshot_owners o WHERE o.snapshot_id=json_extract(t.body,'$.snapshot_id') AND o.tenant_id=t.tenant) OR NOT EXISTS(SELECT 1 FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=t.tenant AND q.kind='snapshot' AND q.resource_key=json_extract(t.body,'$.snapshot_id') AND q.storage_mib=json_extract(t.body,'$.shape.disk_mib')+json_extract(t.body,'$.shape.memory_mib') AND f.holder_kind='active_use' AND f.holder_id='template:'||t.id))`).
		Scan(&catalogBad); err != nil {
		return err
	}
	if catalogBad != 0 {
		return api.Fail(
			api.ManualRecovery,
			"template catalog lost its snapshot cost, owner or holder",
		)
	}
	return nil
}

// Ready rows retain the version originally published by their refill. A
// rolling template replacement does not rewrite that frozen owner or shape.
func validateReadyWarmRows(ctx context.Context, e *sql.DB) error {
	rows, err := e.QueryContext(
		ctx,
		`SELECT vm_id,template_id,snapshot_id FROM warm`,
	)
	if err != nil {
		return err
	}
	type ready struct{ vm, template, snapshot string }
	var inventory []ready
	for rows.Next() {
		var item ready
		if err = rows.Scan(
			&item.vm,
			&item.template,
			&item.snapshot,
		); err != nil {
			_ = rows.Close()
			return err
		}
		inventory = append(inventory, item)
	}
	if err = errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, item := range inventory {
		if err = validateReadyWarmRow(
			ctx,
			e,
			item.vm,
			item.template,
			item.snapshot,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateReadyWarmRow(
	ctx context.Context,
	e executor,
	vm, template, snapshot string,
) error {
	bad := func() error { return api.Fail(api.ManualRecovery, "ready warm lost its frozen cost, source or holder") }
	var count int
	var raw []byte
	if err := e.QueryRowContext(ctx, `SELECT count(*),body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='warm' AND json_extract(body,'$.allocation_group.identity.vm')=?`, vm).
		Scan(&count, &raw); err != nil {
		return err
	}
	var r resourceRecovery
	if count != 1 || json.Unmarshal(raw, &r) != nil || r.Group == nil ||
		r.Group.Identity == nil {
		return bad()
	}
	g, i := r.Group, r.Group.Identity
	if r.ID != "allocation:"+r.Source || r.TenantID == "" ||
		g.Subject != (allocationSubject{Kind: "warm", ID: r.Source}) || g.Gate != nil ||
		g.Selection != nil || g.Template.ID != template || g.Template.SnapshotID != snapshot ||
		g.Template.TenantID != r.TenantID || i.VM != vm || i.Owner != r.TenantID ||
		i.Shape != g.Template.Shape || i.Instance == "" || i.Generation == nil ||
		i.Receipt.Status != "succeeded" || i.Generation.Status != "succeeded" ||
		i.Generation.ResultInstance != i.Instance || len(r.Allocations) != 1 {
		return bad()
	}
	if err := validateGroupSource(ctx, e, r); err != nil {
		return errors.Join(bad(), err)
	}
	if err := validateAllocationProvenance(r); err != nil {
		return err
	}
	if err := e.QueryRowContext(ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return errors.Join(bad(), err)
	}
	var refill Operation
	if json.Unmarshal(raw, &refill) != nil || refill.ID != r.Source ||
		refill.Command.Action != "refill" ||
		refill.Command.TemplateID != template ||
		refill.ChargeTenantID != r.TenantID {
		return bad()
	}
	a := r.Allocations[0]
	step, ok := g.Steps[a.Step]
	if !ok || !step.Bound || !step.Dispatched || step.Proof == nil ||
		!reflect.DeepEqual(
			*step.Proof,
			i.Receipt,
		) || !reflect.DeepEqual(a.Request, i.Receipt.Request) ||
		a.OperationID != r.Source || a.TenantID != refill.TenantID || a.ChargeTenantID != r.TenantID || a.Shape != i.Shape ||
		a.Subject != g.Subject || a.Kind != "vm_disk" || !a.RequestBound ||
		a.Request.Key != stepKey(a.operation(), a.Step) ||
		a.Resolution.State != "active" || a.Resolution.Destination != "warm:"+vm ||
		a.Resolution.Proof == nil || !reflect.DeepEqual(*a.Resolution.Proof, i.Receipt) {
		return bad()
	}
	if snapshot != "" &&
		(!reflect.DeepEqual(a.Request, api.Request{Key: a.Request.Key, Action: "clone", SnapshotID: snapshot}) || !reflect.DeepEqual(i.Receipt, *i.Generation)) {
		return bad()
	}
	if err := e.QueryRowContext(ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.resource_key=? AND q.sandbox_slots=0 AND q.storage_mib=? AND q.source_operation_id=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, vm, i.Shape.DiskMiB, r.Source, "warm:"+vm).
		Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return bad()
	}
	if snapshot != "" {
		if err := e.QueryRowContext(ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) JOIN snapshot_owners o ON o.snapshot_id=q.resource_key AND o.tenant_id=q.tenant_id WHERE q.tenant_id=? AND q.kind='snapshot' AND q.resource_key=? AND q.storage_mib=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, snapshot, i.Shape.DiskMiB+i.Shape.MemoryMiB, "backing:"+vm).
			Scan(&count); err != nil {
			return err
		}
		if count != 1 {
			return bad()
		}
	}
	return nil
}

func (s *Service) selectWarmTx(
	ctx context.Context,
	tx *sql.Tx,
	t Template,
) (*resourceRecovery, error) {
	var vm string
	err := tx.QueryRowContext(ctx, `SELECT vm_id FROM warm WHERE template_id=? AND snapshot_id=? ORDER BY created_at,vm_id LIMIT 1`, t.ID, t.SnapshotID).
		Scan(&vm)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = validateReadyWarmRow(ctx, tx, vm, t.ID, t.SnapshotID); err != nil {
		return nil, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='allocation_group' AND json_extract(body,'$.allocation_group.subject.kind')='warm' AND json_extract(body,'$.allocation_group.identity.vm')=?`, vm).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm VM has no durable provenance",
		)
	}
	if err != nil {
		return nil, err
	}
	var r resourceRecovery
	if err = json.Unmarshal(raw, &r); err != nil {
		return nil, err
	}
	g := r.Group
	if g == nil || g.Identity == nil || g.Gate != nil ||
		r.TenantID != t.TenantID ||
		g.Template.ID != t.ID ||
		g.Template.SnapshotID != t.SnapshotID ||
		g.Template.Shape != t.Shape ||
		g.Identity.Instance == "" ||
		g.Identity.Generation == nil ||
		g.Identity.Generation.Status != "succeeded" ||
		g.Identity.Generation.ResultInstance != g.Identity.Instance ||
		g.Identity.Generation.VMID != vm ||
		len(r.Allocations) != 1 ||
		r.Allocations[0].Resolution.State != "active" ||
		r.Allocations[0].Resolution.Destination != "warm:"+vm {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm provenance does not match template",
		)
	}
	var cost int64
	if err = tx.QueryRowContext(ctx, `SELECT storage_mib FROM quota_resources WHERE tenant_id=? AND kind='vm_disk' AND resource_key=?`, t.TenantID, vm).
		Scan(&cost); err != nil {
		return nil, err
	}
	if cost != t.Shape.DiskMiB {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm disk cost mismatch",
		)
	}
	var refs int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, t.TenantID, vm, "warm:"+vm).
		Scan(&refs); err != nil {
		return nil, err
	}
	if refs != 1 {
		return nil, api.Fail(
			api.ManualRecovery,
			"ready warm disk holder is missing",
		)
	}
	if t.SnapshotID != "" {
		if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, t.TenantID, t.SnapshotID, "backing:"+vm).
			Scan(&refs); err != nil {
			return nil, err
		}
		if refs != 1 {
			return nil, api.Fail(
				api.ManualRecovery,
				"ready warm backing holder is missing",
			)
		}
	}
	return &r, nil
}

func (s *Service) claimWarmTx(
	ctx context.Context,
	tx *sql.Tx,
	o Operation,
	b *Sandbox,
	t Template,
	refill resourceRecovery,
) error {
	i := *refill.Group.Identity
	result, err := tx.ExecContext(
		ctx,
		`DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=?`,
		i.VM,
		t.ID,
		t.SnapshotID,
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
		t.TenantID,
		i.VM,
		"warm:"+i.VM,
	); err != nil {
		return err
	}
	if err = putQuotaRef(
		ctx,
		tx,
		t.TenantID,
		"vm_disk",
		i.VM,
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	); err != nil {
		return err
	}
	refill.Allocations[0].Resolution.Destination = "sandbox:" + b.ID
	if err = writeRecovery(ctx, tx, refill); err != nil {
		return err
	}
	r := newAllocationGroup(o, allocationSubject{Kind: "sandbox", ID: b.ID}, t)
	r.SandboxID = b.ID
	r.Group.Selection = &warmSelection{
		Refill:   refill.ID,
		Identity: i,
		Stage:    "selected",
	}
	b.VMID = i.VM
	b.Instance = i.Instance
	b.ProvisionSource = "warm"
	return insertRecovery(ctx, tx, r)
}

func (s *Service) commitSelectionActiveTx(
	tx *sql.Tx,
	o Operation,
	b Sandbox,
	target *activationTarget,
) error {
	r, err := loadRecoveryTx(s.ctx, tx, "allocation:"+target.Origin)
	if err != nil {
		return err
	}
	selection := r.Group.Selection
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if selection == nil || selection.Stage != "selected" ||
		current.State != "provisioning" ||
		current.OperationID != target.Origin ||
		current.VMID != target.VM ||
		current.Instance != target.Instance ||
		current.TenantID != r.TenantID ||
		selection.Identity.VM != target.VM ||
		selection.Identity.Instance != target.Instance {
		return api.Fail(api.Conflict, "warm selection publication changed")
	}
	var holders int
	identity := selection.Identity
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs f USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.resource_key=? AND q.storage_mib=? AND q.source_operation_id=? AND f.holder_kind='active_use' AND f.holder_id=?`, r.TenantID, identity.VM, identity.Shape.DiskMiB, identity.Source, "sandbox:"+b.ID).
		Scan(&holders); err != nil {
		return err
	}
	if holders != 1 {
		return api.Fail(
			api.ManualRecovery,
			"selection active destination lost its disk holder",
		)
	}
	if r.Group.Template.SnapshotID != "" {
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`, r.TenantID, r.Group.Template.SnapshotID, "backing:"+identity.VM).
			Scan(&holders); err != nil {
			return err
		}
		if holders != 1 {
			return api.Fail(
				api.ManualRecovery,
				"selection active destination lost its backing",
			)
		}
	}
	selection.Stage = "active"
	r.Done = true
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return retireSelectionReaderTx(s.ctx, tx, r)
}

// The active selection is a historical transfer to the real sandbox. Later
// pause/release operations may change its VM, without reviving the old reader.
func retireSelectionReaderTx(
	ctx context.Context,
	e executor,
	r resourceRecovery,
) error {
	if err := validateWarmSelection(ctx, e, r); err != nil {
		return err
	}
	if r.Group == nil || r.Group.Selection == nil ||
		r.Group.Selection.Stage != "active" ||
		!r.Done ||
		!groupStepsTerminal(r) ||
		hasUnresolvedAllocations(r) {
		return api.Fail(
			api.ManualRecovery,
			"selection has no completed active destination",
		)
	}
	if r.Group.Template.SnapshotID == "" {
		return nil
	}
	return deleteQuotaRows(
		ctx,
		e,
		`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`,
		r.TenantID,
		r.Group.Template.SnapshotID,
		"operation:"+r.Source,
	)
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
	o.Hash = s.store.sign(o.ID)
	if err = s.quota(
		s.ctx,
		tx,
		Principal{TenantID: t.TenantID},
		0,
		t.Shape.DiskMiB,
	); err != nil {
		if !isCode(err, api.Exhausted) && !isCode(err, api.NotFound) {
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

func (s *Service) prepareBuildRetirement(o Operation) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	r, err := loadRecoveryTx(s.ctx, tx, allocationGroupID(o))
	if err != nil {
		return err
	}
	for index := range r.Allocations {
		a := &r.Allocations[index]
		if a.Kind != "snapshot" || a.Resolution.delivered() {
			continue
		}
		if a.Resolution.Proof == nil ||
			a.Resolution.Proof.Status != "succeeded" ||
			a.Resolution.Proof.SnapshotID == "" {
			return api.Fail(
				api.ManualRecovery,
				"build capture has not delivered its identity",
			)
		}
		c, e := snapshotCollector(
			s.ctx,
			tx,
			r.TenantID,
			a.Resolution.Proof.SnapshotID,
			r.Source,
		)
		if e != nil {
			return e
		}
		a.Resolution.State = "cleanup"
		a.Resolution.Destination = c.ID
		if err = putQuotaRef(
			s.ctx,
			tx,
			r.TenantID,
			"snapshot",
			a.Resolution.Proof.SnapshotID,
			quotaRef{Kind: "engine_dependency", ID: "operation:" + o.ID},
		); err != nil {
			return err
		}
	}
	if err = writeRecovery(s.ctx, tx, r); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) commitGroupActiveTx(
	tx *sql.Tx,
	b Sandbox,
	target *activationTarget,
) error {
	r, err := loadRecoveryTx(s.ctx, tx, "allocation:"+target.Origin)
	if err != nil {
		return err
	}
	current, err := getSandbox(s.ctx, tx, b.ID)
	if err != nil {
		return err
	}
	if r.Group == nil || r.Group.Identity == nil ||
		current.State != "provisioning" ||
		current.OperationID != target.Origin ||
		current.TenantID != r.TenantID ||
		current.VMID != target.VM ||
		r.Group.Identity.VM != target.VM ||
		r.Group.Identity.Instance != target.Instance ||
		b.Instance != target.Instance {
		return api.Fail(api.Conflict, "allocation publication changed")
	}
	for index := range r.Allocations {
		a := &r.Allocations[index]
		if a.Kind == "vm_disk" && a.Resolution.Proof != nil &&
			a.Resolution.Proof.VMID == target.VM {
			a.Resolution.State = "active"
			a.Resolution.Destination = "sandbox:" + b.ID
		}
	}
	r.Done = true
	if err = putQuotaRef(
		s.ctx,
		tx,
		r.TenantID,
		"vm_disk",
		target.VM,
		quotaRef{Kind: "active_use", ID: "sandbox:" + b.ID},
	); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND holder_kind='engine_dependency' AND holder_id=?`,
		r.TenantID,
		"operation:"+r.Source,
	); err != nil {
		return err
	}
	if err = saveSandbox(s.ctx, tx, b); err != nil {
		return err
	}
	return writeRecovery(s.ctx, tx, r)
}

// Recovery billing never reconstructs shape or owner from mutable public state.
func (s *Service) quotaRecoveryAllocation(
	o Operation,
	step string,
) (*recoveryAllocation, error) {
	var raw []byte
	var matches int
	err := s.store.db.QueryRowContext(s.ctx, `SELECT a.value,count(*) OVER() FROM resource_recovery r, json_each(r.body,'$.allocations') a WHERE json_extract(a.value,'$.operation_id')=? AND json_extract(a.value,'$.step')=?`, o.ID, step).
		Scan(&raw, &matches)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if matches != 1 {
		return nil, api.Fail(
			api.ManualRecovery,
			"allocation step has multiple descriptors",
		)
	}
	var a recoveryAllocation
	if err = json.Unmarshal(raw, &a); err != nil {
		return nil, err
	}
	return &a, nil
}

func (a recoveryAllocation) validate(proof api.Operation) error {
	if proof.Key != a.Request.Key ||
		!bytes.Equal(encode(proof.Request), encode(a.Request)) ||
		(a.Request.Action == "restore" && proof.VMID != "" && proof.VMID != a.Request.VMID) ||
		(a.Request.Action == "restore" && proof.Status == "succeeded" &&
			(proof.VMID == "" || proof.VMID != a.Request.VMID || proof.ResultInstance == "")) {
		return api.Fail(
			api.ManualRecovery,
			"recovery receipt does not match frozen allocation request",
		)
	}
	return nil
}

// Receipt resolution grants lifecycle progress separately from parent completion.
func (s *Service) reconcileRecoveryAllocations(
	r *resourceRecovery,
) (recoveryProgress, error) {
	current, err := s.loadRecovery(r.ID)
	if err != nil {
		return recoveryProgress{}, err
	}
	*r = current
	progress := recoveryProgress{
		AllowedLifecycleWork: true,
		AllowedParentDone:    true,
	}
	for i := range r.Allocations {
		a := r.Allocations[i]
		if a.Resolution.delivered() {
			continue
		}
		if a.Resolution.State != "unresolved" &&
			a.Resolution.State != "result_known" {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"invalid allocation resolution",
			)
		}
		proof, err := s.findResourceStep(
			a.operation(),
			a.Step,
			a.Request.Action,
		)
		if err != nil {
			return recoveryProgress{}, err
		}
		if proof == nil {
			var dispatched int
			if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE holder_id=?`, "dispatch:"+a.Request.Key).
				Scan(&dispatched); err != nil {
				return recoveryProgress{}, err
			}
			if dispatched != 0 {
				progress.AllowedLifecycleWork = false
				progress.AllowedParentDone = false
				continue
			}
			b, e := getSandbox(s.ctx, s.store.db, a.SandboxID)
			if e != nil && !isCode(e, api.NotFound) {
				return recoveryProgress{}, e
			}
			if e == nil &&
				(b.OperationID == r.Source || strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")) &&
				(b.State == "resuming" || b.State == "pausing") {
				progress.AllowedParentDone = false
				continue
			}
			// The durable marker is written before Submit. Its absence is the only
			// cancellation proof; a receipt query's NotFound alone never suffices.
			tx, err := s.store.db.BeginTx(s.ctx, nil)
			if err != nil {
				return recoveryProgress{}, err
			}
			next := *r
			next.Allocations = append(
				[]recoveryAllocation(nil),
				r.Allocations...,
			)
			next.Allocations[i].Resolution = allocationResolution{
				State:       "cancelled", //nolint:misspell // Preserve the durable Resolution state token.
				Destination: "never-dispatched:" + a.Request.Key,
			}
			_, err = tx.ExecContext(
				s.ctx,
				`DELETE FROM quota_resources WHERE tenant_id=? AND kind='pending_storage' AND resource_key=?`,
				a.ChargeTenantID,
				a.Request.Key,
			)
			if err == nil {
				_, err = tx.ExecContext(
					s.ctx,
					`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=? AND NOT EXISTS(SELECT 1 FROM quota_resources WHERE source_operation_id=? AND kind='pending_storage')`,
					"operation:"+a.OperationID,
					a.OperationID,
				)
			}
			if err == nil {
				err = writeRecovery(s.ctx, tx, next)
			}
			if err == nil {
				err = tx.Commit()
			} else {
				_ = tx.Rollback()
			}
			if err != nil {
				return recoveryProgress{}, err
			}
			*r = next
			continue
		}
		if err = a.validate(*proof); err != nil {
			return recoveryProgress{}, err
		}
		if !recoveryTerminal(proof.Status) {
			progress.AllowedLifecycleWork = false
			progress.AllowedParentDone = false
			continue
		}
		if err = s.acceptQuotaReceipt(
			a.operation(),
			a.Step,
			a.Request,
			*proof,
		); err != nil {
			return recoveryProgress{}, err
		}
		*r, err = s.loadRecovery(r.ID)
		if err != nil {
			return recoveryProgress{}, err
		}
		a = r.Allocations[i]
		if a.Resolution.delivered() {
			continue
		}
		if a.Kind == "snapshot" && proof.SnapshotID != "" {
			ownership := s.lockResourceOwnership()
			tx, err := s.store.db.BeginTx(s.ctx, nil)
			if err == nil {
				next := *r
				next.Allocations = append(
					[]recoveryAllocation(nil),
					r.Allocations...,
				)
				cleanup := resourceRecovery{
					ID:         "snapshot:" + a.OperationID + ":" + a.Step + ":" + proof.SnapshotID,
					Source:     r.Source,
					Step:       a.Step,
					Kind:       "snapshot",
					Snapshot:   proof.SnapshotID,
					TenantID:   a.ChargeTenantID,
					Allocation: &a,
				}
				err = recordSnapshotOwner(
					s.ctx,
					tx,
					proof.SnapshotID,
					a.ChargeTenantID,
					a.OperationID,
					"captured",
				)
				if err == nil {
					_, err = tx.ExecContext(
						s.ctx,
						`INSERT INTO resource_recovery(id,body) VALUES(?,?) ON CONFLICT(id) DO NOTHING`,
						cleanup.ID,
						encode(cleanup),
					)
				}
				if err == nil {
					err = putQuotaRef(
						s.ctx,
						tx,
						a.ChargeTenantID,
						"snapshot",
						proof.SnapshotID,
						quotaRef{Kind: "cleanup", ID: "recovery:" + cleanup.ID},
					)
				}
				next.Allocations[i].Resolution.State = "cleanup"
				next.Allocations[i].Resolution.Destination = cleanup.ID
				if err == nil {
					_, err = tx.ExecContext(
						s.ctx,
						`DELETE FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
						"operation:"+a.OperationID,
					)
				}
				if err == nil {
					err = writeRecovery(s.ctx, tx, next)
				}
				if err == nil {
					err = tx.Commit()
				} else {
					_ = tx.Rollback()
				}
				if err == nil {
					*r = next
				}
			}
			ownership()
			if err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if proof.VMID == "" && proof.SnapshotID == "" {
			if err = s.deliverKnownAllocation(
				r,
				i,
				"reclaimed",
				"terminal-no-allocation:"+proof.ID,
				"",
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		b, err := getSandbox(s.ctx, s.store.db, a.SandboxID)
		if err != nil {
			return recoveryProgress{}, err
		}
		v, err := s.opts.Engine.GetVM(s.ctx, proof.VMID)
		if isCode(err, api.NotFound) &&
			(proof.Status == "failed" || proof.Status == "succeeded") &&
			b.TenantID == a.ChargeTenantID &&
			(b.VMID == proof.VMID || b.VMID == "") {
			if err = s.deliverKnownAllocation(
				r,
				i,
				"reclaimed",
				"vm-absent:"+proof.VMID,
				proof.VMID,
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if err != nil {
			return recoveryProgress{}, err
		}
		if b.TenantID != a.ChargeTenantID || b.VMID != proof.VMID ||
			!v.Verified ||
			v.OperationID != "" ||
			v.ID != proof.VMID {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"allocation destination is unverified or owned elsewhere",
			)
		}
		if proof.Status == "failed" {
			// An accepted failure's VMID conservatively bills a disk version. It is
			// handed to proven existing V responsibility, never to a guessed candidate.
			if v.Instance == "" || v.Instance != a.Request.ExpectedInstance ||
				b.Instance != v.Instance {
				return recoveryProgress{}, api.Fail(
					api.ManualRecovery,
					"failed allocation has no proven existing VM holder",
				)
			}
			if err = s.deliverKnownAllocation(
				r,
				i,
				"cleanup",
				"vm:"+v.ID,
				v.ID,
			); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		if proof.ResultInstance == "" || v.Instance != proof.ResultInstance ||
			(b.Instance != a.Request.ExpectedInstance && b.Instance != proof.ResultInstance) {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"successful allocation lacks exact result instance proof",
			)
		}
		if b.State == "terminated" {
			if err = s.handoffInstanceCleanup(r, i, b, v); err != nil {
				return recoveryProgress{}, err
			}
			continue
		}
		owned := b.OperationID == r.Source ||
			strings.HasPrefix(b.OperationID, "recovery:"+r.ID+":")
		if !owned || b.State != "resuming" {
			return recoveryProgress{}, api.Fail(
				api.ManualRecovery,
				"result has no lawful lifecycle continuation",
			)
		}
		progress.AllowedParentDone = false
	}
	return progress, nil
}

// Terminal receipt dependency retirement shares the destination transaction.
// Existing-VM delivery retains every conservative D version until V is reclaimed.
func (s *Service) deliverKnownAllocation(
	r *resourceRecovery,
	index int,
	state, destination, vm string,
) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	a := r.Allocations[index]
	if a.Resolution.Proof == nil ||
		!recoveryTerminal(a.Resolution.Proof.Status) {
		return api.Fail(api.ManualRecovery, "allocation is not terminal")
	}
	if vm != "" && state == "cleanup" {
		var holders int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources q JOIN quota_refs r USING(tenant_id,kind,resource_key) WHERE q.tenant_id=? AND q.kind='vm_disk' AND q.source_operation_id=? AND r.holder_kind='cleanup' AND r.holder_id=?`, a.ChargeTenantID, a.OperationID, "vm:"+vm).
			Scan(&holders); err != nil {
			return err
		}
		b, err := getSandbox(s.ctx, tx, a.SandboxID)
		if err != nil {
			return err
		}
		if holders == 0 || b.VMID != vm ||
			b.Instance != a.Request.ExpectedInstance ||
			b.TenantID != a.ChargeTenantID {
			return api.Fail(
				api.ManualRecovery,
				"terminal allocation lost its VM destination",
			)
		}
	}
	if vm != "" && state == "reclaimed" &&
		a.Resolution.Proof.Status == "succeeded" {
		if err = s.validateAbsentRestoreTx(tx, *r, a, vm); err != nil {
			return err
		}
	}
	next := *r
	next.Allocations = append([]recoveryAllocation(nil), r.Allocations...)
	next.Allocations[index].Resolution.State = state
	next.Allocations[index].Resolution.Destination = destination
	if err = writeRecovery(s.ctx, tx, next); err != nil {
		return err
	}
	var pending int
	if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_resources WHERE source_operation_id=? AND kind='pending_storage'`, a.OperationID).
		Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return api.Fail(
			api.Conflict,
			"terminal allocation still has pending storage",
		)
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`,
		"operation:"+a.OperationID,
	); err != nil {
		return err
	}
	if vm != "" && state == "reclaimed" {
		b, err := getSandbox(s.ctx, tx, a.SandboxID)
		if err != nil {
			return err
		}
		if b.TenantID != a.ChargeTenantID || (b.VMID != vm && b.VMID != "") {
			return api.Fail(
				api.ManualRecovery,
				"absent allocation has another sandbox target",
			)
		}
		if err = s.reclaimQuotaTx(tx, "vm_disk", vm); err != nil {
			return err
		}
		var remaining int
		if err = tx.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND holder_id=?`, a.ChargeTenantID, "vm:"+vm).
			Scan(&remaining); err != nil {
			return err
		}
		if remaining > 0 {
			return api.Fail(
				api.Conflict,
				"absent VM still has unresolved allocation dependencies",
			)
		}
		if a.Resolution.Proof.Status == "succeeded" && b.VMID == vm {
			b.VMID, b.Instance = "", ""
			if err = saveSandbox(s.ctx, tx, b); err != nil {
				return err
			}
			current, err := getSandbox(s.ctx, tx, b.ID)
			if err != nil {
				return err
			}
			if current.VMID != "" || current.Instance != "" ||
				current.State != b.State ||
				current.TenantID != b.TenantID {
				return api.Fail(
					api.ManualRecovery,
					"absent VM metadata was not retired",
				)
			}
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*r = next
	return nil
}

// The frozen restore intent and its immutable public source remain provenance
// after a successful release clears V. Current metadata cannot supply a witness.
func (s *Service) validateAbsentRestoreTx(
	tx *sql.Tx,
	r resourceRecovery,
	a recoveryAllocation,
	vm string,
) error {
	proof := a.Resolution.Proof
	if proof == nil || proof.Status != "succeeded" ||
		proof.ID == "" || proof.ResultInstance == "" ||
		a.Kind != "vm_disk" || a.Step != "rollback-restore" ||
		a.Request.Action != "restore" || a.Request.VMID != vm ||
		proof.VMID != vm || a.Request.ExpectedInstance == "" ||
		a.Request.SnapshotID == "" || r.Kind != "sandbox" ||
		r.SandboxID != a.SandboxID ||
		!strings.HasPrefix(a.OperationID, "recovery:"+r.ID+":") ||
		a.Request.Key != stepKey(a.operation(), a.Step) {
		return api.Fail(
			api.ManualRecovery,
			"absent restore has no exact successful history",
		)
	}
	if err := a.validate(*proof); err != nil {
		return err
	}
	var raw []byte
	if err := tx.QueryRowContext(s.ctx, `SELECT body FROM operations WHERE id=?`, r.Source).
		Scan(&raw); err != nil {
		return err
	}
	var source Operation
	if err := json.Unmarshal(raw, &source); err != nil {
		return err
	}
	b, err := getSandbox(s.ctx, tx, a.SandboxID)
	if err != nil {
		return err
	}
	if source.ID != r.Source || source.Status != "failed" ||
		source.Command.Action != "rollback" || source.Step != "rollback_restore" ||
		source.SourceInstance != a.Request.ExpectedInstance ||
		source.Command.SandboxID != a.SandboxID || source.Command.CheckpointID == "" ||
		source.TenantID != a.TenantID || source.ChargeTenantID != a.ChargeTenantID ||
		b.TenantID != a.ChargeTenantID ||
		b.Shape != a.Shape ||
		b.State != "terminated" ||
		(b.VMID != vm && b.VMID != "") ||
		(b.VMID == vm && b.Instance != a.Request.ExpectedInstance && b.Instance != proof.ResultInstance) ||
		(b.VMID == "" && b.Instance != "") {
		return api.Fail(
			api.ManualRecovery,
			"absent restore history belongs to another lifecycle",
		)
	}
	return nil
}
