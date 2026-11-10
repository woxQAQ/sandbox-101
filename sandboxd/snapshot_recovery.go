package sandboxd

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/woxQAQ/sandbox-engine/api"
)

// retireSnapshot holds the shared sandbox reference lock until engine deletion
// finishes. A checkpoint may share a sleeping sandbox's immutable capture.
func (s *Service) retireSnapshot(
	o Operation,
	snapshot, excludeSandbox, excludeCheckpoint string,
) error {
	if snapshot == "" {
		return nil
	}
	unlock := s.lockResourceOwnership()
	defer unlock()
	return s.retireSnapshotOwned(o, snapshot, excludeSandbox, excludeCheckpoint)
}

func (s *Service) retireSnapshotOwned(
	o Operation,
	snapshot, excludeSandbox, excludeCheckpoint string,
) error {
	return s.retireSnapshotOwnedExcept(
		o,
		snapshot,
		excludeSandbox,
		excludeCheckpoint,
		o.ID,
	)
}

func (s *Service) retireSnapshotOwnedExcept(
	o Operation,
	snapshot, excludeSandbox, excludeCheckpoint, excludeOperation string,
) error {
	var users int
	if err := s.store.db.QueryRowContext(s.ctx, `SELECT
 (SELECT count(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_kind IN ('active_use','engine_dependency') AND holder_id NOT LIKE 'backing:%' AND holder_id NOT IN (?,?,?)) +
 (SELECT count(*) FROM sandboxes WHERE id!=? AND json_extract(body,'$.snapshot_id')=?) +
 (SELECT count(*) FROM checkpoints WHERE id!=? AND json_extract(body,'$.snapshot_id')=?) +
 (SELECT count(*) FROM templates WHERE json_extract(body,'$.snapshot_id')=?) +
 (SELECT count(*) FROM warm WHERE snapshot_id=?) +
 (SELECT count(*) FROM operations WHERE id!=? AND status IN ('pending','running') AND
   (json_extract(body,'$.snapshot_id')=? OR json_extract(body,'$.command.template.snapshot_id')=?))`, snapshot, "sandbox:"+excludeSandbox, "checkpoint:"+excludeCheckpoint, "operation:"+excludeOperation, excludeSandbox, snapshot, excludeCheckpoint, snapshot, snapshot, snapshot, excludeOperation, snapshot, snapshot).Scan(&users); err != nil {
		return err
	}
	if users > 0 {
		return nil
	}
	_, err := s.engine(
		o,
		"retire-snapshot:"+snapshot,
		api.Request{Action: "delete_snapshot", SnapshotID: snapshot},
	)
	return err
}

func (s *Service) proveRetiredSnapshot(b Sandbox) error {
	if err := requireSnapshotOwner(
		s.ctx,
		s.store.db,
		b.SnapshotID,
		b.TenantID,
	); err != nil {
		if isCode(err, ErrNotFound) {
			return Fail(
				ErrManualRecovery,
				"sleep snapshot provenance cannot be proven",
			)
		}
		return err
	}
	return nil
}

func (s *Service) recoverRetiredSnapshot(r *resourceRecovery) error {
	ownership := s.lockResourceOwnership()
	defer ownership()
	// New allocation parents retain publication readers after their VM retires.
	// These readers and published templates cannot make accounting progress here.
	group, err := s.findAllocationGroup(Operation{ID: r.Source})
	if err != nil {
		return err
	}
	if group != nil {
		var readers int
		if err = s.store.db.QueryRowContext(s.ctx, `SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND ((holder_kind='active_use' AND holder_id LIKE 'template:%') OR (holder_kind='engine_dependency' AND holder_id=? AND EXISTS(SELECT 1 FROM operations WHERE id=? AND status IN ('pending','running'))))`, r.TenantID, r.Snapshot, "operation:"+r.Source, r.Source).
			Scan(&readers); err != nil {
			return err
		}
		if readers != 0 {
			return nil
		}
	}
	_, err = s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
	if isEngineCode(err, api.NotFound) {
		if err = s.reclaimQuota("snapshot", r.Snapshot); err != nil {
			return err
		}
		return s.finishRecoveryAttempt(r, nil)
	}
	if err != nil {
		return err
	}
	// Duplicate handoffs share one deletion sequence, while all retain their
	// persisted charges until physical absence is confirmed.
	var collector string
	if err = s.store.db.QueryRowContext(s.ctx, `SELECT id FROM resource_recovery
        WHERE json_extract(body,'$.kind')='retired_snapshot'
        AND json_extract(body,'$.snapshot_id')=?
        AND COALESCE(json_extract(body,'$.done'),0)=0 ORDER BY id LIMIT 1`, r.Snapshot).Scan(&collector); err != nil {
		return err
	}
	if collector != r.ID {
		return nil
	}
	o := r.operation()
	if err = s.retireSnapshotOwnedExcept(
		o,
		r.Snapshot,
		"",
		"",
		r.Source,
	); err != nil {
		return s.finishRecoveryAttempt(r, err, "retire-snapshot:"+r.Snapshot)
	}
	// A shared live reference leaves both the intent and its charge intact.
	_, err = s.opts.Engine.GetSnapshot(s.ctx, r.Snapshot)
	if isEngineCode(err, api.NotFound) {
		return s.finishRecoveryAttempt(r, nil)
	}
	return err
}

func (s *Service) recoverySnapshotReady(id string) error {
	snap, err := s.opts.Engine.GetSnapshot(s.ctx, id)
	if err != nil {
		return err
	}
	if !snap.Published || snap.Deleting {
		return Fail(
			ErrConflict,
			"recovery snapshot %s is not published",
			id,
		)
	}
	return nil
}

func snapshotCollector(
	ctx context.Context,
	e executor,
	tenant, sid, source string,
) (resourceRecovery, error) {
	var raw []byte
	err := e.QueryRowContext(ctx, `SELECT body FROM resource_recovery WHERE json_extract(body,'$.kind')='retired_snapshot' AND json_extract(body,'$.snapshot_id')=? AND COALESCE(json_extract(body,'$.done'),0)=0 ORDER BY id LIMIT 1`, sid).
		Scan(&raw)
	var r resourceRecovery
	if err == nil {
		err = json.Unmarshal(raw, &r)
		if err == nil && r.TenantID != tenant {
			err = Fail(
				ErrManualRecovery,
				"snapshot collector owner mismatch",
			)
		}
		return r, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return r, err
	}
	h := sha256.Sum256(
		encode([]string{"plan013-retired-snapshot-v1", tenant, sid}),
	)
	r = resourceRecovery{
		ID:       "template-retired:" + hex.EncodeToString(h[:]),
		Source:   source,
		TenantID: tenant,
		Kind:     "retired_snapshot",
		Snapshot: sid,
	}
	if err = insertRecovery(ctx, e, r); err != nil {
		return r, err
	}
	err = putQuotaRef(
		ctx,
		e,
		tenant,
		"snapshot",
		sid,
		quotaRef{Kind: "cleanup", ID: "recovery:" + r.ID},
	)
	return r, err
}

// Snapshot owners record immutable authorization provenance, never GC roots.
// captured means a sandboxd engine receipt; external_import means a root grant.
func requireSnapshotOwner(
	ctx context.Context,
	e executor,
	snapshot, tenant string,
) error {
	if snapshot == "" {
		return nil
	}
	var owner string
	err := e.QueryRowContext(ctx, "SELECT tenant_id FROM snapshot_owners WHERE snapshot_id=?", snapshot).
		Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && owner != tenant) {
		return Fail(ErrNotFound, "not found")
	}
	return err
}

func recordSnapshotOwner(
	ctx context.Context,
	e executor,
	snapshot, tenant, operation, kind string,
) error {
	if snapshot == "" || tenant == "" || operation == "" {
		return Fail(ErrManualRecovery, "snapshot source is incomplete")
	}
	if _, err := e.ExecContext(
		ctx,
		`INSERT INTO snapshot_owners(snapshot_id,tenant_id,source_operation_id,source_kind)
        VALUES(?,?,?,?) ON CONFLICT(snapshot_id) DO NOTHING`,
		snapshot,
		tenant,
		operation,
		kind,
	); err != nil {
		return err
	}
	var owner, source, sourceKind string
	if err := e.QueryRowContext(ctx, "SELECT tenant_id,source_operation_id,source_kind FROM snapshot_owners WHERE snapshot_id=?", snapshot).
		Scan(&owner, &source, &sourceKind); err != nil {
		return err
	}
	if owner != tenant {
		return Fail(ErrNotFound, "not found")
	}
	if source != operation || sourceKind != kind {
		return Fail(
			ErrManualRecovery,
			"snapshot source conflicts with immutable provenance",
		)
	}
	return nil
}
