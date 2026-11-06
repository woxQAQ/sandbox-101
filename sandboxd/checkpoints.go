package sandboxd

import (
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

func (s *Service) deleteCheckpoint(o *Operation) error {
	unlock := s.lock("checkpoint:" + o.Command.CheckpointID)
	defer unlock()
	cp, err := s.store.checkpoint(s.ctx, o.Command.CheckpointID)
	if err != nil {
		if isCode(err, api.NotFound) {
			return nil
		}
		return err
	}
	ownership := s.lockResourceOwnership()
	defer ownership()
	if err = s.retireSnapshotOwned(*o, cp.SnapshotID, "", cp.ID); err != nil {
		return err
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(
		s.ctx,
		"DELETE FROM checkpoints WHERE id=?",
		cp.ID,
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`UPDATE quota_refs SET holder_kind='cleanup' WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_id=?`,
		cp.TenantID,
		cp.SnapshotID,
		"checkpoint:"+cp.ID,
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if _, err = s.opts.Engine.GetSnapshot(
		s.ctx,
		cp.SnapshotID,
	); isCode(
		err,
		api.NotFound,
	) {
		return s.reclaimQuota("snapshot", cp.SnapshotID)
	}
	return err
}

func (s *Service) rollback(o *Operation, b *Sandbox) error {
	if b.State != "running" && b.State != "paused" && b.State != "resuming" {
		return api.Fail(api.Conflict, "sandbox cannot roll back in %s", b.State)
	}
	unlock := s.lock("checkpoint:" + o.Command.CheckpointID)
	defer unlock()
	cp, err := s.store.checkpoint(s.ctx, o.Command.CheckpointID)
	if err != nil {
		return err
	}
	if cp.SandboxID != b.ID || cp.TenantID != b.TenantID {
		return api.Fail(api.NotFound, "not found")
	}
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		cp.SnapshotID,
		b.TenantID,
	); err != nil {
		return err
	}
	if s.CredentialsMissing(*b) {
		return api.Fail(
			api.Conflict,
			"host credentials must be supplied before rollback",
		)
	}
	s.closeConnections(b.ID)
	if err = s.revoke(b.ID); err != nil {
		return err
	}
	if o.Step == "" {
		o.Step = "rollback_clone"
		if b.VMID != "" {
			o.Step = "rollback_restore"
			o.SourceInstance = b.Instance
		}
		if err = saveOp(s.ctx, s.store.db, *o); err != nil {
			return err
		}
	}
	b.State = "resuming"
	b.OperationID = o.ID
	if err = saveSandbox(s.ctx, s.store.db, *b); err != nil {
		return err
	}
	if o.Step == "rollback_restore" {
		if _, err = s.engine(
			*o,
			"rollback-stop",
			api.Request{
				Action:           "stop",
				VMID:             b.VMID,
				ExpectedInstance: o.SourceInstance,
			},
		); err != nil {
			return err
		}
		if _, err = s.engine(
			*o,
			"rollback-restore",
			api.Request{
				Action:     "restore",
				VMID:       b.VMID,
				SnapshotID: cp.SnapshotID,
			},
		); err != nil {
			return err
		}
	} else {
		op, e := s.engine(
			*o,
			"rollback-clone",
			api.Request{Action: "clone", SnapshotID: cp.SnapshotID},
		)
		if e != nil {
			return e
		}
		b.VMID = op.VMID
		if err = s.saveSandboxReference(*b); err != nil {
			return err
		}
	}
	if err = s.activate(*o, b, false); err != nil {
		return err
	}
	return s.finishResume(o, b)
}
