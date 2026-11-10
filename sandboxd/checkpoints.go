package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) deleteCheckpoint(o *Operation) error {
	unlock := s.lock("checkpoint:" + o.Command.CheckpointID)
	defer unlock()
	cp, err := s.store.checkpoint(s.ctx, o.Command.CheckpointID)
	if err != nil {
		if isCode(err, ErrNotFound) {
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
	result, err := tx.ExecContext(
		s.ctx,
		"DELETE FROM checkpoints WHERE id=?",
		cp.ID,
	)
	if err = requireSQLWrite(result, err); err != nil {
		return err
	}
	if err = deleteQuotaRows(
		s.ctx,
		tx,
		`FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_id=?`,
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
	); isEngineCode(
		err,
		api.NotFound,
	) {
		return s.reclaimQuota("snapshot", cp.SnapshotID)
	}
	return err
}

func (s *Service) rollback(o *Operation, b *Sandbox) error {
	if b.State != "running" && b.State != "paused" && b.State != "resuming" {
		return Fail(ErrConflict, "sandbox cannot roll back in %s", b.State)
	}
	unlock := s.lock("checkpoint:" + o.Command.CheckpointID)
	defer unlock()
	cp, err := s.store.checkpoint(s.ctx, o.Command.CheckpointID)
	if err != nil {
		return err
	}
	if cp.SandboxID != b.ID || cp.TenantID != b.TenantID {
		return Fail(ErrNotFound, "not found")
	}
	if err = requireSnapshotOwner(
		s.ctx,
		s.store.db,
		cp.SnapshotID,
		b.TenantID,
	); err != nil {
		return err
	}
	if s.credentialsMissing(*b) {
		return Fail(
			ErrConflict,
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

// Checkpoints lists a tenant's checkpoints, or all checkpoints for root callers.
// Sandbox-scoped principals cannot list checkpoints.
func (s *Service) Checkpoints(
	ctx context.Context,
	p Principal,
) ([]Checkpoint, error) {
	if p.SandboxID != "" {
		return nil, Fail(ErrConflict, "tenant access required")
	}
	rows, err := s.store.db.QueryContext(
		ctx,
		"SELECT body FROM checkpoints ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Checkpoint{}
	for rows.Next() {
		var raw []byte
		var cp Checkpoint
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &cp); err != nil {
			return nil, err
		}
		if p.Root || p.TenantID == cp.TenantID {
			out = append(out, cp)
		}
	}
	return out, rows.Err()
}

func getCheckpointTx(
	ctx context.Context,
	e executor,
	id string,
) (Checkpoint, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM checkpoints WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, Fail(ErrNotFound, "not found")
	}
	var cp Checkpoint
	if err == nil {
		err = json.Unmarshal(b, &cp)
	}
	return cp, err
}

func (s *store) checkpoint(ctx context.Context, id string) (Checkpoint, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM checkpoints WHERE id=?", id).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, Fail(ErrNotFound, "not found")
	}
	var cp Checkpoint
	if err == nil {
		err = json.Unmarshal(raw, &cp)
	}
	return cp, err
}

// Checkpoint identifies a sandbox capture retained independently of its live VM.
type Checkpoint struct {
	ID         string    `json:"id"`
	TenantID   string    `json:"tenant_id"`
	SandboxID  string    `json:"sandbox_id"`
	SnapshotID string    `json:"snapshot_id"`
	Shape      api.Shape `json:"shape"`
	CreatedAt  time.Time `json:"created_at"`
}
