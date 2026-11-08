package engine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

type vmRecord struct {
	VM       api.VM   `json:"vm"`
	Resource Resource `json:"resource"`
}

type snapshotRecord struct {
	Snapshot api.Snapshot `json:"snapshot"`
	Dir      string       `json:"dir"`
}

type journal struct {
	RestoreDeletion  *gcDeletion       `json:"restore_deletion,omitempty"`
	RestoreMaterials *restoreMaterials `json:"restore_materials,omitempty"`
	RestoreBlocked   bool              `json:"restore_blocked,omitempty"`
	RestoreCleaned   bool              `json:"restore_cleaned,omitempty"`
	BlockedRestoreID string            `json:"blocked_restore_id,omitempty"`
	CaptureBoundary  *captureBoundary  `json:"capture_boundary,omitempty"`
	Materialized     bool              `json:"materialized,omitempty"`
	CaptureFiles     map[string]string `json:"capture_files,omitempty"`
	ArtifactsSynced  bool              `json:"artifacts_synced,omitempty"`
	ResumeRequired   bool              `json:"resume_required,omitempty"`
	Captured         bool              `json:"captured,omitempty"`
	Resource         *Resource         `json:"resource,omitempty"`
	Old              *Resource         `json:"old,omitempty"`
	Lease            bool              `json:"lease,omitempty"`
	Manifest         *Manifest         `json:"manifest,omitempty"`
}

type opRecord struct {
	Operation api.Operation `json:"operation"`
	Journal   journal       `json:"journal"`
}

func newID(prefix string) string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b)
}

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func (e *Engine) loadVM(ctx context.Context, id string) (vmRecord, error) {
	var v vmRecord
	var b []byte
	err := e.db.QueryRowContext(ctx, "SELECT body FROM vms WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return v, api.Fail(api.NotFound, "VM %s not found", id)
	}
	if err == nil {
		err = json.Unmarshal(b, &v)
	}
	return v, err
}

func (e *Engine) saveVM(v vmRecord) error {
	_, err := e.db.Exec("UPDATE vms SET body=? WHERE id=?", encode(v), v.VM.ID)
	return err
}

func (e *Engine) loadOp(ctx context.Context, id string) (opRecord, error) {
	var o opRecord
	var b []byte
	err := e.db.QueryRowContext(ctx, "SELECT body FROM operations WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return o, api.Fail(api.NotFound, "operation %s not found", id)
	}
	if err == nil {
		err = json.Unmarshal(b, &o)
	}
	return o, err
}

func (e *Engine) saveOp(o *opRecord, step string) error {
	o.Operation.Step = step
	o.Operation.UpdatedAt = time.Now().UTC()
	_, err := e.db.Exec(
		"UPDATE operations SET status=?,body=? WHERE id=?",
		o.Operation.Status,
		encode(o),
		o.Operation.ID,
	)
	if err == nil && e.opts.Fault != nil {
		err = e.opts.Fault(step)
	}
	return err
}

func (e *Engine) loadSnapshot(
	ctx context.Context,
	id string,
) (snapshotRecord, error) {
	var s snapshotRecord
	var b []byte
	err := e.db.QueryRowContext(ctx, "SELECT body FROM snapshots WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return s, api.Fail(api.NotFound, "snapshot %s not found", id)
	}
	if err == nil {
		err = json.Unmarshal(b, &s)
	}
	return s, err
}

func (e *Engine) saveSnapshot(s snapshotRecord) error {
	_, err := e.db.Exec(
		"UPDATE snapshots SET body=? WHERE id=?",
		encode(s),
		s.Snapshot.ID,
	)
	return err
}

func terminal(s string) bool { return s == "succeeded" || s == "failed" }

func asAPI(err error) *api.Error {
	if a, ok := errors.AsType[*api.Error](err); ok {
		return a
	}
	return &api.Error{Code: api.Internal, Message: err.Error()}
}
