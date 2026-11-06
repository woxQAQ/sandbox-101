package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

type RecoveryPoint struct {
	ID             string    `json:"id"`
	Store          string    `json:"store"`
	Epoch          int64     `json:"epoch"`
	CreatedAt      time.Time `json:"created_at"`
	DatabaseSHA256 string    `json:"database_sha256,omitempty"`
}

func (e *Engine) checkpoint() error {
	var p RecoveryPoint
	p.CreatedAt = time.Now().UTC()
	if err := e.db.QueryRow("SELECT value FROM meta WHERE key='identity'").
		Scan(&p.Store); err != nil {
		return err
	}
	if err := e.db.QueryRow("SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").
		Scan(&p.Epoch); err != nil {
		return err
	}
	return writeJSON(filepath.Join(e.opts.StateDir, "HEAD.json"), p)
}

// Backup uses SQLite's consistent snapshot machinery; copying a live WAL DB is
// never used. The resulting DB and receipt are synced before return.
func (e *Engine) Backup(
	ctx context.Context,
	destination string,
) (RecoveryPoint, error) {
	e.admission.Lock()
	defer e.admission.Unlock()
	p := RecoveryPoint{ID: newID("backup-"), CreatedAt: time.Now().UTC()}
	if _, err := os.Lstat(destination); err == nil {
		return p, api.Fail(api.Conflict, "backup destination exists")
	}
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='identity'").
		Scan(&p.Store); err != nil {
		return p, err
	}
	if _, err := e.db.ExecContext(
		ctx,
		"INSERT OR REPLACE INTO meta VALUES('backup_point',?)",
		p.ID,
	); err != nil {
		return p, err
	}
	if _, err := e.db.ExecContext(
		ctx,
		"VACUUM INTO ?",
		destination,
	); err != nil {
		return p, err
	}
	f, err := os.OpenFile(destination, os.O_RDWR, 0o600)
	if err != nil {
		return p, err
	}
	err = f.Chmod(0o600)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return p, err
	}
	db, err := sql.Open("sqlite", destination)
	if err != nil {
		return p, err
	}
	err = db.QueryRow("SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").
		Scan(&p.Epoch)
	err = errors.Join(err, db.Close())
	if err != nil {
		return p, err
	}
	p.DatabaseSHA256, err = digestFile(destination)
	if err != nil {
		return p, err
	}
	if err = writeJSON(destination+".json", p); err != nil {
		return p, err
	}
	return p, syncDir(filepath.Dir(destination))
}

type Diagnostic struct {
	StateDir         string   `json:"state_dir"`
	Database         string   `json:"database"`
	RecoveryRequired bool     `json:"recovery_required"`
	RecoveryPoint    string   `json:"recovery_point,omitempty"`
	VMs              int      `json:"vms"`
	Pending          int      `json:"pending_operations"`
	Issues           []string `json:"issues"`
}

func Doctor(ctx context.Context, stateDir string) (Diagnostic, error) {
	d := Diagnostic{StateDir: stateDir, Issues: []string{}}
	if _, err := os.Stat(
		filepath.Join(stateDir, "RECOVERY_REQUIRED"),
	); err == nil {
		d.RecoveryRequired = true
	}
	dbpath := filepath.Join(stateDir, "metadata.db")
	if _, err := os.Stat(dbpath); err != nil {
		d.Database = "missing"
		d.RecoveryRequired = true
		d.Issues = append(d.Issues, err.Error())
		return d, nil
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbpath)+"?mode=ro")
	if err != nil {
		return d, err
	}
	defer func() { _ = db.Close() }()
	if err = db.QueryRowContext(ctx, "PRAGMA quick_check").
		Scan(&d.Database); err != nil {
		d.Database = "corrupt"
		d.RecoveryRequired = true
		d.Issues = append(d.Issues, err.Error())
		return d, nil
	}
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM vms").Scan(&d.VMs)
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM operations WHERE status IN ('pending','running')").
		Scan(&d.Pending)
	_ = db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='backup_point'").
		Scan(&d.RecoveryPoint)
	rows, err := db.QueryContext(ctx, "SELECT body FROM vms")
	if err != nil {
		return d, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var data []byte
		var v vmRecord
		if err = rows.Scan(&data); err != nil {
			return d, err
		}
		if err = json.Unmarshal(data, &v); err != nil {
			return d, err
		}
		if _, err = os.Stat(v.Resource.Dir); err != nil {
			d.Issues = append(
				d.Issues,
				fmt.Sprintf(
					"%s resource directory unavailable: %v",
					v.VM.ID,
					err,
				),
			)
		}
	}
	return d, rows.Err()
}

// RestoreBackup never overwrites live metadata. Preserve the entire original
// directory, then use a NEW empty state directory and bind/move retained resource
// directories to their recorded absolute paths before recovery acknowledgement.
// It intentionally requires the original state path because paths are durable.
func RestoreBackup(stateDir, backup string) error {
	lock, err := adminLock(stateDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	for _, name := range []string{"metadata.db", "metadata.db-wal", "metadata.db-shm"} {
		if _, err := os.Lstat(filepath.Join(stateDir, name)); err == nil {
			return api.Fail(
				api.Conflict,
				"preserve and move existing metadata.db, -wal and -shm before explicit recovery",
			)
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	var p RecoveryPoint
	if err := readJSON(backup+".json", &p); err != nil {
		return err
	}
	hash, err := digestFile(backup)
	if err != nil {
		return err
	}
	if hash != p.DatabaseSHA256 {
		return api.Fail(api.Incompatible, "backup checksum mismatch")
	}
	if err = writeJSON(
		filepath.Join(stateDir, "RECOVERY_REQUIRED"),
		p,
	); err != nil {
		return err
	}
	target := filepath.Join(stateDir, "metadata.db")
	if err = copyFile(backup, target, 0o600); err != nil {
		return err
	}
	if err = syncTreeFiles(target); err != nil {
		return err
	}
	return syncDir(stateDir)
}

func adminLock(stateDir string) (*os.File, error) {
	f, e := os.OpenFile(
		filepath.Join(stateDir, "writer.lock"),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		_ = f.Close()
		return nil, api.Fail(api.Conflict, "daemon owns writer lock")
	}
	return f, nil
}

func syncTreeFiles(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	return errors.Join(f.Sync(), f.Close())
}

// AcknowledgeRecovery records an operator's review, not an assertion that lost
// operation history was reconstructed. Recovered VMs remain blocked until the
// subsequent ordinary reconciliation proves each resource's identity.
func AcknowledgeRecovery(stateDir, note string) error {
	if note == "" {
		return api.Fail(api.Invalid, "explicit recovery review note required")
	}
	lock, err := adminLock(stateDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()
	var p RecoveryPoint
	if err = readJSON(
		filepath.Join(stateDir, "RECOVERY_REQUIRED"),
		&p,
	); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", filepath.Join(stateDir, "metadata.db"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	var integrity string
	if err = db.QueryRow("PRAGMA quick_check").
		Scan(&integrity); err != nil ||
		integrity != "ok" {
		return api.Fail(
			api.ManualRecovery,
			"restored database integrity failed",
		)
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Operations after the backup may have had effects. Never resume intentions
	// from a rolled-back history automatically; require new explicit intentions.
	rows, err := tx.Query(
		"SELECT id,body FROM operations WHERE status IN ('pending','running')",
	)
	if err != nil {
		return err
	}
	var ops []opRecord
	for rows.Next() {
		var id string
		var data []byte
		var o opRecord
		if err = rows.Scan(&id, &data); err != nil {
			break
		}
		if err = json.Unmarshal(data, &o); err != nil {
			break
		}
		ops = append(ops, o)
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	for _, o := range ops {
		o.Operation.Status = "failed"
		o.Operation.Error = &api.Error{
			Code:    api.ManualRecovery,
			Message: "database restored from " + p.ID + "; operation outcome requires manual review",
		}
		o.Operation.UpdatedAt = time.Now().UTC()
		if _, err = tx.Exec(
			"UPDATE operations SET status='failed',body=? WHERE id=?",
			encode(o),
			o.Operation.ID,
		); err != nil {
			return err
		}
	}
	rows, err = tx.Query("SELECT body FROM vms")
	if err != nil {
		return err
	}
	var vms []vmRecord
	for rows.Next() {
		var data []byte
		var v vmRecord
		if err = rows.Scan(&data); err != nil {
			break
		}
		if err = json.Unmarshal(data, &v); err != nil {
			break
		}
		vms = append(vms, v)
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	for _, v := range vms {
		v.VM.Verified = false
		v.VM.BlockedReason = "manual recovery from " + p.ID + ": " + note
		v.VM.OperationID = ""
		if _, err = tx.Exec(
			"UPDATE vms SET body=? WHERE id=?",
			encode(v),
			v.VM.ID,
		); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(
		"INSERT OR REPLACE INTO meta VALUES('recovery_note',?)",
		string(
			encode(
				map[string]any{
					"point":       p,
					"note":        note,
					"reviewed_at": time.Now().UTC(),
				},
			),
		),
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	// Retain the old HEAD as evidence, replacing the active comparison point only
	// after the explicit review has been persisted.
	if old, x := os.ReadFile(filepath.Join(stateDir, "HEAD.json")); x == nil {
		if err = atomicWrite(
			filepath.Join(stateDir, "HEAD.before-recovery.json"),
			old,
			0o600,
		); err != nil {
			return err
		}
	}
	var store string
	var epoch int64
	if err = db.QueryRow("SELECT value FROM meta WHERE key='identity'").
		Scan(&store); err != nil {
		return err
	}
	if err = db.QueryRow("SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").
		Scan(&epoch); err != nil {
		return err
	}
	if err = writeJSON(
		filepath.Join(stateDir, "HEAD.json"),
		RecoveryPoint{Store: store, Epoch: epoch, CreatedAt: time.Now().UTC()},
	); err != nil {
		return err
	}
	if err = os.Rename(
		filepath.Join(stateDir, "RECOVERY_REQUIRED"),
		filepath.Join(stateDir, "RECOVERY_REVIEWED.json"),
	); err != nil {
		return err
	}
	return syncDir(stateDir)
}
