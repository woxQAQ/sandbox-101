package sandboxd

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/sandbox-engine/api"
)

const storeSchemaVersion = 3

type store struct {
	db      *sql.DB
	lock    *os.File
	key     []byte
	auditMu sync.Mutex
}

func openStore(dir string, initialize bool) (*store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, api.Fail(
			api.Invalid,
			"state directory must be a directory, not a symlink",
		)
	}
	if err = os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(
		filepath.Join(dir, "writer.lock"),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, api.Fail(
			api.Conflict,
			"another sandboxd owns state directory",
		)
	}
	s := &store{lock: lock}
	fail := func(e error) (*store, error) { return nil, errors.Join(e, s.close()) }
	if _, e := os.Lstat(filepath.Join(dir, "RECOVERY_REQUIRED")); e == nil {
		return fail(
			api.Fail(
				api.ManualRecovery,
				"sandboxd metadata was restored; review engine inventory and acknowledge the recovery point",
			),
		)
	} else if !os.IsNotExist(e) {
		return fail(e)
	}
	path := filepath.Join(dir, "sandboxd.db")
	databaseInfo, err := os.Lstat(path)
	fresh := os.IsNotExist(err)
	if fresh {
		if !initialize {
			return fail(
				api.Fail(
					api.ManualRecovery,
					"database missing; use --init only for a new installation",
				),
			)
		}
		if _, e := os.Stat(filepath.Join(dir, "initialized")); e == nil {
			return fail(
				api.Fail(
					api.ManualRecovery,
					"database lost; preserve state and restore a consistent backup",
				),
			)
		}
	} else if err != nil {
		return fail(err)
	} else if initialize {
		return fail(
			api.Fail(api.Conflict, "database already exists; omit --init"),
		)
	} else if !databaseInfo.Mode().IsRegular() {
		return fail(
			api.Fail(api.ManualRecovery, "database must be a regular file"),
		)
	}
	keyPath := filepath.Join(dir, "node.key")
	key, err := os.ReadFile(keyPath)
	if os.IsNotExist(err) && initialize {
		key = make([]byte, 32)
		if _, err = randRead(key); err != nil {
			return fail(err)
		}
		if err = atomicFile(keyPath, key); err != nil {
			return fail(err)
		}
	} else if err != nil {
		return fail(
			api.Fail(api.ManualRecovery, "node key missing or unreadable"),
		)
	}
	if len(key) != 32 {
		return fail(api.Fail(api.ManualRecovery, "invalid node key"))
	}
	s.key = key
	s.db, err = sql.Open(
		"sqlite",
		path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)",
	)
	if err != nil {
		return fail(err)
	}
	s.db.SetMaxOpenConns(1)
	var integrity string
	if err = s.db.QueryRow("PRAGMA quick_check").
		Scan(&integrity); err != nil ||
		integrity != "ok" {
		return fail(
			api.Fail(
				api.ManualRecovery,
				"database integrity check failed: %v %s",
				err,
				integrity,
			),
		)
	}
	if err = os.Chmod(path, 0o600); err != nil {
		return fail(err)
	}
	if !fresh {
		var version int
		if err = s.db.QueryRow("SELECT version FROM version").
			Scan(&version); err != nil ||
			version != storeSchemaVersion {
			return fail(
				api.Fail(api.Incompatible, "unsupported database schema"),
			)
		}
	}
	_, err = s.db.Exec(`
 CREATE TABLE IF NOT EXISTS version(version INTEGER NOT NULL);
 INSERT INTO version SELECT 3 WHERE NOT EXISTS (SELECT 1 FROM version);
 CREATE TABLE IF NOT EXISTS tenants(id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS sandboxes(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, state TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS templates(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, idem_key TEXT NOT NULL, spec_hash TEXT NOT NULL, status TEXT NOT NULL, body BLOB NOT NULL, UNIQUE(tenant,idem_key));
 CREATE TABLE IF NOT EXISTS warm(vm_id TEXT PRIMARY KEY, template_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, created_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS snapshot_owners(snapshot_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, source_operation_id TEXT NOT NULL, source_kind TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS configuration_fences(sandbox_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS resource_recovery(id TEXT PRIMARY KEY, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS journal(seq INTEGER PRIMARY KEY AUTOINCREMENT, body BLOB NOT NULL, hash TEXT NOT NULL);
 `)
	if err != nil {
		return fail(err)
	}
	var version int
	if err = s.db.QueryRow("SELECT version FROM version").
		Scan(&version); err != nil ||
		version != storeSchemaVersion {
		return fail(api.Fail(api.Incompatible, "unsupported database schema"))
	}
	if err = atomicFile(
		filepath.Join(dir, "initialized"),
		[]byte(fmt.Sprintf("sandboxd schema %d\n", storeSchemaVersion)),
	); err != nil {
		return fail(err)
	}
	if err = s.verifyJournal(context.Background()); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *store) close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	if s.lock != nil {
		err = errors.Join(err, s.lock.Close())
	}
	return err
}

func atomicFile(path string, b []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".publish-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if err = f.Chmod(0o600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err == nil {
		err = os.Rename(f.Name(), path)
	}
	if err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

// diskSandbox intentionally contains only guest-visible values. Host secrets never reach SQLite.
type diskSandbox struct {
	Sandbox

	Env map[string]string `json:"guest_env"`
}

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func decodeSandbox(b []byte) (Sandbox, error) {
	var d diskSandbox
	err := json.Unmarshal(b, &d)
	d.Sandbox.Env = d.Env
	return d.Sandbox, err
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
		return api.Fail(api.NotFound, "not found")
	}
	return err
}

func recordSnapshotOwner(
	ctx context.Context,
	e executor,
	snapshot, tenant, operation, kind string,
) error {
	if snapshot == "" || tenant == "" || operation == "" {
		return api.Fail(api.ManualRecovery, "snapshot source is incomplete")
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
		return api.Fail(api.NotFound, "not found")
	}
	if source != operation || sourceKind != kind {
		return api.Fail(
			api.ManualRecovery,
			"snapshot source conflicts with immutable provenance",
		)
	}
	return nil
}

func saveSandbox(ctx context.Context, e executor, b Sandbox) error {
	_, err := e.ExecContext(
		ctx,
		"INSERT INTO sandboxes(id,tenant,state,body) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET state=excluded.state,body=excluded.body",
		b.ID,
		b.TenantID,
		b.State,
		encode(diskSandbox{b, b.Env}),
	)
	return err
}

// recordConfigFence atomically admits a configure intent. A newer explicit
// configure replaces the fence; an older operation can never remove it.
// Only identities are stored here, never previous or new parameters.
func recordConfigFence(
	ctx context.Context,
	e executor,
	sandboxID, operationID string,
) error {
	_, err := e.ExecContext(
		ctx,
		"INSERT INTO configuration_fences(sandbox_id,operation_id) VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id",
		sandboxID,
		operationID,
	)
	return err
}

// configFence returns the operation owning the configuration boundary. A
// failed query is never reported as the absence of a fence.
func configFence(
	ctx context.Context,
	e executor,
	sandboxID string,
) (string, error) {
	var operation string
	err := e.QueryRowContext(
		ctx,
		"SELECT operation_id FROM configuration_fences WHERE sandbox_id=?",
		sandboxID,
	).Scan(&operation)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return operation, err
}

// clearConfigFence removes only the fence owned by this operation.
func clearConfigFence(
	ctx context.Context,
	e executor,
	sandboxID, operationID string,
) error {
	res, err := e.ExecContext(
		ctx,
		"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?",
		sandboxID,
		operationID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err == nil && n != 1 {
		return api.Fail(api.Conflict, "configuration operation was superseded")
	}
	return err
}

func getSandbox(ctx context.Context, e executor, id string) (Sandbox, error) {
	var b []byte
	err := e.QueryRowContext(ctx, "SELECT body FROM sandboxes WHERE id=?", id).
		Scan(&b)
	if errors.Is(err, sql.ErrNoRows) {
		return Sandbox{}, api.Fail(api.NotFound, "not found")
	}
	if err != nil {
		return Sandbox{}, err
	}
	return decodeSandbox(b)
}

func saveOp(ctx context.Context, e executor, o Operation) error {
	_, err := e.ExecContext(
		ctx,
		"INSERT INTO operations(id,tenant,idem_key,spec_hash,status,body) VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,body=excluded.body",
		o.ID,
		o.TenantID,
		o.Key,
		o.Hash,
		o.Status,
		encode(o),
	)
	return err
}

func (s *store) operation(ctx context.Context, id string) (Operation, error) {
	var b []byte
	var hash string
	err := s.db.QueryRowContext(ctx, "SELECT body,spec_hash FROM operations WHERE id=?", id).
		Scan(&b, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, api.Fail(api.NotFound, "not found")
	}
	var o Operation
	if err == nil {
		err = json.Unmarshal(b, &o)
		o.Hash = hash
	}
	return o, err
}

func (s *store) byKey(
	ctx context.Context,
	tenant, key string,
) (Operation, error) {
	var id string
	err := s.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE tenant=? AND idem_key=?", tenant, key).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, api.Fail(api.NotFound, "not found")
	}
	if err != nil {
		return Operation{}, err
	}
	return s.operation(ctx, id)
}

func (s *store) sandboxes(ctx context.Context) ([]Sandbox, error) {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT body FROM sandboxes ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Sandbox{}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		b, e := decodeSandbox(raw)
		if e != nil {
			return nil, e
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *store) templates(ctx context.Context) ([]Template, error) {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT body FROM templates ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Template{}
	for rows.Next() {
		var b []byte
		var t Template
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *store) template(ctx context.Context, id string) (Template, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Template{}, api.Fail(api.NotFound, "not found")
	}
	var t Template
	if err == nil {
		err = json.Unmarshal(raw, &t)
	}
	return t, err
}

func (s *store) checkpoint(ctx context.Context, id string) (Checkpoint, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM checkpoints WHERE id=?", id).
		Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return Checkpoint{}, api.Fail(api.NotFound, "not found")
	}
	var cp Checkpoint
	if err == nil {
		err = json.Unmarshal(raw, &cp)
	}
	return cp, err
}

func (s *store) sign(parts ...string) string {
	h := hmac.New(sha256.New, s.key)
	for _, p := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashToken(
	v string,
) string {
	h := sha256.Sum256([]byte(v))
	return hex.EncodeToString(h[:])
}

func (s *store) token(
	b Sandbox,
) string {
	return s.sign("sandbox-token-v1", b.ID, b.TenantID, b.Epoch)
}

func (s *store) audit(ctx context.Context, event any) error {
	s.auditMu.Lock()
	defer s.auditMu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var prev string
	err = tx.QueryRowContext(ctx, "SELECT hash FROM journal ORDER BY seq DESC LIMIT 1").
		Scan(&prev)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	body := encode(struct {
		Time  time.Time `json:"time"`
		Event any       `json:"event"`
	}{time.Now().UTC(), event})
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO journal(body,hash) VALUES(?,?)",
		body,
		s.sign("journal-v1", prev, string(body)),
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *store) verifyJournal(ctx context.Context) error {
	rows, err := s.db.QueryContext(
		ctx,
		"SELECT body,hash FROM journal ORDER BY seq",
	)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	prev := ""
	for rows.Next() {
		var b []byte
		var hash string
		if err = rows.Scan(&b, &hash); err != nil {
			return err
		}
		if !hmac.Equal(
			[]byte(hash),
			[]byte(s.sign("journal-v1", prev, string(b))),
		) {
			return api.Fail(
				api.ManualRecovery,
				"audit journal verification failed",
			)
		}
		prev = hash
	}
	return rows.Err()
}
