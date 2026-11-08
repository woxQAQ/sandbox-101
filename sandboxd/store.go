package sandboxd

import (
	"context"
	"crypto/hmac"
	"database/sql"
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

const storeSchemaVersion = 6

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
 INSERT INTO version SELECT 6 WHERE NOT EXISTS (SELECT 1 FROM version);
 CREATE TABLE IF NOT EXISTS tenants(id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS sandboxes(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, state TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS templates(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS template_intents(template_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, operation_id TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS checkpoints(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, tenant TEXT NOT NULL, idem_key TEXT NOT NULL, spec_hash TEXT NOT NULL, status TEXT NOT NULL, body BLOB NOT NULL, UNIQUE(tenant,idem_key));
 CREATE TABLE IF NOT EXISTS warm(vm_id TEXT PRIMARY KEY, template_id TEXT NOT NULL, snapshot_id TEXT NOT NULL, created_at INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS snapshot_owners(snapshot_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, source_operation_id TEXT NOT NULL, source_kind TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS configuration_fences(sandbox_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS resource_recovery(id TEXT PRIMARY KEY, body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS quota_resources(tenant_id TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('sandbox_slot','build_slot','vm_disk','snapshot','pending_storage')), resource_key TEXT NOT NULL, sandbox_slots INTEGER NOT NULL CHECK(sandbox_slots>=0), storage_mib INTEGER NOT NULL CHECK(storage_mib>=0), source_operation_id TEXT NOT NULL, PRIMARY KEY(tenant_id,kind,resource_key));
 CREATE TABLE IF NOT EXISTS quota_refs(tenant_id TEXT NOT NULL, kind TEXT NOT NULL, resource_key TEXT NOT NULL, holder_kind TEXT NOT NULL CHECK(holder_kind IN ('active_use','engine_dependency','cleanup')), holder_id TEXT NOT NULL, PRIMARY KEY(tenant_id,kind,resource_key,holder_kind,holder_id), FOREIGN KEY(tenant_id,kind,resource_key) REFERENCES quota_resources(tenant_id,kind,resource_key) ON DELETE CASCADE);
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
	if err = s.validateQuota(context.Background()); err != nil {
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

func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
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
