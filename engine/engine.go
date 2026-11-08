package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Options configures durable state, backend effects, and admission and
// reconciliation limits. Fault is an embedding crash boundary.
type Options struct {
	StateDir          string
	Init              bool
	Backend           Backend
	SafetyBytes       int64
	MaxMemoryMiB      int64
	MaxVCPUs          int
	ReconcileInterval time.Duration
	GCInterval        time.Duration
	// Fault is a test/embedding crash boundary, called only after durable steps.
	Fault func(string) error
}

var ErrInterrupted = errors.New(
	"operation interrupted; durable intent retained",
)

// Engine serializes durable VM intents and coordinates backend effects with
// operation, snapshot, and resource ownership records.
type Engine struct {
	opts      Options
	db        *sql.DB
	lock      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	admission sync.Mutex
	mu        sync.Mutex
	locks     map[string]*sync.Mutex
	active    map[string]bool
	closing   bool
	capture   chan struct{}
	artifacts sync.Mutex
	lastGC    GCReport
}

var _ api.Client = (*Engine)(nil)

// Open opens or explicitly initializes state, adopts existing instances, and
// starts operation dispatch, reconciliation, and garbage collection.
func Open(opts Options) (*Engine, error) {
	if opts.StateDir == "" || opts.Backend == nil {
		return nil, api.Fail(
			api.Invalid,
			"state directory and real backend are required",
		)
	}
	dir, err := filepath.Abs(opts.StateDir)
	if err != nil {
		return nil, err
	}
	opts.StateDir = dir
	if opts.SafetyBytes < 0 || opts.MaxMemoryMiB < 0 || opts.MaxVCPUs < 0 ||
		opts.ReconcileInterval < 0 ||
		opts.GCInterval < 0 {
		return nil, api.Fail(api.Invalid, "negative resource/reconcile limits")
	}
	if opts.SafetyBytes == 0 {
		opts.SafetyBytes = 256 << 20
	}
	if opts.MaxMemoryMiB == 0 {
		var s unix.Sysinfo_t
		if unix.Sysinfo(&s) == nil {
			opts.MaxMemoryMiB = int64(
				s.Totalram,
			) * int64(
				s.Unit,
			) / (1 << 20) * 8 / 10
		}
	}
	if opts.MaxVCPUs == 0 {
		opts.MaxVCPUs = runtime.NumCPU()
	}
	if opts.ReconcileInterval == 0 {
		opts.ReconcileInterval = 2 * time.Second
	}
	if opts.Init {
		if err = os.MkdirAll(dir, 0o700); err != nil {
			return nil, err
		}
	}
	info, x := os.Stat(dir)
	if x != nil {
		return nil, x
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, api.Fail(
			api.Invalid,
			"state directory must be private (mode 0700)",
		)
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
			"another engine writer owns this state directory",
		)
	}
	fail := func(e error) (*Engine, error) { return nil, errors.Join(e, lock.Close()) }
	dbpath := filepath.Join(dir, "metadata.db")
	_, staterr := os.Stat(dbpath)
	if staterr != nil && !opts.Init {
		return fail(
			api.Fail(
				api.ManualRecovery,
				"metadata missing: initialize a fresh directory explicitly or recover a consistent backup",
			),
		)
	}
	if opts.Init && os.IsNotExist(staterr) {
		entries, e := os.ReadDir(dir)
		if e != nil {
			return fail(e)
		}
		for _, entry := range entries {
			if entry.Name() != "writer.lock" {
				return fail(
					api.Fail(
						api.ManualRecovery,
						"initialization requires an empty state directory",
					),
				)
			}
		}
	}
	if _, e := os.Stat(filepath.Join(dir, "RECOVERY_REQUIRED")); e == nil {
		return fail(
			api.Fail(
				api.ManualRecovery,
				"backup restored: run recover with explicit acknowledgement before starting",
			),
		)
	}
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.ToSlash(
			dbpath,
		)+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)",
	)
	if err != nil {
		return fail(err)
	}
	db.SetMaxOpenConns(1)
	failDB := func(e error) (*Engine, error) { return fail(errors.Join(e, db.Close())) }
	if staterr == nil {
		var integrity string
		if err = db.QueryRow("PRAGMA quick_check").
			Scan(&integrity); err != nil ||
			integrity != "ok" {
			return failDB(
				api.Fail(
					api.ManualRecovery,
					"database integrity check failed: %v %s",
					err,
					integrity,
				),
			)
		}
		var version string
		if err = db.QueryRow("SELECT value FROM meta WHERE key='schema'").
			Scan(&version); err != nil ||
			version != "1" {
			return failDB(
				api.Fail(
					api.ManualRecovery,
					"existing DB does not have the supported engine schema",
				),
			)
		}
	}
	schema := `CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY,key TEXT NOT NULL UNIQUE,hash TEXT NOT NULL,status TEXT NOT NULL,body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS vms(id TEXT PRIMARY KEY,slot INTEGER NOT NULL UNIQUE,body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS snapshots(id TEXT PRIMARY KEY,body BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS leases(snapshot TEXT NOT NULL,operation TEXT PRIMARY KEY);
 CREATE TABLE IF NOT EXISTS reservations(owner TEXT PRIMARY KEY,bytes INTEGER NOT NULL,memory INTEGER NOT NULL DEFAULT 0,cpus INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE IF NOT EXISTS backing(vm TEXT NOT NULL,path TEXT PRIMARY KEY,digest TEXT NOT NULL);
 CREATE TABLE IF NOT EXISTS gc_deletions(path TEXT PRIMARY KEY,body BLOB NOT NULL);`
	if _, err = db.Exec(schema); err != nil {
		return failDB(err)
	}
	if _, err = db.Exec(
		"INSERT OR IGNORE INTO meta VALUES('identity',?)",
		newID("store-"),
	); err != nil {
		return failDB(err)
	}
	if _, err = db.Exec(
		"INSERT OR IGNORE INTO meta VALUES('schema','1'); INSERT OR IGNORE INTO meta VALUES('epoch','0')",
	); err != nil {
		return failDB(err)
	}
	for _, table := range []string{"operations", "vms", "snapshots", "leases", "reservations", "backing", "gc_deletions"} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			q := fmt.Sprintf(
				"CREATE TRIGGER IF NOT EXISTS epoch_%s_%s AFTER %s ON %s BEGIN UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='epoch'; END",
				table,
				event,
				event,
				table,
			)
			if _, err = db.Exec(q); err != nil {
				return failDB(err)
			}
		}
	}
	var head RecoveryPoint
	if err = readJSON(filepath.Join(dir, "HEAD.json"), &head); err == nil {
		var store string
		var epoch int64
		if err = db.QueryRow("SELECT value FROM meta WHERE key='identity'").
			Scan(&store); err != nil {
			return failDB(err)
		}
		if err = db.QueryRow("SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").
			Scan(&epoch); err != nil {
			return failDB(err)
		}
		if store != head.Store || epoch < head.Epoch {
			return failDB(
				api.Fail(
					api.ManualRecovery,
					"database identity/epoch is older than durable HEAD; explicit backup recovery required",
				),
			)
		}
	} else if !os.IsNotExist(err) {
		return failDB(err)
	}
	for _, d := range []string{"vms", "snapshots", "images", "imports", "backups"} {
		if err = os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			return failDB(err)
		}
	}
	if err = os.Chmod(dbpath, 0o600); err != nil {
		return failDB(err)
	}
	if err = syncDir(dir); err != nil {
		return failDB(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		opts:    opts,
		db:      db,
		lock:    lock,
		ctx:     ctx,
		cancel:  cancel,
		locks:   map[string]*sync.Mutex{},
		active:  map[string]bool{},
		capture: make(chan struct{}, 1),
	}
	if os.IsNotExist(staterr) {
		if err = e.checkpoint(); err != nil {
			cancel()
			return failDB(err)
		}
	}
	// Startup adoption finishes before accepting new work.
	if err = e.Reconcile(ctx); err != nil {
		cancel()
		return failDB(err)
	}
	rows, err := db.Query(
		"SELECT body FROM operations WHERE status IN ('pending','running') ORDER BY rowid",
	)
	if err != nil {
		cancel()
		return failDB(err)
	}
	var pending []opRecord
	for rows.Next() {
		var b []byte
		var op opRecord
		if err = rows.Scan(&b); err == nil {
			err = json.Unmarshal(b, &op)
		}
		if err != nil {
			break
		}
		pending = append(pending, op)
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		cancel()
		return failDB(err)
	}
	for _, op := range pending {
		e.dispatch(op.Operation.ID)
	}
	var events <-chan struct{}
	if source, ok := opts.Backend.(interface{ Events() <-chan struct{} }); ok {
		events = source.Events()
	}
	e.wg.Go(func() {
		t := time.NewTicker(opts.ReconcileInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = e.Reconcile(ctx)
			case <-events:
				_ = e.Reconcile(ctx)
			}
		}
	})
	e.startGC()
	return e, nil
}

// Close cancels background work and waits for workers before releasing state.
func (e *Engine) Close() error {
	e.admission.Lock()
	e.mu.Lock()
	e.closing = true
	e.mu.Unlock()
	e.cancel()
	e.admission.Unlock()
	e.wg.Wait()
	// Imports and explicit GC calls may be embedding API callers, not workers.
	// Never wait for their guard while holding admission: imports publish there.
	e.artifacts.Lock()
	defer e.artifacts.Unlock()
	return errors.Join(e.db.Close(), e.lock.Close())
}

func (e *Engine) vmLock(id string) *sync.Mutex {
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.locks[id]
	if l == nil {
		l = &sync.Mutex{}
		e.locks[id] = l
	}
	return l
}
