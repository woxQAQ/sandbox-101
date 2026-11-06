package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/woxQAQ/sandbox-engine/api"
)

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

func newID(prefix string) string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return prefix + hex.EncodeToString(b)
}

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

func checkConfig(c *api.Config) error {
	if c == nil {
		return api.Fail(api.Invalid, "config required")
	}
	if !strings.HasPrefix(c.Image, "sha256:") || len(c.Image) != 71 {
		return api.Fail(
			api.Invalid,
			"image must be an imported sha256 manifest digest",
		)
	}
	if _, err := hex.DecodeString(c.Image[7:]); err != nil {
		return api.Fail(api.Invalid, "invalid image digest")
	}
	s := c.Shape
	if s.VCPUs < 1 || s.VCPUs > 255 || s.MemoryMiB < 128 ||
		s.MemoryMiB > 1<<24 ||
		s.DiskMiB < 64 ||
		s.DiskMiB > 1<<30 {
		return api.Fail(api.Invalid, "shape outside supported bounds")
	}
	return nil
}

func (e *Engine) reserve(
	tx *sql.Tx,
	owner string,
	bytes, memory int64,
	cpus int,
) error {
	var oldBytes, oldMemory int64
	var oldCPUs int
	err := tx.QueryRow("SELECT bytes,memory,cpus FROM reservations WHERE owner=?", owner).
		Scan(&oldBytes, &oldMemory, &oldCPUs)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && oldBytes >= bytes && oldMemory >= memory &&
		oldCPUs >= cpus {
		return nil
	}
	bytes = max(bytes, oldBytes)
	memory = max(memory, oldMemory)
	cpus = max(cpus, oldCPUs)
	var held, mem int64
	var cpu int
	if err = tx.QueryRow("SELECT COALESCE(SUM(bytes),0),COALESCE(SUM(memory),0),COALESCE(SUM(cpus),0) FROM reservations").
		Scan(&held, &mem, &cpu); err != nil {
		return err
	}
	free, err := freeBytes(e.opts.StateDir)
	if err != nil {
		return err
	}
	if free-held+oldBytes-bytes < e.opts.SafetyBytes ||
		mem-oldMemory+memory > e.opts.MaxMemoryMiB ||
		cpu-oldCPUs+cpus > e.opts.MaxVCPUs {
		return api.Fail(
			api.Exhausted,
			"persistent storage, memory, or CPU reservation exceeds host budget: free_bytes=%d held_bytes=%d owner_old_bytes=%d requested_bytes=%d margin_bytes=%d memory_mib=%d/%d vcpus=%d/%d",
			free,
			held,
			oldBytes,
			bytes,
			e.opts.SafetyBytes,
			mem-oldMemory+memory,
			e.opts.MaxMemoryMiB,
			cpu-oldCPUs+cpus,
			e.opts.MaxVCPUs,
		)
	}
	_, err = tx.Exec(
		"INSERT INTO reservations(owner,bytes,memory,cpus) VALUES(?,?,?,?) ON CONFLICT(owner) DO UPDATE SET bytes=excluded.bytes,memory=excluded.memory,cpus=excluded.cpus",
		owner,
		bytes,
		memory,
		cpus,
	)
	return err
}

func (e *Engine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	if r.Key == "" || len(r.Key) > 256 {
		return api.Operation{}, api.Fail(
			api.Invalid,
			"a unique key of 1..256 bytes is required",
		)
	}
	b := encode(r)
	sum := sha256.Sum256(b)
	hash := hex.EncodeToString(sum[:])
	var cachedBody []byte
	var cachedHash string
	if x := e.db.QueryRowContext(ctx, "SELECT body,hash FROM operations WHERE key=?", r.Key).
		Scan(&cachedBody, &cachedHash); x == nil {
		var cached opRecord
		if x = json.Unmarshal(cachedBody, &cached); x != nil {
			return api.Operation{}, x
		}
		if cachedHash != hash {
			return cached.Operation, api.Fail(
				api.Conflict,
				"key already belongs to different parameters",
			)
		}
		return cached.Operation, nil
	} else if !errors.Is(
		x,
		sql.ErrNoRows,
	) {
		return api.Operation{}, x
	}
	// Lock ordering is VM then admission everywhere, including reconciliation.
	if r.VMID != "" {
		l := e.vmLock(r.VMID)
		l.Lock()
		defer l.Unlock()
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	e.mu.Lock()
	closed := e.closing
	e.mu.Unlock()
	if closed {
		return api.Operation{}, api.Fail(api.Internal, "engine shutting down")
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return api.Operation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var old, oldHash []byte
	err = tx.QueryRowContext(ctx, "SELECT body,hash FROM operations WHERE key=?", r.Key).
		Scan(&old, &oldHash)
	if err == nil {
		var o opRecord
		if err = json.Unmarshal(old, &o); err != nil {
			return api.Operation{}, err
		}
		if string(oldHash) != hash {
			return o.Operation, api.Fail(
				api.Conflict,
				"key already belongs to different parameters",
			)
		}
		return o.Operation, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return api.Operation{}, err
	}
	switch r.Action {
	case "create",
		"start",
		"stop",
		"snapshot",
		"restore",
		"clone",
		"delete",
		"delete_snapshot",
		"network",
		"network_policy":
	default:
		return api.Operation{}, api.Fail(
			api.Invalid,
			"unsupported action %q",
			r.Action,
		)
	}
	if r.Action != "create" && r.Action != "clone" &&
		r.Action != "delete_snapshot" &&
		r.VMID == "" {
		return api.Operation{}, api.Fail(api.Invalid, "vm_id required")
	}
	if (r.Action == "restore" || r.Action == "clone" || r.Action == "delete_snapshot") &&
		r.SnapshotID == "" {
		return api.Operation{}, api.Fail(api.Invalid, "snapshot_id required")
	}
	if r.Action == "network" || r.Action == "network_policy" {
		if r.ExpectedInstance == "" {
			return api.Operation{}, api.Fail(
				api.Invalid,
				"network changes require expected_instance",
			)
		}
	}
	if r.Action == "network_policy" {
		if err = validateNetworkPolicy(r.Policy); err != nil {
			return api.Operation{}, err
		}
	} else if r.Policy != nil {
		return api.Operation{}, api.Fail(
			api.Invalid,
			"policy is only accepted by network_policy",
		)
	}
	o := opRecord{
		Operation: api.Operation{
			ID:         newID("op-"),
			Key:        r.Key,
			Request:    r,
			Status:     "pending",
			CreatedAt:  time.Now().UTC(),
			UpdatedAt:  time.Now().UTC(),
			VMID:       r.VMID,
			SnapshotID: r.SnapshotID,
		},
	}
	var v vmRecord
	if r.Action == "create" || r.Action == "clone" {
		if r.VMID != "" {
			return api.Operation{}, api.Fail(
				api.Invalid,
				"engine allocates VM IDs",
			)
		}
		config := r.Config
		if r.Action == "clone" {
			var sb []byte
			err = tx.QueryRow("SELECT body FROM snapshots WHERE id=?", r.SnapshotID).
				Scan(&sb)
			if errors.Is(err, sql.ErrNoRows) {
				return api.Operation{}, api.Fail(
					api.NotFound,
					"snapshot not found",
				)
			}
			if err != nil {
				return api.Operation{}, err
			}
			var s snapshotRecord
			if err = json.Unmarshal(sb, &s); err != nil {
				return api.Operation{}, err
			}
			config = &s.Snapshot.Config
		}
		if err = checkConfig(config); err != nil {
			return api.Operation{}, err
		}
		o.Operation.VMID = newID("vm-")
		rows, x := tx.Query("SELECT slot FROM vms ORDER BY slot")
		if x != nil {
			return api.Operation{}, x
		}
		slot := 1
		for rows.Next() {
			var n int
			if err = rows.Scan(&n); err != nil {
				break
			}
			if n == slot {
				slot++
			}
		}
		if err == nil {
			err = rows.Err()
		}
		err = errors.Join(err, rows.Close())
		if err != nil {
			return api.Operation{}, err
		}
		if slot >= 32768 {
			return api.Operation{}, api.Fail(
				api.Exhausted,
				"network address pool exhausted",
			)
		}
		dir := filepath.Join(e.opts.StateDir, "vms", o.Operation.VMID)
		v = vmRecord{
			VM: api.VM{
				ID:          o.Operation.VMID,
				Config:      *config,
				Phase:       "creating",
				CreatedAt:   time.Now().UTC(),
				OperationID: o.Operation.ID,
			},
			Resource: Resource{
				VMID: o.Operation.VMID,
				Dir:  dir,
				Disk: filepath.Join(
					dir,
					"disks",
					o.Operation.ID+".raw",
				),
				ImageDir: filepath.Join(
					e.opts.StateDir,
					"images",
					config.Image[7:],
				),
				Config:       *config,
				NetworkToken: newID("net-"),
				Slot:         slot,
			},
		}
		bytes := config.Shape.DiskMiB << 20
		if r.Action == "clone" {
			bytes += config.Shape.MemoryMiB << 20
		}
		if err = e.reserve(
			tx,
			v.VM.ID,
			bytes,
			config.Shape.MemoryMiB+128,
			config.Shape.VCPUs,
		); err != nil {
			return api.Operation{}, err
		}
		if _, err = tx.Exec(
			"INSERT INTO vms VALUES(?,?,?)",
			v.VM.ID,
			slot,
			encode(v),
		); err != nil {
			return api.Operation{}, err
		}
	} else if r.Action != "delete_snapshot" {
		var vb []byte
		err = tx.QueryRow("SELECT body FROM vms WHERE id=?", r.VMID).Scan(&vb)
		if errors.Is(err, sql.ErrNoRows) {
			return api.Operation{}, api.Fail(api.NotFound, "VM not found")
		}
		if err != nil {
			return api.Operation{}, err
		}
		if err = json.Unmarshal(vb, &v); err != nil {
			return api.Operation{}, err
		}
		if v.VM.Deleting && r.Action != "delete" {
			return api.Operation{}, api.Fail(api.Conflict, "VM deleting")
		}
		if v.VM.OperationID != "" {
			var priorBody []byte
			var prior opRecord
			if err = tx.QueryRow("SELECT body FROM operations WHERE id=?", v.VM.OperationID).
				Scan(&priorBody); err != nil {
				return api.Operation{}, err
			}
			if err = json.Unmarshal(priorBody, &prior); err != nil {
				return api.Operation{}, err
			}
			blockedID := prior.Operation.ID
			if prior.Operation.Request.Action == "delete" {
				blockedID = prior.Journal.BlockedRestoreID
			}
			var blocked opRecord
			if blockedID == prior.Operation.ID {
				blocked = prior
			} else if blockedID != "" {
				if err = tx.QueryRow("SELECT body FROM operations WHERE id=?", blockedID).
					Scan(&priorBody); err != nil {
					return api.Operation{}, err
				}
				if err = json.Unmarshal(priorBody, &blocked); err != nil {
					return api.Operation{}, err
				}
			}
			if r.Action != "delete" || prior.Operation.Status != "failed" ||
				prior.Operation.VMID != r.VMID ||
				blocked.Operation.ID != blockedID ||
				blocked.Operation.VMID != r.VMID ||
				blocked.Operation.Status != "failed" ||
				!blocked.Journal.RestoreBlocked ||
				blocked.Journal.RestoreCleaned ||
				(blocked.Operation.Request.Action != "restore" && blocked.Operation.Request.Action != "clone") ||
				r.ExpectedInstance != v.VM.Instance {
				return api.Operation{}, api.Fail(
					api.Conflict,
					"VM has an active or unresolved operation: %s",
					v.VM.OperationID,
				)
			}
			o.Journal.BlockedRestoreID = blockedID
		}
		if r.ExpectedInstance != "" && r.ExpectedInstance != v.VM.Instance {
			return api.Operation{}, api.Fail(
				api.Conflict,
				"running instance changed",
			)
		}
		if r.Action == "delete" {
			v.VM.Deleting = true
		}
		v.VM.OperationID = o.Operation.ID
		if _, err = tx.Exec(
			"UPDATE vms SET body=? WHERE id=?",
			encode(v),
			v.VM.ID,
		); err != nil {
			return api.Operation{}, err
		}
	}
	if r.Action == "clone" || r.Action == "restore" ||
		r.Action == "delete_snapshot" {
		var sb []byte
		err = tx.QueryRow("SELECT body FROM snapshots WHERE id=?", r.SnapshotID).
			Scan(&sb)
		if errors.Is(err, sql.ErrNoRows) {
			return api.Operation{}, api.Fail(api.NotFound, "snapshot not found")
		}
		if err != nil {
			return api.Operation{}, err
		}
		var s snapshotRecord
		if err = json.Unmarshal(sb, &s); err != nil {
			return api.Operation{}, err
		}
		if r.Action == "delete_snapshot" {
			s.Snapshot.Deleting = true
			if _, err = tx.Exec(
				"UPDATE snapshots SET body=? WHERE id=?",
				encode(s),
				s.Snapshot.ID,
			); err != nil {
				return api.Operation{}, err
			}
		} else {
			if !s.Snapshot.Published || s.Snapshot.Deleting {
				return api.Operation{}, api.Fail(
					api.Conflict,
					"snapshot unavailable or deleting",
				)
			}
			if _, err = tx.Exec(
				"INSERT INTO leases VALUES(?,?)",
				r.SnapshotID,
				o.Operation.ID,
			); err != nil {
				return api.Operation{}, err
			}
			o.Journal.Lease = true
		}
	}
	if r.Action == "snapshot" {
		o.Operation.SnapshotID = newID("snap-")
		dir := filepath.Join(
			e.opts.StateDir,
			"snapshots",
			o.Operation.SnapshotID,
		)
		s := snapshotRecord{
			Snapshot: api.Snapshot{
				ID:         o.Operation.SnapshotID,
				SourceVMID: v.VM.ID,
				Config:     v.VM.Config,
				CreatedAt:  time.Now().UTC(),
			},
			Dir: dir,
		}
		// CH output and the independent publication may both occupy full space.
		bytes := (2*v.VM.Config.Shape.MemoryMiB + v.VM.Config.Shape.DiskMiB + 224) << 20
		if err = e.reserve(tx, s.Snapshot.ID, bytes, 0, 0); err != nil {
			return api.Operation{}, err
		}
		if _, err = tx.Exec(
			"INSERT INTO snapshots VALUES(?,?)",
			s.Snapshot.ID,
			encode(s),
		); err != nil {
			return api.Operation{}, err
		}
	}
	if _, err = tx.Exec(
		"INSERT INTO operations VALUES(?,?,?,?,?)",
		o.Operation.ID,
		r.Key,
		hash,
		o.Operation.Status,
		encode(o),
	); err != nil {
		return api.Operation{}, err
	}
	if err = tx.Commit(); err != nil {
		return api.Operation{}, err
	}
	if err = e.checkpoint(); err != nil {
		return api.Operation{}, err
	}
	e.dispatch(o.Operation.ID)
	return o.Operation, nil
}

func (e *Engine) dispatch(id string) {
	e.mu.Lock()
	if e.active[id] || e.closing {
		e.mu.Unlock()
		return
	}
	e.active[id] = true
	e.wg.Add(1)
	e.mu.Unlock()
	go func() {
		defer e.wg.Done()
		defer func() { e.mu.Lock(); delete(e.active, id); e.mu.Unlock() }()
		e.execute(id)
	}()
}

func (e *Engine) GetOperation(
	ctx context.Context,
	id string,
) (api.Operation, error) {
	o, err := e.loadOp(ctx, id)
	return o.Operation, err
}

func (e *Engine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	var id string
	err := e.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE key=?", key).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return api.Operation{}, api.Fail(
			api.NotFound,
			"idempotency key not found",
		)
	}
	if err != nil {
		return api.Operation{}, err
	}
	return e.GetOperation(ctx, id)
}

func (e *Engine) GetVM(ctx context.Context, id string) (api.VM, error) {
	v, err := e.loadVM(ctx, id)
	return v.VM, err
}

func (e *Engine) GetSnapshot(
	ctx context.Context,
	id string,
) (api.Snapshot, error) {
	s, err := e.loadSnapshot(ctx, id)
	return s.Snapshot, err
}

func (e *Engine) ListVMs(ctx context.Context) ([]api.VM, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT body FROM vms ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []api.VM{}
	for rows.Next() {
		var b []byte
		var v vmRecord
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &v); err != nil {
			return nil, err
		}
		out = append(out, v.VM)
	}
	return out, rows.Err()
}

func (e *Engine) ListSnapshots(ctx context.Context) ([]api.Snapshot, error) {
	rows, err := e.db.QueryContext(
		ctx,
		"SELECT body FROM snapshots ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []api.Snapshot{}
	for rows.Next() {
		var b []byte
		var s snapshotRecord
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &s); err != nil {
			return nil, err
		}
		out = append(out, s.Snapshot)
	}
	return out, rows.Err()
}

func cpuIdentity() string {
	b, _ := os.ReadFile("/proc/cpuinfo")
	first, _, _ := strings.Cut(string(b), "\n\n")
	var fields []string
	for line := range strings.SplitSeq(first, "\n") {
		key, _, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(key) {
		case "vendor_id",
			"cpu family",
			"model",
			"stepping",
			"microcode",
			"flags",
			"Features",
			"CPU implementer",
			"CPU architecture",
			"CPU variant",
			"CPU part",
			"CPU revision":
			fields = append(fields, strings.TrimSpace(line))
		}
	}
	if len(fields) > 0 {
		return strings.Join(fields, "\n")
	}
	return fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH)
}
