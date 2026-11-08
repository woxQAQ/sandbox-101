package engine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

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
