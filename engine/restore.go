package engine

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) restore(o *opRecord, v *vmRecord) error {
	if o.Journal.RestoreBlocked {
		return api.Fail(
			api.ManualRecovery,
			"restore materials blocked; explicit delete required",
		)
	}
	var s snapshotRecord
	var m Manifest
	var err error
	if o.Journal.Lease {
		s, err = e.loadSnapshot(e.ctx, o.Operation.Request.SnapshotID)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(s.Snapshot.Manifest, &m); err != nil {
			return err
		}
	} else if o.Journal.Manifest != nil {
		m = *o.Journal.Manifest
	} else {
		return api.Fail(
			api.ManualRecovery,
			"restore manifest missing after dependency transfer",
		)
	}
	if m.Format != 1 || m.Backend != e.opts.Backend.Compatibility() ||
		m.Architecture != runtime.GOARCH ||
		m.CPU != cpuIdentity() ||
		m.MemoryMode != "CopyOnWrite" || m.PublicationMode != "IndependentInodes" {
		return api.Fail(
			api.Incompatible,
			"snapshot backend, CPU, format, COW or independent publication compatibility mismatch; legacy materials require manual recovery",
		)
	}
	if !reflect.DeepEqual(v.VM.Config.Shape, m.Config.Shape) ||
		v.VM.Config.Image != m.Config.Image {
		return api.Fail(
			api.Incompatible,
			"target shape/image must match snapshot",
		)
	}
	if err = e.checkImagePublication(v.Resource, m.Image); err != nil {
		return err
	}
	if o.Journal.Lease {
		if err = verifyFiles(s.Dir, m.Files); err != nil {
			return api.Fail(api.Incompatible, "snapshot integrity: %v", err)
		}
	}
	if o.Journal.Resource == nil {
		obs, x := e.observe(v)
		if x != nil {
			return x
		}
		if obs.Alive || v.VM.Phase == "running" {
			return api.Fail(api.Conflict, "restore requires a stopped target")
		}
		if err = e.opts.Backend.Network(e.ctx, v.Resource, false); err != nil {
			return err
		}
		if err = e.opts.Backend.Check(e.ctx, v.VM.Config); err != nil {
			return err
		}
		e.admission.Lock()
		tx, x := e.db.Begin()
		if x == nil {
			x = e.reserve(
				tx,
				v.VM.ID+":"+o.Operation.ID,
				(v.VM.Config.Shape.MemoryMiB+v.VM.Config.Shape.DiskMiB+64)<<20,
				0,
				0,
			)
			if x == nil {
				x = tx.Commit()
			} else {
				x = errors.Join(x, tx.Rollback())
			}
		}
		e.admission.Unlock()
		if x != nil {
			return x
		}
		old := v.Resource
		r := old
		r.Instance = newID("run-")
		r.RunDir = filepath.Join(r.Dir, "runs", r.Instance)
		r.RestoreDir = filepath.Join(
			r.Dir,
			"versions",
			o.Operation.ID,
			"restore",
		)
		r.Disk = filepath.Join(r.Dir, "versions", o.Operation.ID, "disk.raw")
		o.Journal.Old = &old
		o.Journal.Resource = &r
		o.Journal.Manifest = &m
		if err = e.saveOp(o, "restore_prepare_intent"); err != nil {
			return err
		}
	}
	r := *o.Journal.Resource
	if o.Journal.Lease {
		if err = e.prepareRestoreMaterials(o, s, m); err != nil {
			return err
		}
		if err = e.opts.Backend.Prepare(e.ctx, r); err != nil {
			return err
		}
		if err = e.verifyRestoreMaterials(o, m, true); err != nil {
			return err
		}
		backing := filepath.Join(r.RestoreDir, "memory-ranges")
		// The immutable hardlink and private copies replace the source-object lease.
		o.Journal.Lease = false
		o.Operation.Step = "restore_resources_ready"
		e.admission.Lock()
		tx, x := e.db.Begin()
		if x == nil {
			_, x = tx.Exec(
				"INSERT OR IGNORE INTO backing VALUES(?,?,?)",
				v.VM.ID,
				backing,
				m.Files["ch/memory-ranges"],
			)
			if x == nil {
				_, x = tx.Exec(
					"DELETE FROM leases WHERE operation=?",
					o.Operation.ID,
				)
			}
			if x == nil {
				_, x = tx.Exec(
					"UPDATE operations SET body=? WHERE id=?",
					encode(o),
					o.Operation.ID,
				)
			}
			if x == nil {
				x = tx.Commit()
			} else {
				x = errors.Join(x, tx.Rollback())
			}
		}
		e.admission.Unlock()
		if x != nil {
			o.Journal.Lease = true
			return x
		}
		if err = e.saveOp(o, "restore_resources_ready"); err != nil {
			return err
		}
	}
	obs, err := e.opts.Backend.Inspect(e.ctx, r)
	if err != nil {
		return err
	}
	if obs.Alive && o.Operation.Step == "restore_resources_ready" {
		return api.Fail(
			api.ManualRecovery,
			"restore candidate in use without launch intent",
		)
	}
	// A live instance may already have modified its private disk after launch.
	// Never compare those legitimate writes to the delivery digest or recopy it.
	if err = e.verifyRestoreMaterials(
		o,
		m,
		!obs.Alive &&
			(o.Operation.Step == "restore_resources_ready" || o.Operation.Step == "restore_launch_intent"),
	); err != nil {
		return err
	}
	v.Resource = r
	v.VM.Instance = r.Instance
	v.VM.VSockPath = filepath.Join(r.RunDir, "io", "vsock")
	v.VM.NetworkEnabled = false
	v.VM.NetworkActual = false
	v.VM.NetworkPolicy = nil
	v.VM.NetworkPolicyInstance = ""
	v.VM.AgentReady = false
	if v.VM.Phase == "creating" {
		v.VM.Phase = "created"
	}
	if err = e.saveVM(*v); err != nil {
		return err
	}
	if o.Operation.Step == "restore_resources_ready" {
		if err = e.saveOp(o, "restore_launch_intent"); err != nil {
			return err
		}
	}
	return e.start(o, v, true)
}
