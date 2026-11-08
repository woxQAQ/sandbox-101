package engine

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) execute(id string) {
	o, err := e.loadOp(e.ctx, id)
	if err != nil || terminal(o.Operation.Status) {
		return
	}
	lockID := o.Operation.VMID
	if lockID == "" {
		lockID = "snapshot:" + o.Operation.SnapshotID
	}
	lock := e.vmLock(lockID)
	lock.Lock()
	defer lock.Unlock()
	o, err = e.loadOp(e.ctx, id)
	if err != nil || terminal(o.Operation.Status) {
		return
	}
	o.Operation.Status = "running"
	if err = e.saveOp(&o, o.Operation.Step); err != nil {
		return
	}
	if o.Operation.Request.Action == "delete_snapshot" {
		err = e.deleteSnapshot(&o)
	} else {
		var v vmRecord
		v, err = e.loadVM(e.ctx, o.Operation.VMID)
		if err == nil {
			if v.VM.OperationID != id {
				err = api.Fail(api.Conflict, "operation no longer owns VM")
			} else if want := o.Operation.Request.ExpectedInstance; want != "" && want != v.VM.Instance && !ownsInstanceCandidate(o, v) {
				err = api.Fail(api.Conflict, "running instance changed")
			} else if action := o.Operation.Request.Action; (action == "network" || action == "network_policy") && (o.Operation.Request.ExpectedInstance == "" || v.Resource.Instance != v.VM.Instance) {
				err = api.Fail(
					api.Conflict,
					"network operation lacks a matching resource instance",
				)
			} else {
				err = e.perform(&o, &v)
			}
		} else if a, ok := errors.AsType[*api.Error](err); ok && a.Code == api.NotFound && o.Operation.Request.Action == "delete" && o.Journal.BlockedRestoreID != "" {
			// The cleanup transaction can commit before the delete response does.
			// Only the original restore's durable cleanup receipt proves success.
			blocked, x := e.loadOp(e.ctx, o.Journal.BlockedRestoreID)
			if x == nil && blocked.Operation.VMID == o.Operation.VMID &&
				blocked.Operation.Status == "failed" &&
				blocked.Journal.RestoreBlocked &&
				blocked.Journal.RestoreCleaned &&
				!blocked.Journal.Lease {
				err = nil
			}
		}
	}
	if errors.Is(err, ErrInterrupted) || e.ctx.Err() != nil {
		return
	}
	_ = e.finish(&o, err)
}

// ownsInstanceCandidate permits an accepted start/restore to continue its own
// successor to the admission instance. This metadata fence does not replace
// backend Inspect's process identity checks or restore's delivery proof.
func ownsInstanceCandidate(o opRecord, v vmRecord) bool {
	action := o.Operation.Request.Action
	if action != "start" && action != "restore" {
		return false
	}
	r := o.Journal.Resource
	return r != nil && r.Instance != "" &&
		v.VM.OperationID == o.Operation.ID &&
		o.Operation.VMID == v.VM.ID && o.Operation.Request.VMID == v.VM.ID &&
		r.VMID == v.VM.ID && v.VM.Instance == r.Instance &&
		reflect.DeepEqual(v.Resource, *r)
}

func (e *Engine) perform(o *opRecord, v *vmRecord) error {
	switch o.Operation.Request.Action {
	case "create":
		if err := e.checkImagePublication(v.Resource, nil); err != nil {
			return err
		}
		if err := e.opts.Backend.Check(e.ctx, v.VM.Config); err != nil {
			return err
		}
		if err := e.saveOp(o, "prepare_intent"); err != nil {
			return err
		}
		if err := e.opts.Backend.Prepare(e.ctx, v.Resource); err != nil {
			return err
		}
		v.VM.Phase = "created"
		v.VM.Verified = true
		v.VM.BlockedReason = ""
		if err := e.saveVM(*v); err != nil {
			return err
		}
		return e.saveOp(o, "prepared")
	case "start":
		return e.start(o, v, false)
	case "stop":
		return e.stop(o, v)
	case "delete":
		if o.Journal.BlockedRestoreID != "" {
			return e.deleteBlockedRestore(o, v)
		}
		if err := e.stop(o, v); err != nil {
			return err
		}
		if err := e.saveOp(o, "remove_intent"); err != nil {
			return err
		}
		if err := e.opts.Backend.Remove(e.ctx, v.Resource); err != nil {
			return err
		}
		if err := os.RemoveAll(v.Resource.Dir); err != nil {
			return err
		}
		if err := syncDir(filepath.Dir(v.Resource.Dir)); err != nil {
			return err
		}
		e.admission.Lock()
		defer e.admission.Unlock()
		tx, err := e.db.Begin()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err = tx.Exec(
			"DELETE FROM backing WHERE vm=?",
			v.VM.ID,
		); err != nil {
			return err
		}
		if _, err = tx.Exec(
			"DELETE FROM reservations WHERE owner=? OR owner LIKE ?",
			v.VM.ID,
			v.VM.ID+":%",
		); err != nil {
			return err
		}
		if _, err = tx.Exec("DELETE FROM vms WHERE id=?", v.VM.ID); err != nil {
			return err
		}
		return tx.Commit()
	case "network":
		return e.setNetwork(o, v)
	case "network_policy":
		return e.installNetworkPolicy(o, v)
	case "snapshot":
		return e.snapshot(o, v)
	case "restore", "clone":
		return e.restore(o, v)
	}
	return api.Fail(api.Invalid, "unknown action")
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
