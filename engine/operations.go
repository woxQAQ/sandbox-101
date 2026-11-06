package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"time"

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

func (e *Engine) finish(o *opRecord, cause error) error {
	if e.opts.Fault != nil {
		if err := e.opts.Fault("finish_candidate_ready"); err != nil {
			return err
		}
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	// Reload first: a committed terminal response is immutable, including when
	// checkpointing its commit failed. JSON cloning isolates every mutable member.
	durable, err := e.loadOp(e.ctx, o.Operation.ID)
	if err != nil {
		return err
	}
	if terminal(durable.Operation.Status) {
		*o = durable
		return nil
	}
	var next opRecord
	if err = json.Unmarshal(encode(o), &next); err != nil {
		return err
	}
	if !reflect.DeepEqual(durable.Operation.Request, next.Operation.Request) ||
		durable.Operation.ID != next.Operation.ID || durable.Operation.VMID != next.Operation.VMID ||
		!reflect.DeepEqual(durable.Journal, next.Journal) {
		return api.Fail(api.Conflict, "operation completion intent changed")
	}
	if cause != nil &&
		(next.Operation.Request.Action == "restore" || next.Operation.Request.Action == "clone") &&
		(next.Journal.Resource != nil || !next.Journal.Lease) {
		// 010's failed-candidate fence precedes the public terminal receipt. Failure
		// to persist it leaves both the caller and durable operation nonterminal.
		next.Journal.RestoreBlocked = true
		if err = requireCompletionWrite(e.db.Exec(
			"UPDATE operations SET body=? WHERE id=?",
			encode(next),
			next.Operation.ID,
		)); err != nil {
			return err
		}
	}
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var raw []byte
	if err = tx.QueryRow("SELECT body FROM operations WHERE id=?", next.Operation.ID).
		Scan(&raw); err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &durable); err != nil {
		return err
	}
	if terminal(durable.Operation.Status) {
		*o = durable
		return nil
	}
	if !reflect.DeepEqual(durable.Operation.Request, next.Operation.Request) ||
		!reflect.DeepEqual(durable.Journal, next.Journal) ||
		durable.Operation.VMID != next.Operation.VMID {
		return api.Fail(api.Conflict, "durable completion intent changed")
	}
	generation := cause == nil &&
		(next.Operation.Request.Action == "start" || next.Operation.Request.Action == "restore" || next.Operation.Request.Action == "clone")
	next.Operation.ResultInstance = ""
	if next.Operation.VMID != "" {
		queryErr := tx.QueryRow("SELECT body FROM vms WHERE id=?", next.Operation.VMID).
			Scan(&raw)
		if generation && queryErr != nil {
			return queryErr
		}
		if queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
			return queryErr
		}
		if queryErr == nil {
			var v vmRecord
			if err = json.Unmarshal(raw, &v); err != nil {
				return err
			}
			if generation {
				r := next.Journal.Resource
				req := next.Operation.Request
				if r == nil || r.Instance == "" ||
					r.VMID != v.VM.ID ||
					next.Operation.VMID != v.VM.ID ||
					v.VM.OperationID != next.Operation.ID ||
					v.VM.Instance != r.Instance ||
					!reflect.DeepEqual(v.Resource, *r) ||
					v.VM.Phase != "running" ||
					!v.VM.Verified ||
					!v.VM.AgentReady ||
					(req.Action == "clone" && req.VMID != "") ||
					(req.Action != "clone" && req.VMID != v.VM.ID) {
					return api.Fail(
						api.Conflict,
						"generation completion lacks full owned candidate proof",
					)
				}
				next.Operation.ResultInstance = r.Instance
			}
			if v.VM.OperationID == next.Operation.ID {
				if !next.Journal.ResumeRequired &&
					!next.Journal.RestoreBlocked &&
					next.Journal.BlockedRestoreID == "" {
					v.VM.OperationID = ""
				}
				if cause != nil {
					v.VM.BlockedReason = cause.Error()
				}
				if err = requireCompletionWrite(tx.Exec(
					"UPDATE vms SET body=? WHERE id=?",
					encode(v),
					v.VM.ID,
				)); err != nil {
					return err
				}
			}
		}
	}
	if cause != nil {
		next.Operation.Status = "failed"
		next.Operation.Error = asAPI(cause)
	} else {
		next.Operation.Status = "succeeded"
		next.Operation.Step = "complete"
	}
	next.Operation.UpdatedAt = time.Now().UTC()
	if next.Journal.Lease && !next.Journal.RestoreBlocked {
		// Admission creates one lease per operation. A successful restore has
		// already transferred it to backing and cleared Journal.Lease.
		if err = requireCompletionWrite(tx.Exec(
			"DELETE FROM leases WHERE operation=? AND snapshot=?",
			next.Operation.ID,
			next.Operation.Request.SnapshotID,
		)); err != nil {
			return err
		}
		next.Journal.Lease = false
	}
	if err = requireCompletionWrite(tx.Exec(
		"UPDATE operations SET status=?,body=? WHERE id=?",
		next.Operation.Status,
		encode(next),
		next.Operation.ID,
	)); err != nil {
		return err
	}
	if e.opts.Fault != nil {
		if err = e.opts.Fault("finish_before_commit"); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*o = next
	return e.checkpoint()
}

// Every required completion write targets one existing primary-key row. A nil
// SQL error alone does not prove that the statement applied its intended effect.
func requireCompletionWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("completion write returned no result")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("completion write effect unavailable: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("completion write affected %d rows, expected 1", rows)
	}
	return nil
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

func (e *Engine) observe(v *vmRecord) (Observation, error) {
	obs, err := e.opts.Backend.Inspect(e.ctx, v.Resource)
	if err != nil {
		if obs.Network {
			v.VM.NetworkActual = true
		}
		return obs, e.failClosed(e.ctx, v, err)
	}
	v.VM.NetworkActual = obs.Network
	if obs.Network {
		if !v.VM.NetworkEnabled {
			return obs, e.failClosed(
				e.ctx,
				v,
				api.Fail(
					api.Conflict,
					"interfaces active without durable network intent",
				),
			)
		}
		if err = e.verifyNetworkPolicy(e.ctx, v); err != nil {
			return obs, e.failClosed(e.ctx, v, err)
		}
	}
	v.VM.Verified = true
	v.VM.PID = obs.PID
	v.VM.StartTime = obs.StartTime
	v.VM.NetworkActual = obs.Network
	if !obs.Alive {
		v.VM.AgentReady = false
		if v.VM.Phase == "running" {
			v.VM.Phase = "stopped"
		}
	}
	if err = e.saveVM(*v); err != nil {
		return obs, err
	}
	return obs, nil
}

func (e *Engine) checkImagePublication(
	r Resource,
	captured json.RawMessage,
) error {
	requires, ok := e.opts.Backend.(interface{ RequiresImageCatalog() bool })
	if !ok || !requires.RequiresImageCatalog() {
		return nil
	}
	var body string
	if err := e.db.QueryRow("SELECT value FROM meta WHERE key=?", "image:"+r.Config.Image).
		Scan(&body); err != nil {
		return api.Fail(
			api.Incompatible,
			"image has no published catalog record: %v",
			err,
		)
	}
	var published, disk ImageManifest
	if err := json.Unmarshal([]byte(body), &published); err != nil {
		return err
	}
	if err := readJSON(
		filepath.Join(r.ImageDir, "image.json"),
		&disk,
	); err != nil {
		return err
	}
	if !reflect.DeepEqual(published, disk) {
		return api.Fail(
			api.Incompatible,
			"image materialization differs from published catalog",
		)
	}
	if captured != nil {
		var saved ImageManifest
		if err := json.Unmarshal(captured, &saved); err != nil {
			return err
		}
		if !reflect.DeepEqual(published, saved) {
			return api.Fail(
				api.Incompatible,
				"snapshot image materialization differs from published catalog",
			)
		}
	}
	return published.Verify(r.ImageDir, r.Config.Image)
}

func (e *Engine) start(o *opRecord, v *vmRecord, restore bool) error {
	if err := e.checkImagePublication(v.Resource, nil); err != nil {
		return err
	}
	obs, err := e.observe(v)
	if err != nil {
		return err
	}
	if o.Journal.Resource == nil {
		if obs.Alive {
			return api.Fail(api.Conflict, "VM already has a live VMM")
		}
		if v.VM.Phase != "created" && v.VM.Phase != "stopped" {
			return api.Fail(api.Conflict, "VM resources are not ready")
		}
		if err = e.opts.Backend.Check(e.ctx, v.VM.Config); err != nil {
			return err
		}
		if err = e.opts.Backend.Network(e.ctx, v.Resource, false); err != nil {
			return err
		}
		// An installed policy never authorizes a different running instance.
		v.VM.NetworkEnabled = false
		v.VM.NetworkActual = false
		v.VM.NetworkPolicy = nil
		v.VM.NetworkPolicyInstance = ""
		v.Resource.Instance = newID("run-")
		v.Resource.RunDir = filepath.Join(
			v.Resource.Dir,
			"runs",
			v.Resource.Instance,
		)
		v.VM.Instance = v.Resource.Instance
		v.VM.VSockPath = filepath.Join(v.Resource.RunDir, "io", "vsock")
		v.VM.AgentReady = false
		o.Journal.Resource = &v.Resource
		if err = e.saveOp(o, "launch_intent"); err != nil {
			return err
		}
		if err = e.saveVM(*v); err != nil {
			return err
		}
	} else {
		v.Resource = *o.Journal.Resource
		v.VM.Instance = v.Resource.Instance
		v.VM.VSockPath = filepath.Join(v.Resource.RunDir, "io", "vsock")
		if err = e.saveVM(*v); err != nil {
			return err
		}
	}
	// An instance receipt/cgroup is authoritative even if the daemon died before
	// recording Launch's return value. The same intent never starts a second VMM.
	obs, err = e.observe(v)
	if err != nil {
		return err
	}
	if !obs.Alive {
		if o.Operation.Step != "launch_intent" &&
			o.Operation.Step != "restore_launch_intent" {
			return api.Fail(
				api.Unknown,
				"VMM exited during a previous attempt; use a new operation key after inspection",
			)
		}
		if err = e.opts.Backend.Network(e.ctx, v.Resource, false); err != nil {
			return err
		}
		if err = e.opts.Backend.Launch(e.ctx, v.Resource, restore); err != nil {
			return err
		}
		if err = e.saveOp(o, "launched"); err != nil {
			return err
		}
	}
	if err = e.opts.Backend.Boot(e.ctx, v.Resource, restore); err != nil {
		return err
	}
	if err = e.saveOp(o, "booted"); err != nil {
		return err
	}
	if err = e.opts.Backend.Agent(e.ctx, v.Resource, restore); err != nil {
		if !restore {
			netErr := e.opts.Backend.Network(e.ctx, v.Resource, false)
			stopErr := e.opts.Backend.Stop(e.ctx, v.Resource)
			after, inspectErr := e.opts.Backend.Inspect(e.ctx, v.Resource)
			if inspectErr == nil && !after.Alive {
				v.VM.Phase = "stopped"
				v.VM.PID = 0
				v.VM.StartTime = 0
				v.VM.NetworkActual = false
				v.VM.AgentReady = false
				_ = e.saveVM(*v)
			}
			return fmt.Errorf(
				"agent handshake: %w (network=%v stop=%v inspect=%v)",
				err,
				netErr,
				stopErr,
				inspectErr,
			)
		}
		_, _ = e.observe(v)
		return err
	}
	obs, err = e.observe(v)
	if err != nil {
		return err
	}
	if !obs.Alive {
		return api.Fail(api.Unknown, "VMM exited before completion")
	}
	if err = e.saveOp(o, "agent_ready"); err != nil {
		return err
	}
	v.VM.Phase = "running"
	v.VM.AgentReady = true
	v.VM.BlockedReason = ""
	v.VM.Verified = true
	return e.saveVM(*v)
}

func (e *Engine) stop(o *opRecord, v *vmRecord) error {
	if _, err := e.observe(v); err != nil {
		return err
	}
	if err := e.saveOp(o, "stop_intent"); err != nil {
		return err
	}
	// Always attempt process termination even when interface shutdown fails.
	netErr := e.opts.Backend.Network(e.ctx, v.Resource, false)
	if netErr == nil {
		v.VM.NetworkActual = false
	}
	stopErr := e.opts.Backend.Stop(e.ctx, v.Resource)
	obs, err := e.opts.Backend.Inspect(e.ctx, v.Resource)
	if err != nil {
		v.VM.Verified = false
		_ = e.saveVM(*v)
		return err
	}
	if obs.Alive {
		return api.Fail(api.ManualRecovery, "VMM exit unconfirmed: %v", stopErr)
	}
	v.VM.Phase = "stopped"
	v.VM.PID = 0
	v.VM.StartTime = 0
	v.VM.AgentReady = false
	v.VM.Verified = true
	if err = e.saveVM(*v); err != nil {
		return err
	}
	if netErr != nil {
		return netErr
	}
	return stopErr
}

func (e *Engine) snapshot(o *opRecord, v *vmRecord) error {
	s, err := e.loadSnapshot(e.ctx, o.Operation.SnapshotID)
	if err != nil {
		return err
	}
	if s.Snapshot.Published {
		return nil
	}
	if o.Journal.Resource == nil {
		r := v.Resource
		o.Journal.Resource = &r
	}
	r := *o.Journal.Resource
	capture := filepath.Join(r.Dir, "capture", o.Operation.ID)
	if o.Journal.CaptureBoundary == nil &&
		(o.Journal.Captured || o.Journal.ResumeRequired) {
		return e.captureFailure(
			o,
			r,
			api.Fail(
				api.Incompatible,
				"legacy capture journal requires manual recovery; original materials retained",
			),
		)
	}
	stage := filepath.Join(capture, "publication")
	// A replay cannot prove bytes copied before source resume reached storage.
	// Only this uninterrupted attempt may promote those bytes to a durable proof.
	capturedThisAttempt := !o.Journal.Captured
	if !o.Journal.Captured {
		if o.Journal.ResumeRequired {
			obs, x := e.opts.Backend.Inspect(e.ctx, r)
			if x != nil {
				return x
			}
			if obs.Alive && obs.Paused {
				if x = e.opts.Backend.Resume(e.ctx, r); x != nil {
					return x
				}
			}
			o.Journal.ResumeRequired = false
			if x = e.saveOp(o, "capture_interrupted"); x != nil {
				return x
			}
			return api.Fail(
				api.Unknown,
				"capture interrupted before independent disk completion; snapshot not published",
			)
		}
		obs, x := e.observe(v)
		if x != nil {
			return x
		}
		if !obs.Alive || v.VM.Phase != "running" || !v.VM.AgentReady {
			return api.Fail(
				api.Conflict,
				"snapshot requires a verified running VM",
			)
		}
		select {
		case e.capture <- struct{}{}:
		case <-e.ctx.Done():
			return e.ctx.Err()
		}
		defer func() { <-e.capture }()
		if o.Journal.CaptureBoundary == nil {
			o.Journal.CaptureBoundary, err = prepareCapture(r, o.Operation.ID)
			if err != nil {
				return err
			}
			if backend, ok := e.opts.Backend.(interface{ prepareCaptureWriter(Resource, string) error }); ok {
				if err = backend.prepareCaptureWriter(
					r,
					filepath.Join(capture, "ch"),
				); err != nil {
					return err
				}
			}
			if err = e.saveOp(o, "capture_prepared"); err != nil {
				return err
			}
		}
		o.Journal.ResumeRequired = true
		if err = e.saveOp(o, "pause_intent"); err != nil {
			return err
		}
		if err = e.opts.Backend.Pause(e.ctx, r); err != nil {
			return e.captureFailure(o, r, err)
		}
		if err = e.saveOp(o, "paused"); err != nil {
			return err
		}
		if err = e.opts.Backend.Capture(
			e.ctx,
			r,
			filepath.Join(capture, "ch"),
		); err != nil {
			return e.captureFailure(o, r, err)
		}
		if err = copyFile(
			r.Disk,
			filepath.Join(stage, "disk.raw"),
			0o600,
		); err != nil {
			return e.captureFailure(o, r, err)
		}
		o.Journal.Captured = true
		if err = e.saveOp(o, "captured"); err != nil {
			return err
		}
	}
	if o.Journal.ResumeRequired {
		obs, x := e.opts.Backend.Inspect(e.ctx, r)
		if x != nil {
			return x
		}
		if obs.Alive && obs.Paused {
			if err = e.opts.Backend.Resume(e.ctx, r); err != nil {
				return err
			}
		}
		o.Journal.ResumeRequired = false
		if err = e.saveOp(o, "source_resumed"); err != nil {
			return err
		}
	}
	// No artifact fsync, hashing or publication work is done while paused.
	if !capturedThisAttempt &&
		(!o.Journal.Materialized || len(o.Journal.CaptureFiles) != 4) {
		return api.Fail(
			api.Incompatible,
			"capture interrupted before durable independent materials; source resumed and materials retained for manual recovery",
		)
	}
	boundary := o.Journal.CaptureBoundary
	if boundary == nil || boundary.Path != capture {
		return api.Fail(
			api.Incompatible,
			"capture ownership journal missing or inconsistent",
		)
	}
	parent, err := boundary.open()
	if err != nil {
		return err
	}
	_ = parent.Close()
	if st, x := os.Lstat(s.Dir); x == nil {
		if !o.Journal.ArtifactsSynced || gcNode(st) != boundary.Publication {
			return api.Fail(
				api.Incompatible,
				"published directory identity unconfirmed",
			)
		}
		stage = s.Dir
	} else if !os.IsNotExist(x) {
		return x
	}
	stageInfo, err := os.Lstat(stage)
	if err != nil {
		return err
	}
	if gcNode(stageInfo) != boundary.Publication {
		return api.Fail(
			api.Incompatible,
			"publication staging identity changed",
		)
	}
	if !o.Journal.Materialized {
		if err = materializeCapture(
			boundary,
			v.VM.Config.Shape.MemoryMiB,
		); err != nil {
			return err
		}
		// This progress checkpoint is deliberately not a completion proof.
		if err = e.saveOp(o, "capture_copied"); err != nil {
			return err
		}
	}
	files, err := hashTree(stage)
	if err != nil {
		return api.Fail(
			api.Incompatible,
			"independent capture integrity: %v",
			err,
		)
	}
	for _, name := range []string{"ch/config.json", "ch/state.json", "ch/memory-ranges", "disk.raw"} {
		if _, ok := files[name]; !ok {
			return api.Fail(api.Incompatible, "capture missing %s", name)
		}
	}
	if len(files) != 4 {
		return api.Fail(
			api.Incompatible,
			"unexpected independent capture artifacts",
		)
	}
	if o.Journal.Materialized {
		// Never rehash altered replay input into a new authoritative manifest.
		if !reflect.DeepEqual(files, o.Journal.CaptureFiles) {
			return api.Fail(
				api.Incompatible,
				"durable independent capture checksum mismatch; materials retained",
			)
		}
	} else {
		// Include disk.raw, all independent CH data, and every directory entry.
		// The completion flag and fixed checksums become durable only afterwards.
		if err = syncTree(stage); err != nil {
			return err
		}
		o.Journal.CaptureFiles = files
		o.Journal.Materialized = true
		if err = e.saveOp(o, "capture_materialized"); err != nil {
			return err
		}
	}
	image, err := os.ReadFile(filepath.Join(r.ImageDir, "image.json"))
	if err != nil {
		return err
	}
	manifest := Manifest{
		Format:          1,
		Backend:         e.opts.Backend.Compatibility(),
		Architecture:    runtime.GOARCH,
		CPU:             cpuIdentity(),
		Config:          v.VM.Config,
		MemoryMode:      "CopyOnWrite",
		PublicationMode: "IndependentInodes",
		Files:           files,
		Image:           image,
	}
	if err = writeJSON(
		filepath.Join(stage, "manifest.json"),
		manifest,
	); err != nil {
		return err
	}
	if err = sealTree(stage); err != nil {
		return err
	}
	if err = syncTree(stage); err != nil {
		return err
	}
	o.Journal.ArtifactsSynced = true
	if err = e.saveOp(o, "artifacts_synced"); err != nil {
		return err
	}
	if stage != s.Dir {
		if err = os.Rename(stage, s.Dir); err != nil {
			return err
		}
		if err = syncDir(filepath.Dir(stage)); err != nil {
			return err
		}
		if err = syncDir(filepath.Dir(s.Dir)); err != nil {
			return err
		}
	}
	if err = e.saveOp(o, "publish_intent"); err != nil {
		return err
	}
	s.Snapshot.Manifest = encode(manifest)
	s.Snapshot.Published = true
	return e.saveSnapshot(s)
}

func (e *Engine) captureFailure(o *opRecord, r Resource, cause error) error {
	// Pause can partially succeed. Resume is attempted after inspecting actual state.
	obs, err := e.opts.Backend.Inspect(e.ctx, r)
	if err != nil {
		return fmt.Errorf("capture: %v; inspect: %w", cause, err)
	}
	if obs.Alive && obs.Paused {
		if err = e.opts.Backend.Resume(e.ctx, r); err != nil {
			return fmt.Errorf("capture: %v; resume: %w", cause, err)
		}
	}
	o.Journal.ResumeRequired = false
	if err = e.saveOp(o, "capture_failed_resumed"); err != nil {
		return err
	}
	return cause
}

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

func (e *Engine) deleteSnapshot(o *opRecord) error {
	s, err := e.loadSnapshot(e.ctx, o.Operation.SnapshotID)
	if err != nil {
		var a *api.Error
		if errors.As(err, &a) && a.Code == api.NotFound {
			return nil
		}
		return err
	}
	var n int
	if err = e.db.QueryRow("SELECT COUNT(*) FROM leases WHERE snapshot=?", s.Snapshot.ID).
		Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return api.Fail(
			api.Conflict,
			"snapshot deletion waits for %d durable readers; retry with a new key",
			n,
		)
	}
	if err = e.saveOp(o, "snapshot_remove_intent"); err != nil {
		return err
	}
	// Failed unpublished capture files still belong to their source operation.
	rows, err := e.db.Query("SELECT body FROM operations")
	if err != nil {
		return err
	}
	var captures []string
	for rows.Next() {
		var b []byte
		var old opRecord
		if err = rows.Scan(&b); err != nil {
			break
		}
		if err = json.Unmarshal(b, &old); err != nil {
			break
		}
		if old.Operation.SnapshotID == s.Snapshot.ID &&
			old.Operation.Request.Action == "snapshot" {
			if !terminal(old.Operation.Status) || old.Journal.ResumeRequired {
				err = api.Fail(
					api.Conflict,
					"snapshot capture still unresolved",
				)
				break
			}
			if old.Journal.Resource != nil {
				path := filepath.Join(
					old.Journal.Resource.Dir,
					"capture",
					old.Operation.ID,
				)
				if _, x := os.Lstat(path); x == nil {
					boundary := old.Journal.CaptureBoundary
					if boundary == nil || boundary.Path != path {
						err = api.Fail(
							api.Incompatible,
							"legacy capture deletion requires manual recovery",
						)
						break
					}
					parent, x := boundary.open()
					if x != nil {
						err = x
						break
					}
					_ = parent.Close()
					if st, x := os.Lstat(
						filepath.Join(path, "publication"),
					); x == nil &&
						gcNode(st) != boundary.Publication {
						err = api.Fail(
							api.Incompatible,
							"staging identity changed",
						)
						break
					} else if x != nil &&
						!os.IsNotExist(x) {
						err = x
						break
					}
				} else if !os.IsNotExist(x) {
					err = x
					break
				}
				if st, x := os.Lstat(s.Dir); x == nil {
					if old.Journal.CaptureBoundary == nil ||
						gcNode(st) != old.Journal.CaptureBoundary.Publication {
						err = api.Fail(
							api.Incompatible,
							"snapshot deletion identity unconfirmed",
						)
						break
					}
				} else if !os.IsNotExist(x) {
					err = x
					break
				}
				captures = append(
					captures,
					filepath.Join(
						old.Journal.Resource.Dir,
						"capture",
						old.Operation.ID,
					),
				)
			}
		}
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	if err = os.RemoveAll(s.Dir); err != nil {
		return err
	}
	if err = syncDir(filepath.Dir(s.Dir)); err != nil {
		return err
	}
	for _, p := range captures {
		if err = os.RemoveAll(p); err != nil {
			return err
		}
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.Exec(
		"DELETE FROM snapshots WHERE id=?",
		s.Snapshot.ID,
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		"DELETE FROM reservations WHERE owner=?",
		s.Snapshot.ID,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// Reconcile verifies ownership before changing phase or touching network state.
// The Linux backend's pidfds reduce event latency; this periodic pass is the
// correctness path and also reattaches to processes after a daemon restart.
func (e *Engine) Reconcile(ctx context.Context) error {
	vms, err := e.ListVMs(ctx)
	if err != nil {
		return err
	}
	for _, vm := range vms {
		l := e.vmLock(vm.ID)
		if !l.TryLock() {
			continue
		}
		v, x := e.loadVM(ctx, vm.ID)
		if x != nil {
			l.Unlock()
			continue
		}
		if x = e.restoreVMFence(&v); x != nil {
			l.Unlock()
			return x
		}
		obs, x := e.opts.Backend.Inspect(ctx, v.Resource)
		if x != nil {
			if obs.Network {
				v.VM.NetworkActual = true
			}
			_ = e.failClosed(ctx, &v, x)
			l.Unlock()
			continue
		}
		v.VM.NetworkActual = obs.Network
		// Check direct rules even when only one interface is up. Unknown, stale,
		// or uncommitted policy reality always triggers interface shutdown.
		if obs.Network {
			if !v.VM.NetworkEnabled {
				x = api.Fail(
					api.Conflict,
					"interfaces active without durable network intent",
				)
			} else {
				x = e.verifyNetworkPolicy(ctx, &v)
			}
			if x != nil {
				_ = e.failClosed(ctx, &v, x)
				l.Unlock()
				continue
			}
		}
		v.VM.Verified = true
		v.VM.PID = obs.PID
		v.VM.StartTime = obs.StartTime
		v.VM.NetworkActual = obs.Network
		if !obs.Alive {
			if x = e.opts.Backend.Network(ctx, v.Resource, false); x != nil {
				v.VM.BlockedReason = x.Error()
			} else {
				v.VM.NetworkActual = false
			}
			if v.VM.Phase == "running" {
				v.VM.Phase = "stopped"
			}
			v.VM.AgentReady = false
		}
		if v.VM.OperationID != "" {
			o, x := e.loadOp(ctx, v.VM.OperationID)
			if x == nil && terminal(o.Operation.Status) &&
				!o.Journal.RestoreBlocked &&
				o.Journal.BlockedRestoreID == "" &&
				o.Journal.ResumeRequired {
				if !obs.Alive || !obs.Paused {
					x = nil
				} else {
					x = e.opts.Backend.Resume(ctx, v.Resource)
				}
				if x == nil {
					o.Journal.ResumeRequired = false
					_, _ = e.db.Exec(
						"UPDATE operations SET body=? WHERE id=?",
						encode(o),
						o.Operation.ID,
					)
					v.VM.OperationID = ""
				} else {
					v.VM.BlockedReason = x.Error()
				}
			}
		}
		_ = e.saveVM(v)
		l.Unlock()
	}
	return nil
}
