package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

type Manifest struct {
	Format          int               `json:"format"`
	Backend         string            `json:"backend"`
	Architecture    string            `json:"architecture"`
	CPU             string            `json:"cpu"`
	Config          api.Config        `json:"config"`
	MemoryMode      string            `json:"memory_mode"`
	PublicationMode string            `json:"publication_mode"`
	Files           map[string]string `json:"files"`
	Image           json.RawMessage   `json:"image"`
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
