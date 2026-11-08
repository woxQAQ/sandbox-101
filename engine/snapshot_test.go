package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestSnapshotRejectsUntrustedArtifactsAndResumes(t *testing.T) {
	b := newTestBackend()
	b.captureDamage = "symlink"
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "capture", Action: "snapshot", VMID: vm.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitStatus(t, e, op.ID)
	snap, err := e.GetSnapshot(context.Background(), op.SnapshotID)
	if err != nil || final.Status != "failed" || snap.Published {
		t.Fatalf("invalid capture published: %+v %+v %v", final, snap, err)
	}
	v, err := e.loadVM(context.Background(), vm.VMID)
	if err != nil {
		t.Fatal(err)
	}
	b.mu.Lock()
	paused := b.states[v.Resource.Instance].Paused
	resumes := b.resumes
	b.mu.Unlock()
	if paused || resumes != 1 {
		t.Fatalf(
			"failed capture did not resume: paused=%v resumes=%d",
			paused,
			resumes,
		)
	}
}

func TestSnapshotCrashRejectsUnprovenPublication(t *testing.T) {
	for _, damage := range []string{"legacy-journal", "staging-inode", "legacy-proof"} {
		t.Run(damage, func(t *testing.T) {
			b := newTestBackend()
			dir := t.TempDir()
			var hit atomic.Bool
			step := "capture_materialized"
			if damage == "legacy-journal" {
				step = "captured"
			}
			e := openTest(t, dir, b, true, func(s string) error {
				if s == step && hit.CompareAndSwap(false, true) {
					return ErrInterrupted
				}
				return nil
			})
			vm := wait(
				t,
				e,
				api.Request{Key: "create", Action: "create", Config: config()},
			)
			wait(
				t,
				e,
				api.Request{Key: "start", Action: "start", VMID: vm.VMID},
			)
			op, err := e.Submit(
				context.Background(),
				api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
			)
			if err != nil {
				t.Fatal(err)
			}
			awaitStep(t, e, op.ID, step)
			lock := e.vmLock(vm.VMID)
			lock.Lock()
			o, err := e.loadOp(context.Background(), op.ID)
			lock.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			path := o.Journal.CaptureBoundary.Path
			switch damage {
			case "legacy-journal":
				o.Journal.CaptureBoundary = nil
				if err = e.saveOp(&o, "captured"); err != nil {
					t.Fatal(err)
				}
			case "legacy-proof":
				o.Journal.CaptureFiles = nil
				if err = e.saveOp(&o, step); err != nil {
					t.Fatal(err)
				}
			default:
				stage := filepath.Join(path, "publication")
				if err = os.Rename(
					stage,
					filepath.Join(t.TempDir(), "retained-publication"),
				); err != nil {
					t.Fatal(err)
				}
				if err = os.Mkdir(stage, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			e.Close()
			e = openTest(t, dir, b, false, nil)
			defer e.Close()
			final := awaitStatus(t, e, op.ID)
			s, err := e.GetSnapshot(context.Background(), op.SnapshotID)
			if err != nil || final.Status != "failed" || s.Published {
				t.Fatalf(
					"unproven capture published: %+v %+v %v",
					final,
					s,
					err,
				)
			}
			if _, err = os.Stat(path); err != nil {
				t.Fatal("recovery destroyed diagnostic capture", err)
			}
			v, _ := e.loadVM(context.Background(), vm.VMID)
			b.mu.Lock()
			paused := b.states[v.Resource.Instance].Paused
			launches := b.launches
			b.mu.Unlock()
			if paused || launches != 1 {
				t.Fatalf(
					"unsafe recovery: paused=%v launches=%d",
					paused,
					launches,
				)
			}
		})
	}
}

// Filesystem damage models the persisted names with lost/changed data that a
// process-only close/open cannot exercise. Recovery must never bless those bytes
// by calculating a new publication manifest.
func TestSnapshotCrashRejectsLostOrDamagedMaterials(t *testing.T) {
	for _, step := range []string{"captured", "source_resumed", "capture_copied", "capture_materialized", "artifacts_synced", "publish_intent"} {
		files := []string{"disk.raw"}
		if step != "captured" && step != "source_resumed" {
			files = append(files, "ch/memory-ranges")
		}
		for _, file := range files {
			for _, damage := range []string{"truncate", "zero", "lost"} {
				t.Run(step+"/"+file+"/"+damage, func(t *testing.T) {
					b := newTestBackend()
					dir := t.TempDir()
					var hit atomic.Bool
					e := openTest(t, dir, b, true, func(s string) error {
						if s == step && hit.CompareAndSwap(false, true) {
							return ErrInterrupted
						}
						return nil
					})
					vm := wait(
						t,
						e,
						api.Request{
							Key:    "create",
							Action: "create",
							Config: config(),
						},
					)
					wait(
						t,
						e,
						api.Request{
							Key:    "start",
							Action: "start",
							VMID:   vm.VMID,
						},
					)
					op, err := e.Submit(
						context.Background(),
						api.Request{
							Key:    "snapshot",
							Action: "snapshot",
							VMID:   vm.VMID,
						},
					)
					if err != nil {
						t.Fatal(err)
					}
					awaitStep(t, e, op.ID, step)
					lock := e.vmLock(vm.VMID)
					lock.Lock()
					o, err := e.loadOp(context.Background(), op.ID)
					lock.Unlock()
					if err != nil {
						t.Fatal(err)
					}
					capture := o.Journal.CaptureBoundary.Path
					stage := filepath.Join(capture, "publication")
					if step == "publish_intent" {
						s, x := e.loadSnapshot(
							context.Background(),
							op.SnapshotID,
						)
						if x != nil {
							t.Fatal(x)
						}
						stage = s.Dir
					}
					if err = e.Close(); err != nil {
						t.Fatal(err)
					}
					path := filepath.Join(stage, file)
					st, err := os.Stat(path)
					if err != nil {
						t.Fatal(err)
					}
					if err = os.Chmod(path, 0o600); err != nil {
						t.Fatal(err)
					}
					switch damage {
					case "truncate":
						err = os.Truncate(path, 0)
					case "zero":
						err = os.WriteFile(path, make([]byte, st.Size()), 0o600)
					case "lost":
						err = os.Remove(path)
					}
					if err != nil {
						t.Fatal(err)
					}
					e = openTest(t, dir, b, false, nil)
					defer e.Close()
					final := awaitStatus(t, e, op.ID)
					s, err := e.GetSnapshot(context.Background(), op.SnapshotID)
					if err != nil || final.Status != "failed" || s.Published {
						t.Fatalf(
							"damaged capture published: status=%s published=%v error=%v get=%v",
							final.Status,
							s.Published,
							final.Error,
							err,
						)
					}
					if _, err = os.Stat(capture); err != nil {
						t.Fatal("diagnostic capture removed", err)
					}
					if damage == "lost" {
						if _, err = os.Stat(path); !os.IsNotExist(err) {
							t.Fatal(
								"lost file rebuilt from current source",
								err,
							)
						}
					}
					v, err := e.loadVM(context.Background(), vm.VMID)
					if err != nil {
						t.Fatal(err)
					}
					b.mu.Lock()
					paused := b.states[v.Resource.Instance].Paused
					launches, resumes, captures := b.launches, b.resumes, b.captures
					b.mu.Unlock()
					if paused || launches != 1 || resumes != 1 ||
						captures != 1 {
						t.Fatalf(
							"unsafe source recovery: paused=%v launches=%d resumes=%d captures=%d",
							paused,
							launches,
							resumes,
							captures,
						)
					}
				})
			}
		}
	}
}

func TestSnapshotCrashBoundaries(t *testing.T) {
	for _, step := range []string{"capture_prepared", "pause_intent", "paused", "captured", "source_resumed", "capture_copied", "capture_materialized", "artifacts_synced", "publish_intent"} {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			b := newTestBackend()
			var hit atomic.Bool
			e := openTest(t, dir, b, true, func(s string) error {
				if s == step && hit.CompareAndSwap(false, true) {
					return ErrInterrupted
				}
				return nil
			})
			a := wait(
				t,
				e,
				api.Request{Key: "c", Action: "create", Config: config()},
			)
			wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
			op, err := e.Submit(
				context.Background(),
				api.Request{Key: "capture", Action: "snapshot", VMID: a.VMID},
			)
			if err != nil {
				t.Fatal(err)
			}
			awaitStep(t, e, op.ID, step)
			e.Close()
			e = openTest(t, dir, b, false, nil)
			defer e.Close()
			final := awaitStatus(t, e, op.ID)
			snap, _ := e.GetSnapshot(context.Background(), op.SnapshotID)
			if step == "pause_intent" || step == "paused" ||
				step == "captured" ||
				step == "source_resumed" ||
				step == "capture_copied" {
				if final.Status != "failed" || snap.Published {
					t.Fatalf(
						"incomplete capture published %+v %+v",
						final,
						snap,
					)
				}
			} else {
				if final.Status != "succeeded" || !snap.Published {
					t.Fatalf(
						"completed capture not recovered %+v %+v",
						final,
						snap,
					)
				}
				s, err := e.loadSnapshot(context.Background(), op.SnapshotID)
				if err != nil {
					t.Fatal(err)
				}
				for file, want := range map[string]string{"disk.raw": "disk-generation-1", "ch/memory-ranges": "ram-generation-1"} {
					data, err := os.ReadFile(filepath.Join(s.Dir, file))
					if err != nil || string(data) != want {
						t.Fatalf(
							"replay changed captured generation: file=%s data=%q error=%v",
							file,
							data,
							err,
						)
					}
				}
			}
			v, _ := e.loadVM(context.Background(), a.VMID)
			b.mu.Lock()
			paused := b.states[v.Resource.Instance].Paused
			b.mu.Unlock()
			if paused {
				t.Fatal("source left paused after recovery")
			}
		})
	}
}
