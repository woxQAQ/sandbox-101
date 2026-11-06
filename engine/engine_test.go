package engine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// This backend is intentionally confined to tests. Its persisted guest state
// lets crash tests independently compare captured disk and RAM generations.
type testBackend struct {
	mu            sync.Mutex
	states        map[string]Observation
	launches      int
	resumes       int
	captures      int
	agentError    error
	inspectError  error
	pauseGate     chan struct{}
	captureGate   chan struct{}
	captureDamage string
}

func newTestBackend() *testBackend { return &testBackend{states: map[string]Observation{}} }

func (b *testBackend) Compatibility() string                   { return "test-only/v1" }
func (b *testBackend) Check(context.Context, api.Config) error { return nil }
func (b *testBackend) Prepare(_ context.Context, r Resource) error {
	for _, p := range []string{filepath.Dir(r.Disk), r.ImageDir, filepath.Join(r.Dir, "capture")} {
		if e := os.MkdirAll(p, 0o700); e != nil {
			return e
		}
	}
	if _, e := os.Stat(r.Disk); os.IsNotExist(e) {
		if e = os.WriteFile(
			r.Disk,
			[]byte("disk-generation-1"),
			0o600,
		); e != nil {
			return e
		}
	}
	return os.WriteFile(
		filepath.Join(r.ImageDir, "image.json"),
		[]byte(`{"format":1}`),
		0o600,
	)
}

func (b *testBackend) Inspect(
	_ context.Context,
	r Resource,
) (Observation, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.states[r.Instance], b.inspectError
}

func (b *testBackend) Launch(_ context.Context, r Resource, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.states[r.Instance].Alive {
		return errors.New("duplicate launch")
	}
	b.launches++
	b.states[r.Instance] = Observation{
		Alive:     true,
		PID:       100 + b.launches,
		StartTime: uint64(b.launches),
	}
	return nil
}

func (b *testBackend) Boot(_ context.Context, r Resource, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := b.states[r.Instance]
	o.Booted = true
	b.states[r.Instance] = o
	return nil
}

func (b *testBackend) Agent(_ context.Context, _ Resource, _ bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.agentError
}

func (b *testBackend) Stop(_ context.Context, r Resource) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.states[r.Instance] = Observation{}
	return nil
}

func (b *testBackend) Pause(ctx context.Context, r Resource) error {
	if b.pauseGate != nil {
		select {
		case <-b.pauseGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	o := b.states[r.Instance]
	o.Paused = true
	b.states[r.Instance] = o
	return nil
}

func (b *testBackend) Capture(
	ctx context.Context,
	r Resource,
	dst string,
) error {
	if b.captureGate != nil {
		select {
		case <-b.captureGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	b.mu.Lock()
	o := b.states[r.Instance]
	b.captures++
	b.mu.Unlock()
	if !o.Paused {
		return errors.New("capture outside paused window")
	}
	for name, data := range map[string][]byte{"config.json": []byte(`{}`), "state.json": []byte(`{"state":"paused"}`), "memory-ranges": []byte("ram-generation-1")} {
		if e := os.WriteFile(filepath.Join(dst, name), data, 0o600); e != nil {
			return e
		}
	}
	if b.captureDamage != "" {
		path := filepath.Join(dst, "memory-ranges")
		if err := os.Remove(path); err != nil {
			return err
		}
		return os.Symlink("../../outside", path)
	}
	return nil
}

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

func (b *testBackend) Resume(_ context.Context, r Resource) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := b.states[r.Instance]
	o.Paused = false
	b.states[r.Instance] = o
	b.resumes++
	// Changes to the live source immediately after resume must not reach capture.
	return os.WriteFile(r.Disk, []byte("disk-generation-2"), 0o600)
}

func (b *testBackend) Network(_ context.Context, r Resource, on bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	o := b.states[r.Instance]
	o.Network = on
	b.states[r.Instance] = o
	return nil
}
func (b *testBackend) Remove(context.Context, Resource) error { return nil }
func config() *api.Config {
	return &api.Config{
		Image: "sha256:" + strings.Repeat("a", 64),
		Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}
}

func openTest(
	t *testing.T,
	dir string,
	b Backend,
	init bool,
	fault func(string) error,
) *Engine {
	t.Helper()
	if init {
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	e, err := Open(
		Options{
			StateDir:          dir,
			Init:              init,
			Backend:           b,
			SafetyBytes:       1,
			MaxMemoryMiB:      8192,
			MaxVCPUs:          64,
			ReconcileInterval: time.Hour,
			Fault:             fault,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func wait(t *testing.T, e *Engine, r api.Request) api.Operation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	op, err := api.Wait(ctx, e, r)
	if err != nil {
		t.Fatalf("%s failed: %v op=%+v", r.Action, err, op)
	}
	return op
}

func awaitStatus(t *testing.T, e *Engine, id string) api.Operation {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		o, err := e.GetOperation(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if terminal(o.Status) {
			return o
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("terminal status timed out")
	return api.Operation{}
}

func awaitStep(t *testing.T, e *Engine, id, step string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		o, err := e.GetOperation(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if o.Step == step {
			return
		}
		if terminal(o.Status) {
			t.Fatalf("operation terminated before %s: %+v", step, o)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("step timed out")
}

func TestLifecycleIdempotencyAndIndependentDeletion(t *testing.T) {
	b := newPolicyTestBackend()
	e := openPolicyTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	create := api.Request{Key: "create", Action: "create", Config: config()}
	a := wait(t, e, create)
	same, err := e.Submit(context.Background(), create)
	if err != nil || same.ID != a.ID || same.Status != "succeeded" {
		t.Fatalf("idempotent result: %+v %v", same, err)
	}
	different := create
	different.Config = &api.Config{
		Image: config().Image,
		Shape: api.Shape{VCPUs: 2, MemoryMiB: 128, DiskMiB: 64},
	}
	if _, err = e.Submit(context.Background(), different); err == nil {
		t.Fatal("same key accepted changed parameters")
	}
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: a.VMID})
	vm, _ := e.GetVM(context.Background(), a.VMID)
	if vm.Phase != "running" || !vm.AgentReady || vm.NetworkActual {
		t.Fatalf("bad running VM: %+v", vm)
	}
	if _, err = e.Submit(
		context.Background(),
		api.Request{
			Key:              "stale",
			Action:           "stop",
			VMID:             a.VMID,
			ExpectedInstance: "old",
		},
	); err == nil {
		t.Fatal("stale instance accepted")
	}
	wait(
		t,
		e,
		api.Request{
			Key:              "policy",
			Action:           "network_policy",
			VMID:             a.VMID,
			ExpectedInstance: vm.Instance,
			Policy: &api.NetworkPolicy{
				Version:        1,
				AllowedCIDRs:   []string{"1.1.1.1/32"},
				MaxConnections: 256,
			},
		},
	)
	wait(
		t,
		e,
		api.Request{
			Key:              "network",
			Action:           "network",
			VMID:             a.VMID,
			ExpectedInstance: vm.Instance,
			NetworkEnabled:   true,
		},
	)
	s := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: a.VMID},
	)
	sr, _ := e.loadSnapshot(context.Background(), s.SnapshotID)
	disk, err := os.ReadFile(filepath.Join(sr.Dir, "disk.raw"))
	if err != nil || string(disk) != "disk-generation-1" {
		t.Fatalf("disk captured after source resume: %q %v", disk, err)
	}
	child := wait(
		t,
		e,
		api.Request{Key: "clone", Action: "clone", SnapshotID: s.SnapshotID},
	)
	cr, _ := e.loadVM(context.Background(), child.VMID)
	before, err := os.Stat(
		filepath.Join(cr.Resource.RestoreDir, "memory-ranges"),
	)
	if err != nil {
		t.Fatal(err)
	}
	source, _ := os.Stat(filepath.Join(sr.Dir, "ch/memory-ranges"))
	if !os.SameFile(before, source) {
		t.Fatal("COW backing not hardline")
	}
	wait(
		t,
		e,
		api.Request{Key: "delete-source", Action: "delete", VMID: a.VMID},
	)
	wait(
		t,
		e,
		api.Request{
			Key:        "delete-snapshot",
			Action:     "delete_snapshot",
			SnapshotID: s.SnapshotID,
		},
	)
	if _, err = os.Stat(
		filepath.Join(cr.Resource.RestoreDir, "memory-ranges"),
	); err != nil {
		t.Fatal("snapshot deletion broke backing", err)
	}
	if _, err = os.Stat(cr.Resource.Disk); err != nil {
		t.Fatal("source deletion broke clone disk", err)
	}
	vm, err = e.GetVM(context.Background(), child.VMID)
	if err != nil || vm.Phase != "running" {
		t.Fatalf("clone failed independence %+v %v", vm, err)
	}
	wait(t, e, api.Request{Key: "stop-child", Action: "stop", VMID: child.VMID})
	wait(
		t,
		e,
		api.Request{Key: "delete-child", Action: "delete", VMID: child.VMID},
	)
	vms, _ := e.ListVMs(context.Background())
	snaps, _ := e.ListSnapshots(context.Background())
	if len(vms) != 0 || len(snaps) != 0 {
		t.Fatal("incomplete lists after deletion")
	}
	op, err := e.FindOperation(context.Background(), "create")
	if err != nil || op.VMID != a.VMID {
		t.Fatal("deleted VM erased durable operation history")
	}
}

func TestStartupAdoptsLiveInstanceWithoutRelaunch(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	var hit atomic.Bool
	e := openTest(t, dir, b, true, func(step string) error {
		if step == "launched" && hit.CompareAndSwap(false, true) {
			return ErrInterrupted
		}
		return nil
	})
	a := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	o, err := e.Submit(
		context.Background(),
		api.Request{Key: "start", Action: "start", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, o.ID, "launched")
	e.Close()
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	op := awaitStatus(t, e, o.ID)
	if op.Status != "succeeded" {
		t.Fatalf("adoption failed %+v", op)
	}
	b.mu.Lock()
	launches := b.launches
	b.mu.Unlock()
	if launches != 1 {
		t.Fatalf("recovery relaunched VMM %d times", launches)
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

func TestCloneRejectsLegacyPublication(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	op := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	lock := e.vmLock(vm.VMID)
	lock.Lock()
	s, err := e.loadSnapshot(context.Background(), op.SnapshotID)
	lock.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err = json.Unmarshal(s.Snapshot.Manifest, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.PublicationMode = ""
	s.Snapshot.Manifest = encode(manifest)
	if err = e.saveSnapshot(s); err != nil {
		t.Fatal(err)
	}
	clone, err := e.Submit(
		context.Background(),
		api.Request{
			Key:        "legacy-clone",
			Action:     "clone",
			SnapshotID: op.SnapshotID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitStatus(t, e, clone.ID)
	if final.Status != "failed" || final.Error == nil ||
		final.Error.Code != api.Incompatible {
		t.Fatalf("legacy publication trusted: %+v", final)
	}
	b.mu.Lock()
	launches := b.launches
	b.mu.Unlock()
	if launches != 1 {
		t.Fatal("legacy publication launched another VMM")
	}
	if err = verifyFiles(s.Dir, manifest.Files); err != nil {
		t.Fatal("legacy evidence altered", err)
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

func TestCloneRecoveryAfterBackingTransferAndSnapshotDeletion(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	var hit atomic.Bool
	e := openTest(t, dir, b, true, func(s string) error {
		if s == "restore_resources_ready" && hit.CompareAndSwap(false, true) {
			return ErrInterrupted
		}
		return nil
	})
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "boot", Action: "start", VMID: a.VMID})
	s := wait(t, e, api.Request{Key: "snap", Action: "snapshot", VMID: a.VMID})
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "clone", Action: "clone", SnapshotID: s.SnapshotID},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "restore_resources_ready")
	wait(
		t,
		e,
		api.Request{
			Key:        "del-snapshot",
			Action:     "delete_snapshot",
			SnapshotID: s.SnapshotID,
		},
	)
	e.Close()
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	final := awaitStatus(t, e, op.ID)
	if final.Status != "succeeded" {
		t.Fatalf("clone still depends on deleted source snapshot %+v", final)
	}
}

func TestReservationBeforePause(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	free, err := freeBytes(e.opts.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	e.opts.SafetyBytes = free
	if _, err = e.Submit(
		context.Background(),
		api.Request{Key: "full", Action: "snapshot", VMID: a.VMID},
	); err == nil {
		t.Fatal("insufficient storage admitted")
	}
	b.mu.Lock()
	captures, resumes := b.captures, b.resumes
	b.mu.Unlock()
	if captures != 0 || resumes != 0 {
		t.Fatal("reservation rejected after pausing")
	}
	e.opts.SafetyBytes = 1
	s := wait(
		t,
		e,
		api.Request{Key: "budgeted", Action: "snapshot", VMID: a.VMID},
	)
	var bytes int64
	if err = e.db.QueryRow("SELECT bytes FROM reservations WHERE owner=?", s.SnapshotID).
		Scan(&bytes); err != nil {
		t.Fatal(err)
	}
	shape := config().Shape
	minimum := (2*shape.MemoryMiB + shape.DiskMiB + 160) << 20
	if bytes < minimum {
		t.Fatalf(
			"reservation %d cannot cover two RAM copies, disk and bounded JSON %d",
			bytes,
			minimum,
		)
	}
}

func TestSingleWriterAndMissingMetadata(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	e := openTest(t, dir, b, true, nil)
	if other, err := Open(Options{StateDir: dir, Backend: b}); err == nil {
		other.Close()
		t.Fatal("second writer admitted")
	}
	e.Close()
	os.Remove(filepath.Join(dir, "metadata.db"))
	if other, err := Open(Options{StateDir: dir, Backend: b}); err == nil {
		other.Close()
		t.Fatal("missing metadata silently recreated")
	}
}

func TestAgentFailureStopsColdBoot(t *testing.T) {
	b := newTestBackend()
	b.agentError = errors.New("guest agent unavailable")
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "s", Action: "start", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	op = awaitStatus(t, e, op.ID)
	if op.Status != "failed" {
		t.Fatal("agent failure accepted")
	}
	v, _ := e.GetVM(context.Background(), a.VMID)
	if v.Phase != "stopped" || v.PID != 0 || v.AgentReady {
		t.Fatalf("unverified cold boot still running %+v", v)
	}
}

func TestUnknownIdentityNeverStopsOrDeletes(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	l := e.vmLock(a.VMID)
	l.Lock()
	l.Unlock() //nolint:staticcheck // Synchronize with terminal publication before inspecting resources.
	b.mu.Lock()
	b.inspectError = api.Fail(api.ManualRecovery, "PID reused")
	b.mu.Unlock()
	_ = e.Reconcile(context.Background())
	v, _ := e.GetVM(context.Background(), a.VMID)
	if v.Verified || v.BlockedReason == "" {
		t.Fatal("unknown identity treated as confirmed")
	}
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "delete", Action: "delete", VMID: a.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	final := awaitStatus(t, e, op.ID)
	if final.Status != "failed" {
		t.Fatal("unconfirmed resource deleted")
	}
	if _, err = e.GetVM(context.Background(), a.VMID); err != nil {
		t.Fatal("ownership record removed despite uncertainty")
	}
}

func TestBackupRecoveryRequiresExplicitReview(t *testing.T) {
	dir := t.TempDir()
	b := newTestBackend()
	e := openTest(t, dir, b, true, nil)
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	backup := filepath.Join(t.TempDir(), "backup.db")
	point, err := e.Backup(context.Background(), backup)
	if err != nil {
		t.Fatal(err)
	}
	if point.ID == "" || point.DatabaseSHA256 == "" {
		t.Fatal("missing recovery point")
	}
	e.Close()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err = os.Remove(
			filepath.Join(dir, "metadata.db") + suffix,
		); err != nil &&
			!os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	if err = RestoreBackup(dir, backup); err != nil {
		t.Fatal(err)
	}
	if other, x := Open(Options{StateDir: dir, Backend: b}); x == nil {
		other.Close()
		t.Fatal("restored backup automatically trusted")
	}
	if err = AcknowledgeRecovery(
		dir,
		"reviewed running scopes and retained files",
	); err != nil {
		t.Fatal(err)
	}
	e = openTest(t, dir, b, false, nil)
	defer e.Close()
	v, err := e.GetVM(context.Background(), a.VMID)
	if err != nil || v.ID != a.VMID {
		t.Fatal("recovery lost known VM", err)
	}
}

func TestRestoreUsesFreshInstanceAndDefaultNetworkBlock(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	a := wait(t, e, api.Request{Key: "a", Action: "create", Config: config()})
	wait(t, e, api.Request{Key: "s", Action: "start", VMID: a.VMID})
	before, _ := e.GetVM(context.Background(), a.VMID)
	s := wait(t, e, api.Request{Key: "snap", Action: "snapshot", VMID: a.VMID})
	wait(t, e, api.Request{Key: "off", Action: "stop", VMID: a.VMID})
	wait(
		t,
		e,
		api.Request{
			Key:        "restore",
			Action:     "restore",
			VMID:       a.VMID,
			SnapshotID: s.SnapshotID,
		},
	)
	after, _ := e.GetVM(context.Background(), a.VMID)
	if after.Instance == before.Instance || after.ID != before.ID ||
		after.NetworkActual ||
		after.NetworkEnabled ||
		!after.AgentReady {
		t.Fatalf("bad restore identity %+v", after)
	}
}

func TestTerminalOperationRecordIsImmutable(t *testing.T) {
	b := newTestBackend()
	e := openTest(t, t.TempDir(), b, true, nil)
	defer e.Close()
	r := api.Request{Key: "a", Action: "create", Config: config()}
	a := wait(t, e, r)
	first, _ := json.Marshal(a)
	for range 8 {
		same, err := e.Submit(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		next, _ := json.Marshal(same)
		if string(first) != string(next) {
			t.Fatal("terminal idempotency response changed")
		}
	}
}
