package engine

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func (b *testBackend) Compatibility() string { return "test-only/v1" }

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

func layerFixture(t *testing.T, headers ...*tar.Header) (string, string) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, h := range headers {
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Size > 0 {
			if _, err := tw.Write(make([]byte, h.Size)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "layer.tar")
	if err := os.WriteFile(file, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(buf.Bytes())
	return file, "sha256:" + hex.EncodeToString(h[:])
}

// Count effects at the Backend boundary while the real Engine persists and replays.
type instanceRecoveryBackend struct {
	*testBackend

	boundary    string
	armed       atomic.Bool
	interrupted chan struct{}
	stops       atomic.Int32
	launchCalls atomic.Int32
	removes     atomic.Int32
	networkOn   atomic.Int32
}

func (b *instanceRecoveryBackend) Launch(
	ctx context.Context,
	r Resource,
	restore bool,
) error {
	b.launchCalls.Add(1)
	interrupt := b.armed.Load() &&
		(b.boundary == "instance_persisted" || b.boundary == "launch_unrecorded")
	if interrupt && b.boundary == "instance_persisted" {
		b.armed.Store(false)
		close(b.interrupted)
		return ErrInterrupted
	}
	if err := b.testBackend.Launch(ctx, r, restore); err != nil {
		return err
	}
	if interrupt {
		b.armed.Store(false)
		close(b.interrupted)
		return ErrInterrupted
	}
	return nil
}

func (b *instanceRecoveryBackend) Stop(ctx context.Context, r Resource) error {
	b.stops.Add(1)
	return b.testBackend.Stop(ctx, r)
}

func (b *instanceRecoveryBackend) Remove(
	ctx context.Context,
	r Resource,
) error {
	b.removes.Add(1)
	return b.testBackend.Remove(ctx, r)
}

func (b *instanceRecoveryBackend) Network(
	ctx context.Context,
	r Resource,
	on bool,
) error {
	if on {
		b.networkOn.Add(1)
	}
	return b.testBackend.Network(ctx, r, on)
}

type instanceRecoveryFixture struct {
	e       *Engine
	dir     string
	b       *instanceRecoveryBackend
	request api.Request
	op      api.Operation
	old     api.VM
	record  opRecord
}

func newInstanceRecoveryFixture(
	t *testing.T,
	action, boundary string,
) *instanceRecoveryFixture {
	t.Helper()
	b := &instanceRecoveryBackend{
		testBackend: newTestBackend(),
		boundary:    boundary,
		interrupted: make(chan struct{}),
	}
	f := &instanceRecoveryFixture{dir: t.TempDir(), b: b}
	f.e = openTest(t, f.dir, b, true, func(step string) error {
		if step == boundary && b.armed.CompareAndSwap(true, false) {
			close(b.interrupted)
			return ErrInterrupted
		}
		return nil
	})
	t.Cleanup(func() { _ = f.e.Close() })
	vm := wait(
		t,
		f.e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(
		t,
		f.e,
		api.Request{Key: "first-start", Action: "start", VMID: vm.VMID},
	)
	f.request = api.Request{Key: "conditional", Action: action, VMID: vm.VMID}
	if action == "restore" {
		snapshot := wait(
			t,
			f.e,
			api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
		)
		f.request.SnapshotID = snapshot.SnapshotID
	}
	wait(t, f.e, api.Request{Key: "stop", Action: "stop", VMID: vm.VMID})
	var err error
	f.old, err = f.e.GetVM(context.Background(), vm.VMID)
	if err != nil || f.old.Instance == "" || f.old.Phase != "stopped" {
		t.Fatalf("missing stopped instance A: %+v %v", f.old, err)
	}
	f.request.ExpectedInstance = f.old.Instance
	b.armed.Store(true)
	f.op, err = f.e.Submit(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.interrupted:
	case <-time.After(10 * time.Second):
		t.Fatal("instance recovery fault was not reached")
	}
	lock := f.e.vmLock(vm.VMID)
	lock.Lock()
	f.record, err = f.e.loadOp(context.Background(), f.op.ID)
	lock.Unlock()
	if err != nil || f.record.Journal.Resource == nil {
		t.Fatalf("missing candidate journal: %+v %v", f.record, err)
	}
	return f
}

func (f *instanceRecoveryFixture) restart(t *testing.T) {
	t.Helper()
	if err := f.e.Close(); err != nil {
		t.Fatal(err)
	}
	f.e = openTest(t, f.dir, f.b, false, nil)
}

type instanceRecoveryEffects struct {
	launches  int32
	stops     int32
	removes   int32
	networkOn int32
}

func (b *instanceRecoveryBackend) effects() instanceRecoveryEffects {
	return instanceRecoveryEffects{
		b.launchCalls.Load(),
		b.stops.Load(),
		b.removes.Load(),
		b.networkOn.Load(),
	}
}

func (f *instanceRecoveryFixture) mutate(
	t *testing.T,
	change func(*opRecord, *vmRecord),
) {
	t.Helper()
	lock := f.e.vmLock(f.op.VMID)
	lock.Lock()
	defer lock.Unlock()
	o, err := f.e.loadOp(context.Background(), f.op.ID)
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.e.loadVM(context.Background(), f.op.VMID)
	if err != nil {
		t.Fatal(err)
	}
	change(&o, &v)
	if err = f.e.saveVM(v); err != nil {
		t.Fatal(err)
	}
	if err = f.e.saveOp(&o, o.Operation.Step); err != nil {
		t.Fatal(err)
	}
}

// The fixture uses lifecycle capture, then stops a clone at a durable boundary.
func restoreCandidateFixture(
	t *testing.T,
	boundary string,
	actions ...string,
) (string, *testBackend, api.Operation, opRecord, snapshotRecord) {
	t.Helper()
	dir := t.TempDir()
	b := newTestBackend()
	var armed atomic.Bool
	e := openTest(t, dir, b, true, func(step string) error {
		if armed.Load() && step == boundary {
			return ErrInterrupted
		}
		return nil
	})
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	snap := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	request := api.Request{
		Key:        "clone",
		Action:     "clone",
		SnapshotID: snap.SnapshotID,
	}
	if len(actions) != 0 && actions[0] == "restore" {
		wait(
			t,
			e,
			api.Request{Key: "stop-source", Action: "stop", VMID: vm.VMID},
		)
		request.Action, request.VMID = "restore", vm.VMID
	}
	armed.Store(true)
	op, err := e.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, boundary)
	// Read under the lifecycle lock after the faulted worker has returned.
	lock := e.vmLock(op.VMID)
	lock.Lock()
	record, err := e.loadOp(context.Background(), op.ID)
	if err != nil {
		t.Fatal(err)
	}
	source, err := e.loadSnapshot(context.Background(), snap.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	lock.Unlock()
	if err = e.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, b, op, record, source
}

func damageRestoreCandidate(t *testing.T, path, damage string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	switch damage {
	case "truncate":
		err = os.Truncate(path, 1)
	case "rewrite":
		err = os.WriteFile(path, bytes.Repeat([]byte{'x'}, len(data)), 0o600)
	case "remove":
		err = os.Remove(path)
	}
	if err != nil {
		t.Fatal(err)
	}
}
