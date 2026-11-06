package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

type gcTestBackend struct {
	*testBackend

	image ImageManifest
}

func (b *gcTestBackend) RequiresImageCatalog() bool { return true }
func (b *gcTestBackend) Prepare(_ context.Context, r Resource) error {
	for _, path := range []string{filepath.Dir(r.Disk), filepath.Join(r.Dir, "capture")} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	if _, err := os.Stat(r.Disk); os.IsNotExist(err) {
		return os.WriteFile(r.Disk, []byte("disk-generation-1"), 0o600)
	}
	return nil
}

func openGCTest(
	t *testing.T,
	fault func(string) error,
	intervals ...time.Duration,
) (*Engine, *gcTestBackend) {
	t.Helper()
	b := &gcTestBackend{testBackend: newTestBackend()}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	interval := time.Hour
	if len(intervals) != 0 {
		interval = intervals[0]
	}
	e, err := Open(
		Options{
			StateDir:          dir,
			Init:              true,
			Backend:           b,
			SafetyBytes:       1,
			MaxMemoryMiB:      8192,
			MaxVCPUs:          64,
			ReconcileInterval: time.Hour,
			GCInterval:        interval,
			Fault:             fault,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	b.image = ImageManifest{
		Format:       1,
		Layout:       "merged-erofs-v1",
		Digest:       config().Image,
		Architecture: runtime.GOARCH,
		Files:        map[string]string{},
	}
	imageDir := filepath.Join(dir, "images", config().Image[7:])
	if err = os.MkdirAll(imageDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kernel", "initramfs", "root.erofs"} {
		path := filepath.Join(imageDir, name)
		if err = os.WriteFile(path, []byte(name), 0o444); err != nil {
			t.Fatal(err)
		}
		b.image.Files[name], err = digestFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = writeJSON(
		filepath.Join(imageDir, "image.json"),
		b.image,
	); err != nil {
		t.Fatal(err)
	}
	if _, err = e.db.Exec(
		"INSERT INTO meta VALUES(?,?)",
		"image:"+b.image.Digest,
		string(encode(b.image)),
	); err != nil {
		t.Fatal(err)
	}
	return e, b
}

func gcPass(t *testing.T, e *Engine, opts GCOptions) GCReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		r, err := e.CollectGarbage(ctx, opts)
		var a *api.Error
		if err == nil {
			return r
		}
		if ctx.Err() != nil || !errors.As(err, &a) || a.Code != api.Conflict {
			t.Fatalf("GC: %v; report=%+v", err, r)
		}
		time.Sleep(time.Millisecond)
	}
}

func gcSource(t *testing.T, e *Engine) (api.Operation, api.Operation) {
	t.Helper()
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	s := wait(
		t,
		e,
		api.Request{Key: "snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	return vm, s
}

func gcStoppedRestore(
	t *testing.T,
	e *Engine,
) (api.Operation, api.Operation, string) {
	t.Helper()
	vm, s := gcSource(t, e)
	v, _ := e.loadVM(context.Background(), vm.VMID)
	old := v.Resource.Disk
	wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: vm.VMID})
	wait(
		t,
		e,
		api.Request{
			Key:        "restore",
			Action:     "restore",
			VMID:       vm.VMID,
			SnapshotID: s.SnapshotID,
		},
	)
	wait(t, e, api.Request{Key: "stop-restored", Action: "stop", VMID: vm.VMID})
	l := e.vmLock(vm.VMID)
	l.Lock()
	l.Unlock() //nolint:staticcheck // Wait for the operation worker to release the VM lock.
	return vm, s, old
}

func gcExists(t *testing.T, path string, want bool) {
	t.Helper()
	_, err := os.Lstat(path)
	if want && err != nil || !want && !os.IsNotExist(err) {
		t.Fatalf("exists(%s)=%v, want %v", path, err, want)
	}
}

func TestGCDryRunPreservesMissingVersionOwnership(t *testing.T) {
	e, _ := openGCTest(t, nil)
	vm, snapshot, _ := gcStoppedRestore(t, e)
	previous, err := e.loadVM(context.Background(), vm.VMID)
	if err != nil {
		t.Fatal(err)
	}
	wait(
		t,
		e,
		api.Request{
			Key:        "restore-next",
			Action:     "restore",
			VMID:       vm.VMID,
			SnapshotID: snapshot.SnapshotID,
		},
	)
	wait(t, e, api.Request{Key: "stop-next", Action: "stop", VMID: vm.VMID})
	lock := e.vmLock(vm.VMID)
	lock.Lock()
	lock.Unlock() //nolint:staticcheck // Wait for the operation worker before removing its resource directory.
	if err := os.RemoveAll(
		filepath.Dir(previous.Resource.RestoreDir),
	); err != nil {
		t.Fatal(err)
	}
	counts := func() [3]int64 {
		var result [3]int64
		for i, query := range []string{"SELECT COUNT(*) FROM backing", "SELECT COUNT(*) FROM reservations", "SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'"} {
			if err := e.db.QueryRow(query).Scan(&result[i]); err != nil {
				t.Fatal(err)
			}
		}
		return result
	}
	before := counts()
	gcPass(t, e, GCOptions{DryRun: true})
	if after := counts(); after != before {
		t.Fatalf(
			"dry run changed backing/reservations/epoch: before=%v after=%v",
			before,
			after,
		)
	}
}

func TestGCReclaimsRetiredVersionsBackingAndReservations(t *testing.T) {
	e, _ := openGCTest(t, nil)
	vm, s, old := gcStoppedRestore(t, e)
	gcPass(t, e, GCOptions{DryRun: true})
	gcExists(t, old, true)
	gcPass(t, e, GCOptions{})
	gcExists(t, old, false)
	before, _ := e.loadVM(context.Background(), vm.VMID)
	oldVersion := filepath.Dir(before.Resource.RestoreDir)
	var firstRestore opRecord
	var b []byte
	if err := e.db.QueryRow("SELECT body FROM operations WHERE key='restore'").
		Scan(&b); err != nil ||
		gcDecode(b, &firstRestore) != nil {
		t.Fatal("restore journal unavailable", err)
	}
	wait(
		t,
		e,
		api.Request{
			Key:        "restore-again",
			Action:     "restore",
			VMID:       vm.VMID,
			SnapshotID: s.SnapshotID,
		},
	)
	// Even retired resources remain while a VMM can still be reading.
	r := gcPass(t, e, GCOptions{})
	gcExists(t, oldVersion, true)
	if len(r.Retained) == 0 {
		t.Fatal("live-reader deferral not reported")
	}
	wait(t, e, api.Request{Key: "stop-again", Action: "stop", VMID: vm.VMID})
	r = gcPass(t, e, GCOptions{})
	gcExists(t, oldVersion, false)
	current, _ := e.loadVM(context.Background(), vm.VMID)
	gcExists(t, current.Resource.Disk, true)
	gcExists(
		t,
		filepath.Join(current.Resource.RestoreDir, "memory-ranges"),
		true,
	)
	var n int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM backing WHERE path=?", filepath.Join(before.Resource.RestoreDir, "memory-ranges")).
		Scan(&n); err != nil ||
		n != 0 {
		t.Fatalf("retired backing row retained: %d %v", n, err)
	}
	if err := e.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner=?", vm.VMID+":"+firstRestore.Operation.ID).
		Scan(&n); err != nil ||
		n != 0 {
		t.Fatalf("retired reservation retained: %d %v", n, err)
	}
	r = gcPass(t, e, GCOptions{})
	if len(r.Removed) != 0 {
		t.Fatalf("repeated pass changed resources: %+v", r)
	}
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	if err != nil || op.ID != vm.ID || op.Status != "succeeded" {
		t.Fatalf("idempotency record changed: %+v %v", op, err)
	}
}

func TestGCSourceSnapshotDeletionPreservesRunningCloneBacking(t *testing.T) {
	e, _ := openGCTest(t, nil)
	vm, s := gcSource(t, e)
	child := wait(
		t,
		e,
		api.Request{Key: "clone", Action: "clone", SnapshotID: s.SnapshotID},
	)
	v, _ := e.loadVM(context.Background(), child.VMID)
	backing := filepath.Join(v.Resource.RestoreDir, "memory-ranges")
	f, err := os.Open(backing)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m, err := unix.Mmap(
		int(f.Fd()),
		0,
		len("ram-generation-1"),
		unix.PROT_READ,
		unix.MAP_PRIVATE,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(m)
	wait(
		t,
		e,
		api.Request{Key: "delete-source", Action: "delete", VMID: vm.VMID},
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
	gcPass(t, e, GCOptions{})
	gcExists(t, backing, true)
	if string(m) != "ram-generation-1" {
		t.Fatal("clone mapping no longer reads original backing")
	}
	wait(t, e, api.Request{Key: "stop-clone", Action: "stop", VMID: child.VMID})
	gcPass(t, e, GCOptions{})
	gcExists(t, backing, true) // A stopped VM still owns its current version.
}

func TestGCDefersPendingOperationLeaseAndSnapshotCapture(t *testing.T) {
	var interrupt bool
	e, _ := openGCTest(t, func(step string) error {
		if interrupt && step == "restore_prepare_intent" {
			return ErrInterrupted
		}
		return nil
	})
	_, s := gcSource(t, e)
	interrupt = true
	op, err := e.Submit(
		context.Background(),
		api.Request{
			Key:        "pending-clone",
			Action:     "clone",
			SnapshotID: s.SnapshotID,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "restore_prepare_intent")
	// Wait for the interrupted worker to release its lifecycle lock.
	deadline := time.Now().Add(3 * time.Second)
	for {
		r, err := e.CollectGarbage(context.Background(), GCOptions{})
		if err == nil {
			if len(r.Removed) != 0 {
				t.Fatalf("pending readers lost resources: %+v", r)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	var leases int
	if err = e.db.QueryRow("SELECT COUNT(*) FROM leases WHERE operation=?", op.ID).
		Scan(&leases); err != nil ||
		leases != 1 {
		t.Fatalf("lease removed: %d %v", leases, err)
	}
	sr, _ := e.loadSnapshot(context.Background(), s.SnapshotID)
	gcExists(t, filepath.Join(sr.Dir, "ch/memory-ranges"), true)
	// A missing root row is corruption, never evidence that the reader expired.
	if _, err = e.db.Exec(
		"DELETE FROM leases WHERE operation=?",
		op.ID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err = e.CollectGarbage(
		context.Background(),
		GCOptions{},
	); err == nil {
		t.Fatal("missing lease root allowed sweep")
	}
}

func TestGCFailsClosedForCorruptOrUnknownRoots(t *testing.T) {
	for _, damage := range []string{"vm-json", "missing-disk", "snapshot-json", "missing-image", "unknown-meta", "unknown-vm", "unknown-version", "symlink-version", "recovery", "reviewed-recovery", "head", "unknown-backing", "unknown-table", "unknown-reservation", "incomplete-import", "instance"} {
		t.Run(damage, func(t *testing.T) {
			e, backend := openGCTest(t, nil)
			vm, s, old := gcStoppedRestore(t, e)
			v, _ := e.loadVM(context.Background(), vm.VMID)
			var err error
			switch damage {
			case "vm-json":
				_, err = e.db.Exec(
					"UPDATE vms SET body='{}' WHERE id=?",
					vm.VMID,
				)
			case "missing-disk":
				err = os.Remove(v.Resource.Disk)
			case "snapshot-json":
				_, err = e.db.Exec(
					"UPDATE snapshots SET body='{}' WHERE id=?",
					s.SnapshotID,
				)
			case "missing-image":
				_, err = e.db.Exec(
					"DELETE FROM meta WHERE key=?",
					"image:"+v.VM.Config.Image,
				)
				if err == nil {
					err = os.RemoveAll(v.Resource.ImageDir)
				}
			case "unknown-meta":
				_, err = e.db.Exec(
					"INSERT INTO meta VALUES('future-resource','{}')",
				)
			case "unknown-vm":
				err = os.Mkdir(
					filepath.Join(e.opts.StateDir, "vms", "unknown"),
					0o700,
				)
			case "unknown-version":
				err = os.Mkdir(
					filepath.Join(v.Resource.Dir, "versions", "unknown"),
					0o700,
				)
			case "symlink-version":
				err = os.Symlink(
					t.TempDir(),
					filepath.Join(v.Resource.Dir, "versions", "unknown"),
				)
			case "recovery":
				err = os.WriteFile(
					filepath.Join(e.opts.StateDir, "RECOVERY_REQUIRED"),
					[]byte("{}"),
					0o600,
				)
			case "reviewed-recovery":
				err = os.WriteFile(
					filepath.Join(e.opts.StateDir, "RECOVERY_REVIEWED.json"),
					[]byte("{}"),
					0o600,
				)
			case "head":
				err = os.Remove(filepath.Join(e.opts.StateDir, "HEAD.json"))
			case "unknown-backing":
				_, err = e.db.Exec(
					"INSERT INTO backing VALUES(?,?,?)",
					vm.VMID,
					filepath.Join(v.Resource.Dir, "unowned"),
					config().Image,
				)
			case "unknown-table":
				_, err = e.db.Exec("CREATE TABLE future_resources(path TEXT)")
			case "unknown-reservation":
				_, err = e.db.Exec(
					"INSERT INTO reservations VALUES('unknown',1,0,0)",
				)
			case "incomplete-import":
				digest := "sha256:" + strings.Repeat("b", 64)
				path := filepath.Join(e.opts.StateDir, "imports", digest[7:])
				err = os.Mkdir(path, 0o700)
				if err == nil {
					err = writeJSON(
						filepath.Join(path, "intent.json"),
						ImportOptions{Digest: digest},
					)
				}
			case "instance":
				backend.mu.Lock()
				backend.inspectError = errors.New("identity unknown")
				backend.mu.Unlock()
			}
			if err != nil {
				t.Fatal(err)
			}
			r, collectErr := e.CollectGarbage(context.Background(), GCOptions{})
			if collectErr == nil && damage != "instance" {
				t.Fatalf("damaged roots allowed collection: %+v", r)
			}
			gcExists(t, old, true)
			if len(r.Retained) == 0 {
				t.Fatal("missing diagnostic")
			}
		})
	}
}

func TestGCCaptureBoundaryIdentityAndDryRun(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprint(replace), func(t *testing.T) {
			e, b := openGCTest(t, nil)
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
			v, _ := e.loadVM(context.Background(), vm.VMID)
			if err := os.Remove(
				filepath.Join(v.Resource.ImageDir, "image.json"),
			); err != nil {
				t.Fatal(err)
			}
			op, err := e.Submit(
				context.Background(),
				api.Request{Key: "capture", Action: "snapshot", VMID: vm.VMID},
			)
			if err != nil {
				t.Fatal(err)
			}
			if final := awaitStatus(t, e, op.ID); final.Status != "failed" {
				t.Fatal(final)
			}
			if err = writeJSON(
				filepath.Join(v.Resource.ImageDir, "image.json"),
				b.image,
			); err != nil {
				t.Fatal(err)
			}
			wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: vm.VMID})
			path := filepath.Join(v.Resource.Dir, "capture", op.ID)
			if replace {
				// Preserve the original outside the inventory while substituting an
				// otherwise identical-looking directory at the journaled pathname.
				retained := filepath.Join(t.TempDir(), "original")
				if err = os.Rename(path, retained); err != nil {
					t.Fatal(err)
				}
				if err = os.Mkdir(path, 0o711); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			gcPass(t, e, GCOptions{DryRun: true})
			after, err := os.Stat(path)
			if err != nil || gcNode(before) != gcNode(after) ||
				!before.ModTime().Equal(after.ModTime()) {
				t.Fatalf("dry run changed capture: %v", err)
			}
			r := gcPass(t, e, GCOptions{})
			gcExists(t, path, replace)
			if replace && len(r.Retained) == 0 {
				t.Fatal("replacement was not diagnosed")
			}
		})
	}
}

func TestGCDeletesOwnedFailedCaptureButKeepsUnknownArtifact(t *testing.T) {
	e, backend := openGCTest(t, nil)
	vm := wait(
		t,
		e,
		api.Request{Key: "create", Action: "create", Config: config()},
	)
	wait(t, e, api.Request{Key: "start", Action: "start", VMID: vm.VMID})
	backend.mu.Lock()
	backend.states[""] = Observation{}
	backend.mu.Unlock()
	// Failing image metadata after capture leaves a terminal, resumed capture.
	v, _ := e.loadVM(context.Background(), vm.VMID)
	if err := os.Remove(
		filepath.Join(v.Resource.ImageDir, "image.json"),
	); err != nil {
		t.Fatal(err)
	}
	o, err := e.Submit(
		context.Background(),
		api.Request{Key: "failed-snapshot", Action: "snapshot", VMID: vm.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if op := awaitStatus(t, e, o.ID); op.Status != "failed" {
		t.Fatal("snapshot did not fail", op)
	}
	if err = writeJSON(
		filepath.Join(v.Resource.ImageDir, "image.json"),
		backend.image,
	); err != nil {
		t.Fatal(err)
	}
	wait(t, e, api.Request{Key: "stop", Action: "stop", VMID: vm.VMID})
	capture := filepath.Join(v.Resource.Dir, "capture", o.ID)
	unknown := filepath.Join(capture, "unknown")
	if err = os.WriteFile(
		unknown,
		[]byte("operator evidence"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	r := gcPass(t, e, GCOptions{})
	gcExists(t, capture, true)
	if len(r.Retained) == 0 {
		t.Fatal("unknown artifact was not diagnosed")
	}
	if err = os.Remove(unknown); err != nil {
		t.Fatal(err)
	}
	gcPass(t, e, GCOptions{})
	gcExists(t, capture, false)
	// Unpublished snapshot metadata and its idempotent failure remain.
	if _, err = e.GetSnapshot(context.Background(), o.SnapshotID); err != nil {
		t.Fatal(err)
	}
}

func TestGCPreservesOpenAndMappedRetiredFiles(t *testing.T) {
	e, _ := openGCTest(t, nil)
	_, _, old := gcStoppedRestore(t, e)
	f, err := os.Open(old)
	if err != nil {
		t.Fatal(err)
	}
	gcPass(t, e, GCOptions{})
	gcExists(t, old, true)
	m, err := unix.Mmap(int(f.Fd()), 0, 1, unix.PROT_READ, unix.MAP_PRIVATE)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	gcPass(t, e, GCOptions{})
	gcExists(t, old, true)
	if err = unix.Munmap(m); err != nil {
		t.Fatal(err)
	}
	gcPass(t, e, GCOptions{})
	gcExists(t, old, false)
}

func TestGCAndImportGuardsDrainBeforeClose(t *testing.T) {
	e, _ := openGCTest(t, nil)
	e.artifacts.Lock()
	if _, err := e.CollectGarbage(
		context.Background(),
		GCOptions{},
	); err == nil {
		t.Fatal("collector bypassed import guard")
	}
	imported := make(chan error, 1)
	go func() {
		_, err := e.ImportOCI(context.Background(), ImportOptions{})
		imported <- err
	}()
	select {
	case <-imported:
		t.Fatal("import bypassed artifact guard")
	case <-time.After(20 * time.Millisecond):
	}
	e.artifacts.Unlock()
	if err := <-imported; err == nil {
		t.Fatal("invalid import accepted")
	}
	// openGCTest owns Close in cleanup; exercise drain in a separate engine.
	dir := t.TempDir()
	other := openTest(t, dir, newTestBackend(), true, nil)
	other.artifacts.Lock()
	closed := make(chan error, 1)
	go func() { closed <- other.Close() }()
	select {
	case <-closed:
		t.Fatal("Close bypassed artifact guard")
	case <-time.After(20 * time.Millisecond):
	}
	other.artifacts.Unlock()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}

func TestGCConcurrentSnapshotCloneDelete(t *testing.T) {
	e, backend := openGCTest(t, nil)
	vm, s := gcSource(t, e)
	gate := make(chan struct{})
	backend.captureGate = gate
	op, err := e.Submit(
		context.Background(),
		api.Request{Key: "snapshot-blocked", Action: "snapshot", VMID: vm.VMID},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitStep(t, e, op.ID, "paused")
	if _, err = e.CollectGarbage(
		context.Background(),
		GCOptions{},
	); err == nil {
		t.Fatal("GC crossed an active capture lifecycle lock")
	}
	close(gate)
	if result := awaitStatus(t, e, op.ID); result.Status != "succeeded" {
		t.Fatal(result)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			child, err := api.Wait(
				ctx,
				e,
				api.Request{
					Key:        "concurrent-clone-" + strconv.Itoa(i),
					Action:     "clone",
					SnapshotID: s.SnapshotID,
				},
			)
			if err == nil {
				_, err = api.Wait(
					ctx,
					e,
					api.Request{
						Key:    "concurrent-delete-" + strconv.Itoa(i),
						Action: "delete",
						VMID:   child.VMID,
					},
				)
			}
			errCh <- err
		}(i)
	}
	for range 20 {
		_, err := e.CollectGarbage(context.Background(), GCOptions{})
		var a *api.Error
		if err != nil && (!errors.As(err, &a) || a.Code != api.Conflict) {
			t.Fatal(err)
		}
		runtime.Gosched()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	gcPass(t, e, GCOptions{})
}

func TestGCActualLayerCacheAndCompletedImport(t *testing.T) {
	e, layout, layers, published := importFixture(t)
	orphan := putOCILayer(
		t,
		layout,
		[]tarEntry{fileEntry("orphan", "data")},
		false,
	)
	file, err := blob(layout, orphan.descriptor.Digest.String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = buildLayerCache(
		context.Background(),
		e.opts.StateDir,
		file,
		orphan.descriptor.MediaType,
		published.Builder,
		orphan.diff.String(),
		10240,
		1,
	); err != nil {
		t.Fatal(err)
	}
	orphanPath := filepath.Join(
		e.opts.StateDir,
		filepath.Dir(
			layerRelativePath(published.Builder, orphan.diff.String()),
		),
	)
	unknown := filepath.Join(
		e.opts.StateDir,
		"layer-cache",
		published.Builder[7:],
		".pending-unknown",
	)
	if err = os.Mkdir(unknown, 0o700); err != nil {
		t.Fatal(err)
	}
	gcPass(t, e, GCOptions{DryRun: true})
	gcExists(t, orphanPath, true)
	r := gcPass(t, e, GCOptions{})
	gcExists(t, orphanPath, false)
	gcExists(t, unknown, true)
	gcExists(
		t,
		filepath.Join(e.opts.StateDir, "imports", published.Digest[7:]),
		false,
	)
	if len(r.Retained) == 0 {
		t.Fatal("unknown stage not reported")
	}
	if err = published.Verify(
		filepath.Join(e.opts.StateDir, "images", published.Digest[7:]),
		published.Digest,
	); err != nil {
		t.Fatal("GC broke published layer cache", err)
	}
	// Serialized import and collection race over a cache hit before publication.
	second := putOCIManifest(t, layout, "gc-concurrent", layers)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			_, err := e.ImportOCI(
				context.Background(),
				ImportOptions{
					Layout:   layout,
					Digest:   second,
					MaxBytes: 4 << 20,
					MaxFiles: 1000,
				},
			)
			if err != nil {
				t.Error(err)
			}
		})
	}
	for range 10 {
		_, err = e.CollectGarbage(context.Background(), GCOptions{})
		var a *api.Error
		if err != nil && (!errors.As(err, &a) || a.Code != api.Conflict) {
			t.Error(err)
		}
	}
	wg.Wait()
}

func TestGCBackgroundAndCancellation(t *testing.T) {
	e, _ := openGCTest(t, nil, 10*time.Millisecond)
	vm, _, old := gcStoppedRestore(t, e)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Lstat(old); os.IsNotExist(err) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"background collection did not progress: %+v",
				e.LastGCReport(),
			)
		}
		time.Sleep(5 * time.Millisecond)
	}
	v, _ := e.loadVM(context.Background(), vm.VMID)
	gcExists(t, v.Resource.Disk, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := e.CollectGarbage(ctx, GCOptions{}); err == nil {
		t.Fatal("canceled pass succeeded")
	}
}

func TestGCUnknownCacheOwnershipIsNeverDeletedByAge(t *testing.T) {
	e, _ := openGCTest(t, nil)
	parent := filepath.Join(
		e.opts.StateDir,
		"layer-cache",
		strings.Repeat("b", 64),
	)
	for _, name := range []string{strings.Repeat("c", 64), ".pending-unknown"} {
		path := filepath.Join(parent, name)
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(
			filepath.Join(path, "layer.erofs"),
			[]byte("unproven"),
			0o444,
		); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(
			path,
			time.Unix(0, 0),
			time.Unix(0, 0),
		); err != nil {
			t.Fatal(err)
		}
	}
	r := gcPass(t, e, GCOptions{})
	if len(r.Removed) != 0 || len(r.Retained) != 2 {
		t.Fatalf("unknown old cache was not retained: %+v", r)
	}
	gcExists(
		t,
		filepath.Join(parent, strings.Repeat("c", 64), "layer.erofs"),
		true,
	)
}

func TestGCBoundedBatchAndDurableDeletionRetry(t *testing.T) {
	var interrupt bool
	e, _ := openGCTest(t, func(step string) error {
		if interrupt && step == "gc_delete_intent" {
			return ErrInterrupted
		}
		return nil
	})
	_, _, old := gcStoppedRestore(t, e)
	interrupt = true
	if _, err := e.CollectGarbage(
		context.Background(),
		GCOptions{MaxArtifacts: 1},
	); !errors.Is(
		err,
		ErrInterrupted,
	) {
		t.Fatal("missing interruption", err)
	}
	gcExists(t, old, true)
	var n int
	if err := e.db.QueryRow("SELECT COUNT(*) FROM gc_deletions").
		Scan(&n); err != nil ||
		n != 1 {
		t.Fatal("missing durable deletion intent", n, err)
	}
	// Simulate the unlink succeeding just before daemon death; retry must finish
	// metadata retirement even though its target no longer exists.
	if err := os.Remove(old); err != nil {
		t.Fatal(err)
	}
	interrupt = false
	r := gcPass(t, e, GCOptions{MaxArtifacts: 1})
	if len(r.Removed) != 1 || r.Removed[0] != old {
		t.Fatalf("deletion retry failed: %+v", r)
	}
	if err := e.db.QueryRow("SELECT COUNT(*) FROM gc_deletions").
		Scan(&n); err != nil ||
		n != 0 {
		t.Fatal("deletion journal retained", n, err)
	}
	// Never treat an inode replacement at an owned path as the original object.
	if err := os.WriteFile(old, []byte("retired"), 0o600); err != nil {
		t.Fatal(err)
	}
	interrupt = true
	if _, err := e.CollectGarbage(
		context.Background(),
		GCOptions{},
	); !errors.Is(
		err,
		ErrInterrupted,
	) {
		t.Fatal(err)
	}
	moved := filepath.Join(t.TempDir(), "old-inode")
	if err := os.Rename(old, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		old,
		[]byte("unknown replacement"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	interrupt = false
	r = gcPass(t, e, GCOptions{})
	gcExists(t, old, true)
	if len(r.Retained) == 0 ||
		!strings.Contains(r.Retained[0].Reason, "inode") {
		t.Fatalf("replacement not diagnosed: %+v", r)
	}
}

func TestGCRetainsBlockedRestoreMaterials(t *testing.T) {
	for _, boundary := range []string{"restore_candidate_copied", "restore_candidate_synced", "restore_materials_complete", "restore_resources_ready"} {
		t.Run(boundary, func(t *testing.T) {
			var armed atomic.Bool
			e, _ := openGCTest(t, func(step string) error {
				if armed.Load() && step == boundary {
					return errors.New("candidate delivery failed")
				}
				return nil
			})
			_, source := gcSource(t, e)
			armed.Store(true)
			op, err := e.Submit(
				context.Background(),
				api.Request{
					Key:        "blocked-clone",
					Action:     "clone",
					SnapshotID: source.SnapshotID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if final := awaitStatus(t, e, op.ID); final.Status != "failed" {
				t.Fatalf("clone did not fail: %+v", final)
			}
			lock := e.vmLock(op.VMID)
			lock.Lock()
			record, err := e.loadOp(context.Background(), op.ID)
			lock.Unlock()
			if err != nil || !record.Journal.RestoreBlocked {
				t.Fatalf("restore not fenced: %+v %v", record.Journal, err)
			}
			version := filepath.Dir(record.Journal.Resource.Disk)
			for _, dry := range []bool{true, false} {
				report := gcPass(t, e, GCOptions{DryRun: dry})
				for _, path := range append(report.Candidates, report.Removed...) {
					if path == version || strings.HasPrefix(path, version+"/") {
						t.Fatalf(
							"blocked version became collectible: %+v",
							report,
						)
					}
				}
				gcExists(t, version, true)
			}
			vm, err := e.GetVM(context.Background(), op.VMID)
			if err != nil {
				t.Fatal(err)
			}
			wait(
				t,
				e,
				api.Request{
					Key:              "explicit-delete",
					Action:           "delete",
					VMID:             op.VMID,
					ExpectedInstance: vm.Instance,
				},
			)
			gcExists(t, version, false)
			wait(
				t,
				e,
				api.Request{
					Key:        "source-delete",
					Action:     "delete_snapshot",
					SnapshotID: source.SnapshotID,
				},
			)
			gcPass(t, e, GCOptions{})
		})
	}
}
