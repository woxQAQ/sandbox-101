package haven

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/loom"
)

func TestHavenSnapshotUntrustedWriterAndIndependence(t *testing.T) {
	parallelLifecycle(t)
	for _, damage := range []string{"extra-file", "symlink", "oversize", "writer-after-copy"} {
		t.Run(damage, func(t *testing.T) {
			var service *Service
			var modified bool
			var sourcePath string
			var setupErr error
			var backend *coldRuntime
			service, backend = coldService(t, func(phase string) error {
				if modified {
					return nil
				}
				if damage == "writer-after-copy" {
					if phase != "capture_materialized" {
						return nil
					}
				} else if phase != "source_resumed" {
					return nil
				}
				var body []byte
				var id string
				if err := service.store.db.QueryRow("SELECT operation_id FROM operation_steps WHERE action='snapshot'").Scan(&id); err != nil {
					return err
				}
				if err := service.store.db.QueryRow("SELECT input FROM operation_steps WHERE operation_id=? AND step_key='capture'", id).Scan(&body); err != nil {
					return err
				}
				step, err := loadCapture(context.Background(), service.store.db, id)
				if err != nil {
					return err
				}
				sourcePath = filepath.Join(step.State.Boundary.Path, "ch", "memory-ranges")
				modified = true
				switch damage {
				case "extra-file":
					setupErr = os.WriteFile(filepath.Join(step.State.Boundary.Path, "ch", "unknown"), []byte("unknown fixture"), 0o600)
				case "symlink":
					setupErr = os.Remove(sourcePath)
					if setupErr == nil {
						setupErr = os.Symlink(step.Input.Source.Input.Resource.Disk, sourcePath)
					}
				case "oversize":
					setupErr = os.Truncate(sourcePath, (step.Input.Source.Input.Resource.Config.Shape.MemoryMiB+1)<<20)
				case "writer-after-copy":
					setupErr = os.WriteFile(sourcePath, []byte("changed writer"), 0o600)
				}
				return setupErr
			})
			operation, _ := captureReady(t, service, "writer")
			step := awaitCapture(t, service, operation.ID)
			if setupErr != nil || !modified {
				t.Fatal("writer fixture did not execute", setupErr)
			}
			capturePublished(t, service, step, damage == "writer-after-copy")
			observation, err := backend.Inspect(context.Background(), step.Input.Source.Input.Resource)
			if err != nil || observation.Paused || !observation.Alive {
				t.Fatal("untrusted capture stranded the source", err)
			}
			if damage == "writer-after-copy" {
				data, err := os.ReadFile(filepath.Join(step.Input.Snapshot.Dir, "ch", "memory-ranges"))
				if err != nil || string(data) != "frozen memory" {
					t.Fatal("writer reached published inode", err)
				}
				writer, err := os.Stat(sourcePath)
				if err != nil {
					t.Fatal(err)
				}
				published, err := os.Stat(filepath.Join(step.Input.Snapshot.Dir, "ch", "memory-ranges"))
				if err != nil {
					t.Fatal(err)
				}
				if os.SameFile(writer, published) {
					t.Fatal("capture publication retained a mutable writer inode")
				}
			}
		})
	}
}

func TestHavenSnapshotShutdownResumesSource(t *testing.T) {
	parallelLifecycle(t)
	service, backend := coldService(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	backend.before = func(name string, _ loom.Resource) error {
		if name == "capture" {
			close(entered)
			<-release
		}
		return nil
	}
	operation, _ := captureReady(t, service, "shutdown")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("capture did not enter host boundary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown did not retain bounded caller wait", err)
	}
	unblock.Do(func() { close(release) })
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.before = nil
	backend.mu.Lock()
	for _, observation := range backend.observations {
		if observation.Paused {
			t.Error("shutdown canceled required source resume")
		}
	}
	backend.mu.Unlock()
	opts := service.opts
	opts.Initialize = false
	opts.Fault = nil
	resumed, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resumed.Close(context.Background()) })
	step := awaitCapture(t, resumed, operation.ID)
	if step.Status != "failed" || backend.count("capture") != 1 || backend.count("launch") != 1 {
		t.Fatal("shutdown recaptured unproved bytes or restarted source", step.State)
	}
	capturePublished(t, resumed, step, false)
}

func TestHavenSnapshotOwnerIsPermanent(t *testing.T) {
	parallelLifecycle(t)
	service, _ := coldService(t, nil)
	operation, _ := captureReady(t, service, "owner")
	step := awaitCapture(t, service, operation.ID)
	capturePublished(t, service, step, true)
	if _, err := service.store.db.Exec("UPDATE snapshot_owners SET tenant_id='other' WHERE snapshot_id=?", step.Input.Snapshot.ID); err == nil {
		t.Fatal("snapshot authorization owner transferred")
	}
	if _, err := service.store.db.Exec("DELETE FROM snapshot_owners WHERE snapshot_id=?", step.Input.Snapshot.ID); err == nil {
		t.Fatal("snapshot authorization history deleted")
	}
	if _, err := service.GetSnapshot(context.Background(), "other", step.Input.Snapshot.ID); err == nil {
		t.Fatal("foreign tenant could read capture")
	}
}
