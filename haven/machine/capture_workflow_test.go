package machine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenSnapshotUntrustedWriterAndIndependence(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, damage := range []string{
		"extra-file",
		"symlink",
		"oversize",
		"writer-after-copy",
	} {
		t.Run(damage, func(t *testing.T) {
			var service *application.App
			var modified bool
			var sourcePath string
			var setupErr error
			var backend *testfixture.ColdRuntime
			service, backend = testapp.ColdService(t, func(phase string) error {
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
				if err := service.State.Database().QueryRow("SELECT operation_id FROM operation_steps WHERE action='snapshot'").Scan(&id); err != nil {
					return err
				}
				if err := service.State.Database().QueryRow("SELECT input FROM operation_steps WHERE operation_id=? AND step_key='capture'", id).Scan(&body); err != nil {
					return err
				}
				step, err := machine.LoadCapture(context.Background(), service.State.Database(), id)
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
			operation, _ := testfixture.CaptureReady(t, service, "writer")
			step := testutil.AwaitCapture(t, service, operation.ID)
			if setupErr != nil || !modified {
				t.Fatal("writer fixture did not execute", setupErr)
			}
			testutil.CapturePublished(t, service, step, damage == "writer-after-copy")
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
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() {
		unblock.Do(func() {
			close(release)
		})
	})
	backend.Before = func(name string, _ loom.Resource) error {
		if name == "capture" {
			close(entered)
			<-release
		}
		return nil
	}
	operation, _ := testfixture.CaptureReady(t, service, "shutdown")
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
	unblock.Do(func() {
		close(release)
	})
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend.Before = nil
	backend.Mu.Lock()
	for _, observation := range backend.Observations {
		if observation.Paused {
			t.Error("shutdown canceled required source resume")
		}
	}
	backend.Mu.Unlock()
	opts := service.Options
	opts.Initialize = false
	opts.Fault = nil
	resumed, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = resumed.Close(context.Background())
	})
	step := testutil.AwaitCapture(t, resumed, operation.ID)
	if step.Status != "failed" || backend.Count("capture") != 1 || backend.Count("launch") != 1 {
		t.Fatal("shutdown recaptured unproved bytes or restarted source", step.State)
	}
	testutil.CapturePublished(t, resumed, step, false)
}

func TestHavenSnapshotOwnerIsPermanent(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _ := testapp.ColdService(t, nil)
	operation, _ := testfixture.CaptureReady(t, service, "owner")
	step := testutil.AwaitCapture(t, service, operation.ID)
	testutil.CapturePublished(t, service, step, true)
	if _, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), "other", step.Input.Snapshot.ID); err == nil {
		t.Fatal("foreign tenant could read capture")
	}
}

func TestHavenSnapshotCompletionRequiresRunningStep(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, nil)
	// The private trigger changes the step after proof loading, inside the
	// publication transaction. Completion must still require a running step.
	if _, err := service.State.Database().Exec(`CREATE TRIGGER change_capture_status
AFTER UPDATE OF published ON snapshots WHEN NEW.published=1 BEGIN
 UPDATE operation_steps SET status='pending'
 WHERE operation_id=json_extract(NEW.body,'$.operation_id') AND step_key='capture';
END`); err != nil {
		t.Fatal(err)
	}
	accepted, _ := testfixture.CaptureReady(t, service, "running-fence")
	step := testutil.AwaitCapture(t, service, accepted.ID)
	if step.Status != "running" || step.State.Phase != "publish_intent" {
		t.Fatalf("completion crossed running fence or failed to roll back: status=%s phase=%s", step.Status, step.State.Phase)
	}
	testutil.CapturePublished(t, service, step, false)
	var checkpoints int
	if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM checkpoints").Scan(&checkpoints); err != nil || checkpoints != 0 {
		t.Fatal("checkpoint escaped rejected completion", checkpoints, err)
	}
	if _, err := service.State.Database().Exec("DROP TRIGGER change_capture_status"); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	step = testutil.AwaitCapture(t, service, accepted.ID)
	testutil.CapturePublished(t, service, step, true)
	if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
		t.Fatal("completion retry repeated a host effect")
	}
}

func TestHavenSnapshotDurability(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, point := range []string{
		"capture_admitted",
		"capture_prepared",
		"pause_intent",
		"paused",
		"captured",
		"source_resumed",
		"capture_copied",
		"capture_materialized",
		"artifacts_synced",
		"publish_intent",
		"capture_completion_before_commit",
		"capture_completed",
	} {
		t.Run(point, func(t *testing.T) {
			service, backend := testapp.ColdService(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation, request := testfixture.CaptureReady(t, service, "capture")
			var before machine.CaptureStep
			var err error
			if point == "capture_admitted" {
				before, err = machine.LoadCapture(context.Background(), service.State.Database(), acceptedOperation.ID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				before = testutil.AwaitCapture(t, service, acceptedOperation.ID)
			}
			service = testapp.ReopenCloneService(t, service, backend)
			after := testutil.AwaitCapture(t, service, acceptedOperation.ID)
			proved := point == "capture_admitted" || point == "capture_prepared" || point == "capture_materialized" || point == "artifacts_synced" || point == "publish_intent" || point == "capture_completion_before_commit" || point == "capture_completed"
			testutil.CapturePublished(t, service, after, proved)
			if after.Input.Snapshot.ID != before.Input.Snapshot.ID || after.Input.Request != request || backend.Count("launch") != 1 || backend.Count("capture") > 1 {
				t.Fatal("capture recovery changed candidate or repeated capture")
			}
			observation, err := backend.Inspect(context.Background(), after.Input.Source.Input.Resource)
			if err != nil || !observation.Alive || observation.Paused {
				t.Fatal("capture left source paused or restarted", err)
			}
			replay, err := service.Captures.AdmitCapture(context.Background(), request)
			if err != nil || replay.ID != acceptedOperation.ID || replay.Status != after.Status {
				t.Fatal("capture key did not retain its outcome", err)
			}
		})
	}
}

func TestHavenSnapshotRejectsUnprovenBytes(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, damage := range []string{
		"truncate-disk",
		"zero-memory",
		"missing-memory",
		"staging-inode",
		"missing-checksums",
		"missing-boundary",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := testapp.ColdService(t, func(point string) error {
				if point == "capture_materialized" {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation, _ := testfixture.CaptureReady(t, service, "damage")
			step := testutil.AwaitCapture(t, service, acceptedOperation.ID)
			stage := filepath.Join(step.State.Boundary.Path, "publication")
			switch damage {
			case "truncate-disk":
				if err := os.Truncate(filepath.Join(stage, "disk.raw"), 0); err != nil {
					t.Fatal(err)
				}
			case "zero-memory":
				if err := os.WriteFile(filepath.Join(stage, "ch", "memory-ranges"), make([]byte, len("frozen memory")), 0o600); err != nil {
					t.Fatal(err)
				}
			case "missing-memory":
				if err := os.Remove(filepath.Join(stage, "ch", "memory-ranges")); err != nil {
					t.Fatal(err)
				}
			case "staging-inode":
				if err := os.Rename(stage, filepath.Join(t.TempDir(), "original-stage")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(stage, 0o700); err != nil {
					t.Fatal(err)
				}
			case "missing-checksums":
				state := step.State
				state.Files = nil
				if _, err := service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='capture'", operation.Encode(state), step.OperationID); err != nil {
					t.Fatal(err)
				}
			case "missing-boundary":
				state := step.State
				state.Boundary = nil
				if _, err := service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='capture'", operation.Encode(state), step.OperationID); err != nil {
					t.Fatal(err)
				}
			}
			service = testapp.ReopenCloneService(t, service, backend)
			after := testutil.AwaitCapture(t, service, acceptedOperation.ID)
			if after.Status != "failed" {
				t.Fatal("altered or unproved capture did not fail", after.State)
			}
			testutil.CapturePublished(t, service, after, false)
			if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
				t.Fatal("recovery rebuilt lost capture bytes")
			}
			if _, err := os.Lstat(step.State.Boundary.Path); err != nil {
				t.Fatal("damaged materials were removed", err)
			}
		})
	}
}

func TestHavenSnapshotAtomicPublication(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"UPDATE", "snapshots", "NEW.published=1"},
		{"INSERT", "checkpoints", "1"},
		{"INSERT", "quota_resources", "NEW.kind='snapshot'"},
		{"DELETE", "quota_refs", "OLD.kind='pending_storage'"},
		{"UPDATE", "vms", "NEW.operation_id IS NULL"},
		{"UPDATE", "operation_steps", "NEW.action='snapshot' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture publication rejection'",
			"IGNORE",
		} {
			t.Run(write.table+"/"+strings.Split(mode, ",")[0], func(t *testing.T) {
				service, backend := testapp.ColdService(t, nil)
				claim := testfixture.ColdClaim(t, service, "source")
				source := testutil.AwaitColdWorker(t, service, claim.ID)
				if source.Status != "succeeded" {
					t.Fatal(source.State)
				}
				statement := fmt.Sprintf("CREATE TRIGGER deny_snapshot BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				acceptedOperation, err := service.Captures.AdmitCapture(context.Background(), machine.CaptureRequest{
					TenantID:         "tenant",
					Key:              "capture",
					SandboxID:        claim.Result.SandboxID,
					ExpectedInstance: source.State.Witness,
				})
				if err != nil {
					t.Fatal(err)
				}
				step := testutil.AwaitCapture(t, service, acceptedOperation.ID)
				if step.State.Phase != "publish_intent" || testutil.Terminal(step.Status) {
					t.Fatal("rejected publication fixed a terminal result", step.State)
				}
				testutil.CapturePublished(t, service, step, false)
				var checkpoints int
				if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM checkpoints").Scan(&checkpoints); err != nil || checkpoints != 0 {
					t.Fatal("checkpoint leaked outside publication transaction", err)
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_snapshot"); err != nil {
					t.Fatal(err)
				}
				service = testapp.ReopenCloneService(t, service, backend)
				step = testutil.AwaitCapture(t, service, acceptedOperation.ID)
				testutil.CapturePublished(t, service, step, true)
				if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
					t.Fatal("publication retry repeated a host capture")
				}
			})
		}
	}
}

func TestHavenSnapshotAdmissionAndShutdown(t *testing.T) {
	testutil.ParallelLifecycle(t)
	t.Run("peak-quota-before-pause", func(t *testing.T) {
		service, backend := testapp.ColdService(t, nil)
		claim := testfixture.ColdClaim(t, service, "source")
		source := testutil.AwaitColdWorker(t, service, claim.ID)
		if _, err := service.State.Database().Exec("UPDATE tenants SET max_storage_mib=255 WHERE id='tenant'"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Captures.AdmitCapture(context.Background(), machine.CaptureRequest{
			TenantID:         "tenant",
			Key:              "exhausted",
			SandboxID:        claim.Result.SandboxID,
			ExpectedInstance: source.State.Witness,
		}); err == nil {
			t.Fatal("capture exceeded tenant quota")
		}
		if backend.Count("pause") != 0 {
			t.Fatal("quota refusal happened after pause")
		}
	})
	t.Run("admission-ignore", func(t *testing.T) {
		service, backend := testapp.ColdService(t, nil)
		claim := testfixture.ColdClaim(t, service, "source")
		source := testutil.AwaitColdWorker(t, service, claim.ID)
		if _, err := service.State.Database().Exec("CREATE TRIGGER deny_capture_owner BEFORE INSERT ON snapshot_owners BEGIN SELECT RAISE(IGNORE); END"); err != nil {
			t.Fatal(err)
		}
		if _, err := service.Captures.AdmitCapture(context.Background(), machine.CaptureRequest{
			TenantID:         "tenant",
			Key:              "deny",
			SandboxID:        claim.Result.SandboxID,
			ExpectedInstance: source.State.Witness,
		}); err == nil {
			t.Fatal("ignored owner registration was accepted")
		}
		var snapshots int
		if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM snapshots").Scan(&snapshots); err != nil || snapshots != 0 {
			t.Fatal("partial capture admission", err)
		}
		if backend.Count("pause") != 0 {
			t.Fatal("uncommitted capture paused source")
		}
	})
}
