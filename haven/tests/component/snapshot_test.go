package component

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
)

func captureReady(t *testing.T, service *application.App, key string) (operation.Record, machine.CaptureRequest) {
	t.Helper()
	claim := coldClaim(t, service, "source-"+key)
	source := awaitColdWorker(t, service, claim.ID)
	if source.Status != "succeeded" {
		t.Fatal(source.State)
	}
	request := machine.CaptureRequest{
		TenantID:         "tenant",
		Key:              key,
		SandboxID:        claim.Result.SandboxID,
		ExpectedInstance: source.State.Witness,
	}
	acceptedOperation, err := service.Captures.AdmitCapture(context.Background(), request)
	if err != nil && !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	return acceptedOperation, request
}

func awaitCapture(t *testing.T, service *application.App, id string) machine.CaptureStep {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		worker := service.Workers.Snapshot(id)
		active, last, seen := worker.Active, worker.Err, worker.Completed
		step, err := machine.LoadCapture(context.Background(), service.State.Database(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !active && (seen || terminal(step.Status)) {
			return step
		}
		if time.Now().After(deadline) {
			t.Fatalf("capture worker did not stop: %+v %v", step.State, last)
		}
		time.Sleep(time.Millisecond)
	}
}

func capturePublished(t *testing.T, service *application.App, step machine.CaptureStep, want bool) {
	t.Helper()
	snapshot, err := machine.LoadOwnedSnapshot(context.Background(), service.State.Database(), step.Input.Request.TenantID, step.Input.Snapshot.ID)
	if err != nil {
		t.Fatal(err)
	}
	if want != (snapshot.Manifest != nil) || want != (step.Status == "succeeded") {
		t.Fatalf("snapshot and step publication disagree: status=%s manifest=%v phase=%s", step.Status, snapshot.Manifest != nil, step.State.Phase)
	}
	var storage, pending, charged int64
	if err = service.State.Database().QueryRow("SELECT COALESCE(SUM(storage_mib),0),COALESCE(SUM(kind='pending_storage'),0),COALESCE(SUM(kind='snapshot'),0) FROM quota_resources WHERE source_operation_id=?", step.OperationID).Scan(&storage, &pending, &charged); err != nil {
		t.Fatal(err)
	}
	if storage != 192 || want && (charged != 1 || pending != 0) || !want && (charged != 0 || pending != 1) {
		t.Fatal("capture lost or partially transferred its liability", storage, pending, charged)
	}
	if want {
		actual, err := loom.HashTree(snapshot.Dir)
		if err != nil || !reflect.DeepEqual(actual, snapshot.Manifest.Files) {
			t.Fatal("published bytes differ from fixed proof", err)
		}
	}
}

func TestHavenSnapshotDurability(t *testing.T) {
	parallelLifecycle(t)
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
			service, backend := coldService(t, func(candidate string) error {
				if candidate == point {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation, request := captureReady(t, service, "capture")
			var before machine.CaptureStep
			var err error
			if point == "capture_admitted" {
				before, err = machine.LoadCapture(context.Background(), service.State.Database(), acceptedOperation.ID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				before = awaitCapture(t, service, acceptedOperation.ID)
			}
			service = resumeColdService(t, service, backend)
			after := awaitCapture(t, service, acceptedOperation.ID)
			proved := point == "capture_admitted" || point == "capture_prepared" || point == "capture_materialized" || point == "artifacts_synced" || point == "publish_intent" || point == "capture_completion_before_commit" || point == "capture_completed"
			capturePublished(t, service, after, proved)
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
	parallelLifecycle(t)
	for _, damage := range []string{
		"truncate-disk",
		"zero-memory",
		"missing-memory",
		"staging-inode",
		"missing-checksums",
		"missing-boundary",
	} {
		t.Run(damage, func(t *testing.T) {
			service, backend := coldService(t, func(point string) error {
				if point == "capture_materialized" {
					return errs.ErrInterrupted
				}
				return nil
			})
			acceptedOperation, _ := captureReady(t, service, "damage")
			step := awaitCapture(t, service, acceptedOperation.ID)
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
				if _, err := service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='capture'", testfixture.Encode(state), step.OperationID); err != nil {
					t.Fatal(err)
				}
			case "missing-boundary":
				state := step.State
				state.Boundary = nil
				if _, err := service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='capture'", testfixture.Encode(state), step.OperationID); err != nil {
					t.Fatal(err)
				}
			}
			service = resumeColdService(t, service, backend)
			after := awaitCapture(t, service, acceptedOperation.ID)
			if after.Status != "failed" {
				t.Fatal("altered or unproved capture did not fail", after.State)
			}
			capturePublished(t, service, after, false)
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
	parallelLifecycle(t)
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
				service, backend := coldService(t, nil)
				claim := coldClaim(t, service, "source")
				source := awaitColdWorker(t, service, claim.ID)
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
				step := awaitCapture(t, service, acceptedOperation.ID)
				if step.State.Phase != "publish_intent" || terminal(step.Status) {
					t.Fatal("rejected publication fixed a terminal result", step.State)
				}
				capturePublished(t, service, step, false)
				var checkpoints int
				if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM checkpoints").Scan(&checkpoints); err != nil || checkpoints != 0 {
					t.Fatal("checkpoint leaked outside publication transaction", err)
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER deny_snapshot"); err != nil {
					t.Fatal(err)
				}
				service = resumeColdService(t, service, backend)
				step = awaitCapture(t, service, acceptedOperation.ID)
				capturePublished(t, service, step, true)
				if backend.Count("capture") != 1 || backend.Count("launch") != 1 {
					t.Fatal("publication retry repeated a host capture")
				}
			})
		}
	}
}

func TestHavenSnapshotAdmissionAndShutdown(t *testing.T) {
	parallelLifecycle(t)
	t.Run("peak-quota-before-pause", func(t *testing.T) {
		service, backend := coldService(t, nil)
		claim := coldClaim(t, service, "source")
		source := awaitColdWorker(t, service, claim.ID)
		if _, err := service.State.Database().Exec("UPDATE tenants SET body=? WHERE id='tenant'", testfixture.Encode(authorization.Limits{
			MaxSandboxes:  10,
			MaxStorageMiB: 255,
		})); err != nil {
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
		service, backend := coldService(t, nil)
		claim := coldClaim(t, service, "source")
		source := awaitColdWorker(t, service, claim.ID)
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
