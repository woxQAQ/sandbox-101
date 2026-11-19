package workflows_test

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

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

func TestHavenStepConcurrentReplayAndCancellation(t *testing.T) {
	testutil.ParallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	service, backend := testapp.ColdService(t, faults.Check)
	ids := make(chan string, 16)
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			operation, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
				TenantID:   "tenant",
				Key:        "concurrent",
				TemplateID: "cold",
			})
			if err != nil {
				t.Error(err)
				return
			}
			ids <- operation.ID
		})
	}
	workers.Wait()
	close(ids)
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("same key allocated multiple operations")
		}
	}
	if first == "" {
		t.Fatal("no operation admitted")
	}
	if step := testutil.AwaitColdWorker(t, service, first); step.Status != "succeeded" || backend.Count("launch") != 1 {
		t.Fatal("concurrent replay duplicated effects", step.State)
	}

	ctx, cancel := context.WithCancel(context.Background())
	faults.Set(func(point string) error {
		if point == "admission_committed" {
			cancel()
		}
		return nil
	})
	operation, err := service.Provisioning.AdmitCold(ctx, machine.FreshRequest{
		TenantID:   "tenant",
		Key:        "cancel-wait",
		TemplateID: "cold",
	})
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("caller was not canceled after admission")
	}
	if step := testutil.AwaitColdWorker(t, service, operation.ID); step.Status != "succeeded" {
		t.Fatal("caller cancellation canceled accepted work", step.State)
	}
}

func TestHavenStepMissingSourceAndQuotaRetainState(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, scenario := range []string{
		"quota-boundary",
		"missing-owner",
		"missing-tenant",
		"catalog-manifest-mismatch",
	} {
		t.Run(scenario, func(t *testing.T) {
			service, backend := testapp.ColdService(t, nil)
			switch scenario {
			case "quota-boundary":
				_, err := service.State.Database().Exec("UPDATE tenants SET body=? WHERE id='tenant'", testfixture.Encode(authorization.Limits{
					MaxSandboxes:  10,
					MaxStorageMiB: 63,
				}))
				if err != nil {
					t.Fatal(err)
				}
			case "missing-owner":
				if _, err := service.State.Database().Exec("DELETE FROM template_intents WHERE template_id='cold'"); err != nil {
					t.Fatal(err)
				}
			case "missing-tenant":
				if _, err := service.State.Database().Exec("DELETE FROM tenants WHERE id='tenant'"); err != nil {
					t.Fatal(err)
				}
			case "catalog-manifest-mismatch":
				manifest, err := image.LoadManifest(context.Background(), service.State.Database(), "sha256:"+strings.Repeat("a", 64))
				if err != nil {
					t.Fatal(err)
				}
				manifest.Tools = map[string]string{
					"unexpected": "fixture",
				}
				if err = loom.WriteJSON(filepath.Join(service.State.Directory(), "images", manifest.Digest[7:], "image.json"), manifest); err != nil {
					t.Fatal(err)
				}
			}
			var before int
			if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operation_steps").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if _, err := service.Provisioning.AdmitCold(context.Background(), machine.FreshRequest{
				TenantID:   "tenant",
				Key:        "denied",
				TemplateID: "cold",
			}); err == nil {
				t.Fatal("unproved admission accepted")
			}
			var count int
			if err := service.State.Database().QueryRow("SELECT COUNT(*) FROM operation_steps").Scan(&count); err != nil || count != before {
				t.Fatal("denied request created a step", count, err)
			}
			if backend.Count("prepare") != 0 || backend.Count("launch") != 0 {
				t.Fatal("denied request performed effects")
			}
		})
	}
}

func TestHavenInstanceRejectsReplacedPreparedDisk(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, func(point string) error {
		if point == "step_agent_ready" {
			return errs.ErrInterrupted
		}
		return nil
	})
	operation := testfixture.ColdClaim(t, service, "disk-proof")
	before := testutil.AwaitColdWorker(t, service, operation.ID)
	if before.State.DiskIdentity == nil {
		t.Fatal("prepared inode was not durably recorded")
	}
	if err := os.Rename(before.Input.Resource.Disk, filepath.Join(t.TempDir(), "original-inode")); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(before.Input.Resource.Disk, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	truncateErr := file.Truncate(before.Input.Resource.Config.Shape.DiskMiB << 20)
	if err = errors.Join(truncateErr, file.Close()); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	after := testutil.AwaitColdWorker(t, service, operation.ID)
	if after.Status == "succeeded" || after.State.Witness != "" || backend.Count("launch") != 1 {
		t.Fatal("replacement was recertified into a ready generation")
	}
	if *after.State.DiskIdentity != *before.State.DiskIdentity {
		t.Fatal("recovery regenerated the prepared proof")
	}
	coldResponsibility(t, service, operation, false)
}

func TestHavenStepReleaseAtomicRetirement(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, write := range []struct {
		event     string
		table     string
		condition string
	}{
		{"DELETE", "quota_refs", "OLD.kind='vm_disk'"},
		{"DELETE", "quota_resources", "OLD.kind='vm_disk'"},
		{"DELETE", "instances", "1"},
		{"DELETE", "vms", "1"},
		{"DELETE", "deletion_intents", "1"},
		{"UPDATE", "cleanup_intents", "NEW.status='done'"},
		{"UPDATE", "operation_steps", "NEW.action='release' AND NEW.status='succeeded'"},
		{"UPDATE", "operations", "NEW.status='succeeded'"},
	} {
		for _, mode := range []string{
			"ABORT,'fixture retirement rejection'",
			"IGNORE",
		} {
			t.Run(write.table+"/"+strings.Split(mode, ",")[0], func(t *testing.T) {
				service, backend := testapp.ColdService(t, nil)
				claim := testfixture.ColdClaim(t, service, "claim")
				if step := testutil.AwaitColdWorker(t, service, claim.ID); step.Status != "succeeded" {
					t.Fatal(step.State)
				}
				statement := fmt.Sprintf("CREATE TRIGGER reject_retirement BEFORE %s ON %s WHEN %s BEGIN SELECT RAISE(%s); END", write.event, write.table, write.condition, mode)
				if _, err := service.State.Database().Exec(statement); err != nil {
					t.Fatal(err)
				}
				release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       "release",
					SandboxID: claim.Result.SandboxID,
				})
				if err != nil {
					t.Fatal(err)
				}
				step := testutil.AwaitColdWorker(t, service, release.ID)
				if step.Status != "running" || step.State.Phase != "files_removed" {
					t.Fatalf("failed retirement fixed a terminal result: %+v", step.State)
				}
				if _, err = os.Lstat(step.Input.Resource.Dir); !os.IsNotExist(err) {
					t.Fatal("fixture did not reach actual file removal", err)
				}
				var slots, storage, vm, receipt, cleanup int
				if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM vms),(SELECT COUNT(*) FROM deletion_intents),(SELECT COUNT(*) FROM cleanup_intents WHERE status='pending')").Scan(&slots, &storage, &vm, &receipt, &cleanup); err != nil || slots != 1 || storage != 64 || vm != 1 || receipt != 1 || cleanup != 1 {
					t.Fatal("partial responsibility retirement", slots, storage, vm, receipt, cleanup, err)
				}
				if _, err = service.State.Database().Exec("DROP TRIGGER reject_retirement"); err != nil {
					t.Fatal(err)
				}
				service = testapp.ReopenCloneService(t, service, backend)
				if step = testutil.AwaitColdWorker(t, service, release.ID); step.Status != "succeeded" {
					t.Fatal("retirement did not resume original proof", step.State)
				}
				if backend.Count("launch") != 1 {
					t.Fatal("retirement replay recreated a VM")
				}
			})
		}
	}
}

func TestHavenStepReleaseRetainsForeignReference(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, func(point string) error {
		if point == "release_admitted" {
			return errs.ErrInterrupted
		}
		return nil
	})
	claim := testfixture.ColdClaim(t, service, "claim")
	if step := testutil.AwaitColdWorker(t, service, claim.ID); step.Status != "succeeded" {
		t.Fatal(step.State)
	}
	release, err := service.Deletion.AdmitRelease(context.Background(), machine.ReleaseRequest{
		TenantID:  "tenant",
		Key:       "release",
		SandboxID: claim.Result.SandboxID,
	})
	if !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	if _, err = service.State.Database().Exec("INSERT INTO resource_refs VALUES('vm',?,'reader','foreign-reader')", claim.Result.VMID); err != nil {
		t.Fatal(err)
	}
	service = testapp.ReopenCloneService(t, service, backend)
	step := testutil.AwaitColdWorker(t, service, release.ID)
	if step.Status == "succeeded" || backend.Count("stop") != 0 || backend.Count("remove") != 0 {
		t.Fatal("foreign reference did not fence host deletion")
	}
	var storage int64
	if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources").Scan(&storage); err != nil || storage != 64 {
		t.Fatal("foreign use released its responsibility", storage, err)
	}
}

func TestHavenStepShutdownKeepsWriterUntilWorkersExit(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend := testapp.ColdService(t, nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	backend.Before = func(name string, _ loom.Resource) error {
		if name == "prepare" {
			once.Do(func() {
				close(entered)
			})
			<-release
		}
		return nil
	}
	operation := testfixture.ColdClaim(t, service, "shutdown")
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := service.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close did not respect deadline", err)
	}
	if other, err := testapp.OpenStore(service.State.Directory(), false); !errors.Is(err, store.ErrConflict) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatal("writer was released while an effect was still running", err)
	}
	close(release)
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := service.Options
	opts.Initialize = false
	backend.Before = nil
	resumed, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = resumed.Close(context.Background())
	})
	if step := testutil.AwaitColdWorker(t, resumed, operation.ID); step.Status != "succeeded" {
		t.Fatal("shutdown invented an unexecuted terminal failure", step.State)
	}
}
