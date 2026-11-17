package component

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

func TestHavenBackupRecoveryFence(t *testing.T) {
	parallelLifecycle(t)
	faults := &testfixture.SnapshotFaults{}
	service, _, _, _ := testapp.SandboxFixtureWithFault(t, faults.Check)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	root := authorization.Principal{
		Root:     true,
		TenantID: "root",
	}
	running := claimSandbox(t, service, "running", nil, egress.Policy{
		Mode: "none",
	})
	faults.Set(func(point string) error {
		if point == "admission_committed" {
			return errs.ErrInterrupted
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, event := range []string{
		"fixture proxy authorized",
		"fixture credential substituted",
	} {
		if err := service.Audits.Audit(ctx, map[string]string{
			"event": event,
		}); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := service.Sandboxes.AdmitClaim(ctx, principal, "pending", operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if !errors.Is(err, errs.ErrInterrupted) || pending.ID == "" {
		t.Fatal(pending, err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err = application.BackupState(ctx, service.State.Directory(), filepath.Join(t.TempDir(), "active-backup")); !errors.Is(err, store.ErrConflict) {
		t.Fatal("active writer allowed offline backup", err)
	}
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	point, err := application.BackupState(ctx, service.State.Directory(), destination)
	if err != nil {
		t.Fatal(err)
	}
	restore := filepath.Join(t.TempDir(), "restored")
	restored, err := application.RestoreBackup(ctx, restore, destination)
	if err != nil || !reflect.DeepEqual(point, restored) {
		t.Fatal(point, restored, err)
	}
	runtime := testfixture.NewColdRuntime()
	options := service.Options
	options.StateDir = restore
	options.Initialize = false
	options.Backend = runtime
	options.Fault = nil
	if accidental, err := application.Open(options); !errors.Is(err, store.ErrRecovery) {
		if accidental != nil {
			_ = accidental.Close(ctx)
		}
		t.Fatal("rollback started without review", err)
	}
	if err = application.AcknowledgeRecovery(ctx, restore, "Reviewed private fixture inventory; resource ownership history remains incomplete."); err != nil {
		t.Fatal(err)
	}
	reviewed, err := application.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reviewed.Close(context.Background())
	})
	for _, acceptedOperation := range []operation.Record{
		running,
		pending,
	} {
		copy, err := reviewed.Operations.GetOperation(ctx, "tenant", acceptedOperation.ID)
		if err != nil || !reflect.DeepEqual(copy, acceptedOperation) {
			t.Fatal("backup changed committed result or pending identity", copy, acceptedOperation, err)
		}
	}
	var count int
	if err = reviewed.State.Database().QueryRow("SELECT COUNT(*) FROM journal").Scan(&count); err != nil || count != 2 {
		t.Fatal("authenticated audit history missing from unified backup", count, err)
	}
	if err = reviewed.State.Database().QueryRow("SELECT COUNT(*) FROM sandbox_settings").Scan(&count); err != nil || count != 2 {
		t.Fatal("business settings missing from unified backup", count, err)
	}
	if err = reviewed.State.Database().QueryRow("SELECT COUNT(*) FROM reservations WHERE owner LIKE 'vm:%'").Scan(&count); err != nil || count != 2 {
		t.Fatal("resource responsibility missing from unified backup", count, err)
	}
	if _, err = reviewed.Sandboxes.AdmitClaim(ctx, principal, "new", operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Policy: egress.Policy{
			Mode: "none",
		},
	}); err == nil {
		t.Fatal("review granted new resource authority")
	}
	if _, err = reviewed.Data.DataReady(ctx, principal, running.Result.SandboxID); err == nil {
		t.Fatal("review granted data readiness")
	}
	report, err := reviewed.GarbageCollection.CollectGarbage(ctx, root, "gc", types.GCOptions{})
	if err != nil || !report.Blocked || len(report.Collected) != 0 {
		t.Fatal("review granted collection authority", report, err)
	}
	if err = reviewed.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	runtime.Mu.Lock()
	effects := len(runtime.Counts)
	runtime.Mu.Unlock()
	if effects != 0 {
		t.Fatal("rollback replayed host effects", runtime.Counts)
	}
	if err = application.AcknowledgeRecovery(ctx, restore, "A second review must not open another writer while this reviewed store is active."); err == nil {
		t.Fatal("active store accepted recovery acknowledgment")
	}
}
