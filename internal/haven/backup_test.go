package haven

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenBackupRecoveryFence(t *testing.T) {
	service, _, _, _ := sandboxFixture(t)
	principal := Principal{TenantID: "tenant"}
	root := Principal{Root: true, TenantID: "root"}
	running := claimSandbox(t, service, "running", nil, egress.Policy{Mode: "none"})
	service.opts.Fault = func(point string) error {
		if point == "admission_committed" {
			return ErrInterrupted
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pending, err := service.AdmitClaim(ctx, principal, "pending", Command{Action: "claim", TemplateID: "cold", TTLSeconds: 60, Policy: egress.Policy{Mode: "none"}})
	if !errors.Is(err, ErrInterrupted) || pending.ID == "" {
		t.Fatal(pending, err)
	}
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err = BackupState(ctx, service.store.directory, filepath.Join(t.TempDir(), "active-backup")); !errors.Is(err, errStoreConflict) {
		t.Fatal("active writer allowed offline backup", err)
	}
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	point, err := BackupState(ctx, service.store.directory, destination)
	if err != nil {
		t.Fatal(err)
	}
	restore := filepath.Join(t.TempDir(), "restored")
	restored, err := RestoreBackup(ctx, restore, destination)
	if err != nil || !reflect.DeepEqual(point, restored) {
		t.Fatal(point, restored, err)
	}
	runtime := newColdRuntime()
	options := service.opts
	options.StateDir = restore
	options.Initialize = false
	options.Backend = runtime
	options.Fault = nil
	if accidental, err := Open(options); !errors.Is(err, errStoreRecovery) {
		if accidental != nil {
			_ = accidental.Close(ctx)
		}
		t.Fatal("rollback started without review", err)
	}
	if err = AcknowledgeRecovery(ctx, restore, "Reviewed private fixture inventory; resource ownership history remains incomplete."); err != nil {
		t.Fatal(err)
	}
	reviewed, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reviewed.Close(context.Background()) })
	for _, operation := range []Operation{running, pending} {
		copy, err := reviewed.GetOperation(ctx, "tenant", operation.ID)
		if err != nil || !reflect.DeepEqual(copy, operation) {
			t.Fatal("backup changed committed result or pending identity", copy, operation, err)
		}
	}
	var count int
	if err = reviewed.store.db.QueryRow("SELECT COUNT(*) FROM sandbox_settings").Scan(&count); err != nil || count != 2 {
		t.Fatal("business settings missing from unified backup", count, err)
	}
	if err = reviewed.store.db.QueryRow("SELECT COUNT(*) FROM reservations WHERE owner LIKE 'vm:%'").Scan(&count); err != nil || count != 2 {
		t.Fatal("resource responsibility missing from unified backup", count, err)
	}
	if _, err = reviewed.AdmitClaim(ctx, principal, "new", Command{Action: "claim", TemplateID: "cold", TTLSeconds: 60, Policy: egress.Policy{Mode: "none"}}); err == nil {
		t.Fatal("review granted new resource authority")
	}
	if _, err = reviewed.DataReady(ctx, principal, running.Result.SandboxID); err == nil {
		t.Fatal("review granted data readiness")
	}
	report, err := reviewed.CollectGarbage(ctx, root, "gc", GCOptions{})
	if err != nil || !report.Blocked || len(report.Collected) != 0 {
		t.Fatal("review granted collection authority", report, err)
	}
	if err = reviewed.Reconcile(); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	effects := len(runtime.counts)
	runtime.mu.Unlock()
	if effects != 0 {
		t.Fatal("rollback replayed host effects", runtime.counts)
	}
	if err = AcknowledgeRecovery(ctx, restore, "A second review must not open another writer while this reviewed store is active."); err == nil {
		t.Fatal("active store accepted recovery acknowledgment")
	}
}
