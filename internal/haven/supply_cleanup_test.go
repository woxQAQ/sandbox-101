package haven

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestHavenSupplyCleanupConservesResponsibility(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "failed"}[failed], func(t *testing.T) {
			service, runtime, _ := goldenSupplyFixture(t, 1)
			if failed {
				runtime.mu.Lock()
				runtime.failBoot = true
				runtime.mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.Execute(ctx, Principal{TenantID: "tenant"}, "refill", Command{Action: "refill", TemplateID: "golden"})
			if !failed && err != nil {
				t.Fatal(err)
			}
			if failed && (err == nil || original.Status != "failed") {
				t.Fatal(original, err)
			}
			collector, err := service.AdmitSupplyCleanup(ctx, Principal{Root: true, TenantID: "tenant"}, "refill", SupplyCleanupRequest{OperationID: original.ID})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, collector.ID); err != nil {
				service.mu.Lock()
				reason := service.lastErrors[collector.ID]
				service.mu.Unlock()
				t.Fatal(reason, err)
			}
			if _, err = service.GetOperation(ctx, "tenant", collector.ID); err == nil {
				t.Fatal("node maintenance appeared in tenant operation scope")
			}
			var pool, warm, vms, slots, storage int
			if err = service.store.db.QueryRow("SELECT (SELECT COUNT(*) FROM pool_slots),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM vms),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&pool, &warm, &vms, &slots, &storage); err != nil || pool != 0 || warm != 0 || vms != 0 || slots != 0 || storage != 192 {
				t.Fatal(pool, warm, vms, slots, storage, err)
			}
			unchanged, err := service.GetOperation(ctx, "tenant", original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("cleanup changed original supply result", original, unchanged, err)
			}
		})
	}
}

func TestHavenSupplyCleanupFenceAndRecovery(t *testing.T) {
	for _, boundary := range []string{"supply_cleanup_admitted", "supply_cleanup_completion_before_commit"} {
		t.Run(boundary, func(t *testing.T) {
			service, _, _ := goldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.Execute(ctx, Principal{TenantID: "tenant"}, "refill", Command{Action: "refill", TemplateID: "golden"})
			if err != nil {
				t.Fatal(err)
			}
			service.opts.Fault = func(point string) error {
				if point == boundary {
					return ErrInterrupted
				}
				return nil
			}
			collector, err := service.AdmitSupplyCleanup(ctx, Principal{Root: true, TenantID: "root"}, "cleanup", SupplyCleanupRequest{OperationID: original.ID})
			if collector.ID == "" {
				t.Fatal(err)
			}
			if boundary == "supply_cleanup_admitted" {
				if !errors.Is(err, ErrInterrupted) {
					t.Fatal(err)
				}
				if _, err = service.AdmitRelease(ctx, ReleaseRequest{TenantID: "tenant", Key: "bypass", SandboxID: original.Result.SandboxID}); err == nil {
					t.Fatal("raw resource workflow passed collector fence")
				}
			} else {
				deadline := time.Now().Add(10 * time.Second)
				for {
					service.mu.Lock()
					active := service.active[collector.ID]
					last := service.lastErrors[collector.ID]
					service.mu.Unlock()
					if !active && last != nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("cleanup interruption missing")
					}
					time.Sleep(time.Millisecond)
				}
			}
			var count int
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'").Scan(&count); err != nil || count != 1 {
				t.Fatal("unproved cleanup freed pool capacity", count, err)
			}
			service = reopenCloneService(t, service, service.opts.Backend)
			if _, err = service.WaitManagement(ctx, Principal{Root: true, TenantID: "root"}, collector.ID); err != nil {
				t.Fatal(err)
			}
			if err = service.store.db.QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}
