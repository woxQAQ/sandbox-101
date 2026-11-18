package component

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenSupplyCleanupConservesResponsibility(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, failed := range []bool{
		false,
		true,
	} {
		t.Run(map[bool]string{
			false: "ready",
			true:  "failed",
		}[failed], func(t *testing.T) {
			service, runtime, _ := fixtures.GoldenSupplyFixture(t, 1)
			if failed {
				runtime.Mu.Lock()
				runtime.FailBoot = true
				runtime.Mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if !failed && err != nil {
				t.Fatal(err)
			}
			if failed && (err == nil || original.Status != "failed") {
				t.Fatal(original, err)
			}
			collector, err := service.SupplyCleanup.AdmitSupplyCleanup(ctx, authorization.Principal{
				Root:     true,
				TenantID: "tenant",
			}, "refill", supply.SupplyCleanupRequest{
				OperationID: original.ID,
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, collector.ID); err != nil {
				reason := service.Workers.State(collector.ID).Err
				t.Fatal(reason, err)
			}
			if _, err = service.Operations.GetOperation(ctx, "tenant", collector.ID); err == nil {
				t.Fatal("node maintenance appeared in tenant operation scope")
			}
			var pool, warm, vms, slots, storage int
			if err = service.State.Database().QueryRow("SELECT (SELECT COUNT(*) FROM pool_slots),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM vms),(SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources)").Scan(&pool, &warm, &vms, &slots, &storage); err != nil || pool != 0 || warm != 0 || vms != 0 || slots != 0 || storage != 192 {
				t.Fatal(pool, warm, vms, slots, storage, err)
			}
			unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("cleanup changed original supply result", original, unchanged, err)
			}
		})
	}
}

func TestHavenSupplyCleanupFenceAndRecovery(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, boundary := range []string{
		"supply_cleanup_admitted",
		"supply_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, _, _ := fixtures.GoldenSupplyFixtureWithFault(t, 1, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.SandboxRequests.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			collector, err := service.SupplyCleanup.AdmitSupplyCleanup(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, "cleanup", supply.SupplyCleanupRequest{
				OperationID: original.ID,
			})
			if collector.ID == "" {
				t.Fatal(err)
			}
			if boundary == "supply_cleanup_admitted" {
				if !errors.Is(err, errs.ErrInterrupted) {
					t.Fatal(err)
				}
				if _, err = service.Deletion.AdmitRelease(ctx, machine.ReleaseRequest{
					TenantID:  "tenant",
					Key:       "bypass",
					SandboxID: original.Result.SandboxID,
				}); err == nil {
					t.Fatal("raw resource workflow passed collector fence")
				}
			} else {
				deadline := time.Now().Add(10 * time.Second)
				for {
					worker := service.Workers.State(collector.ID)
					active, last := worker.Active, worker.Err
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
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots WHERE status='cleanup'").Scan(&count); err != nil || count != 1 {
				t.Fatal("unproved cleanup freed pool capacity", count, err)
			}
			service = testapp.ReopenCloneService(t, service, service.Options.Backend)
			if _, err = service.Operations.WaitManagement(ctx, authorization.Principal{
				Root:     true,
				TenantID: "root",
			}, collector.ID); err != nil {
				t.Fatal(err)
			}
			if err = service.State.Database().QueryRow("SELECT COUNT(*) FROM pool_slots").Scan(&count); err != nil || count != 0 {
				t.Fatal(count, err)
			}
		})
	}
}
