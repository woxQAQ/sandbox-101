package component

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestHavenWarmClaimAndFallback(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	for _, fallback := range []bool{
		false,
		true,
	} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			service, runtime, guest := fixtures.GoldenSupplyFixture(t, 1)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			refill, err := service.SandboxRequests.Execute(ctx, principal, "refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime.Mu.Lock()
			before := runtime.Counts["launch"]
			runtime.Mu.Unlock()
			if fallback {
				guest.Before = func(name string, resource loom.Resource) error {
					if name == "activate" && resource.VMID == refill.Result.VMID {
						return fmt.Errorf("explicit rejected warm instance")
					}
					return nil
				}
			}
			claimed, err := service.SandboxRequests.Execute(ctx, principal, "claim", operation.Command{
				Action:     "claim",
				TemplateID: "golden",
				Policy: egress.Policy{
					Mode: "none",
				},
			})
			if err != nil {
				reason := service.Workers.State(claimed.ID).Err
				t.Fatal(claimed, reason, err)
			}
			sandbox, err := service.Dataplane.Ready(ctx, principal, claimed.Result.SandboxID)
			if err != nil || sandbox.State != "running" || sandbox.ID != refill.Result.SandboxID || (sandbox.VMID == refill.Result.VMID) == fallback {
				t.Fatal(sandbox, refill, err)
			}
			runtime.Mu.Lock()
			after := runtime.Counts["launch"]
			runtime.Mu.Unlock()
			wanted := before
			if fallback {
				wanted++
			}
			if after != wanted {
				t.Fatal("unexpected launches", before, after, wanted)
			}
			var slots, storage, pool, warm, leases int
			if err = service.State.Database().QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM pool_slots),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM source_leases)").Scan(&slots, &storage, &pool, &warm, &leases); err != nil || slots != 1 || storage != 256 || pool != 0 || warm != 0 || leases != 0 {
				t.Fatal(slots, storage, pool, warm, leases, err)
			}
			released, err := service.SandboxRequests.Execute(ctx, principal, "release", operation.Command{
				Action:    "release",
				SandboxID: sandbox.ID,
			})
			if err != nil {
				reason := service.Workers.State(released.ID).Err
				t.Fatal(released, reason, err)
			}
			if err = service.State.Database().QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 0 {
				t.Fatal(slots, err)
			}
		})
	}
}
