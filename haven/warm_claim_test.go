package haven

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenWarmClaimAndFallback(t *testing.T) {
	for _, fallback := range []bool{
		false,
		true,
	} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			service, runtime, guest := goldenSupplyFixture(t, 1)
			principal := Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			refill, err := service.Execute(ctx, principal, "refill", Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			runtime.mu.Lock()
			before := runtime.counts["launch"]
			runtime.mu.Unlock()
			if fallback {
				guest.before = func(name string, resource loom.Resource) error {
					if name == "activate" && resource.VMID == refill.Result.VMID {
						return fmt.Errorf("explicit rejected warm instance")
					}
					return nil
				}
			}
			claimed, err := service.Execute(ctx, principal, "claim", Command{
				Action:     "claim",
				TemplateID: "golden",
				Policy: egress.Policy{
					Mode: "none",
				},
			})
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[claimed.ID]
				service.mu.Unlock()
				t.Fatal(claimed, reason, err)
			}
			sandbox, err := service.DataReady(ctx, principal, claimed.Result.SandboxID)
			if err != nil || sandbox.State != "running" || sandbox.ID != refill.Result.SandboxID || (sandbox.VMID == refill.Result.VMID) == fallback {
				t.Fatal(sandbox, refill, err)
			}
			runtime.mu.Lock()
			after := runtime.counts["launch"]
			runtime.mu.Unlock()
			wanted := before
			if fallback {
				wanted++
			}
			if after != wanted {
				t.Fatal("unexpected launches", before, after, wanted)
			}
			var slots, storage, pool, warm, leases int
			if err = service.store.db.QueryRow("SELECT (SELECT SUM(sandbox_slots) FROM quota_resources),(SELECT SUM(storage_mib) FROM quota_resources),(SELECT COUNT(*) FROM pool_slots),(SELECT COUNT(*) FROM warm),(SELECT COUNT(*) FROM source_leases)").Scan(&slots, &storage, &pool, &warm, &leases); err != nil || slots != 1 || storage != 256 || pool != 0 || warm != 0 || leases != 0 {
				t.Fatal(slots, storage, pool, warm, leases, err)
			}
			released, err := service.Execute(ctx, principal, "release", Command{
				Action:    "release",
				SandboxID: sandbox.ID,
			})
			if err != nil {
				service.mu.Lock()
				reason := service.lastErrors[released.ID]
				service.mu.Unlock()
				t.Fatal(released, reason, err)
			}
			if err = service.store.db.QueryRow("SELECT SUM(sandbox_slots) FROM quota_resources").Scan(&slots); err != nil || slots != 0 {
				t.Fatal(slots, err)
			}
		})
	}
}
