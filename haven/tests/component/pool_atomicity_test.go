package component

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenWarmSelectionRejectsSQL(t *testing.T) {
	parallelLifecycle(t)
	for _, scenario := range []struct {
		name      string
		rejection string
	}{
		{"ignore", "IGNORE"},
		{"abort", "ABORT,'pool-write-refused'"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			service, runtime, _ := goldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			refill, err := service.SandboxRequests.Execute(ctx, principal, "refill", operation.Command{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			var original []byte
			if err = service.State.Database().QueryRowContext(ctx, "SELECT body FROM pool_slots WHERE operation_id=? AND status='ready'", refill.ID).Scan(&original); err != nil {
				t.Fatal(err)
			}
			const responsibility = `SELECT
			 (SELECT COUNT(*) FROM operations),
			 (SELECT COUNT(*) FROM resource_refs),
			 (SELECT COUNT(*) FROM quota_refs),
			 (SELECT SUM(sandbox_slots) FROM quota_resources),
			 (SELECT SUM(storage_mib) FROM quota_resources)`
			var before, after [5]int64
			if err = service.State.Database().QueryRowContext(ctx, responsibility).Scan(&before[0], &before[1], &before[2], &before[3], &before[4]); err != nil {
				t.Fatal(err)
			}
			launches := runtime.Count("launch")
			trigger := fmt.Sprintf("CREATE TRIGGER reject_pool_selection BEFORE UPDATE OF status ON pool_slots WHEN NEW.status='selected' BEGIN SELECT RAISE(%s); END", scenario.rejection)
			if _, err = service.State.Database().ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			command := operation.Command{
				Action:     "claim",
				TemplateID: "golden",
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			if _, err = service.SandboxRequests.Execute(ctx, principal, "claim", command); err == nil {
				t.Fatal("claim accepted without acquiring the selected pool reservation")
			}
			var retained []byte
			if err = service.State.Database().QueryRowContext(ctx, "SELECT body FROM pool_slots WHERE operation_id=? AND status='ready'", refill.ID).Scan(&retained); err != nil || !bytes.Equal(retained, original) {
				t.Fatal("failed selection changed the original ready reservation", err)
			}
			if err = service.State.Database().QueryRowContext(ctx, responsibility).Scan(&after[0], &after[1], &after[2], &after[3], &after[4]); err != nil || before != after {
				t.Fatal("failed admission committed partial responsibility", before, after, err)
			}
			var warm int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM warm WHERE vm_id=?", refill.Result.VMID).Scan(&warm); err != nil || warm != 1 {
				t.Fatal("failed selection removed warm availability", warm, err)
			}
			if runtime.Count("launch") != launches {
				t.Fatal("failed selection launched a replacement VM")
			}
			if _, err = service.State.Database().ExecContext(ctx, "DROP TRIGGER reject_pool_selection"); err != nil {
				t.Fatal(err)
			}
			claimed, err := service.SandboxRequests.Execute(ctx, principal, "claim", command)
			if err != nil || claimed.Result.VMID != refill.Result.VMID || runtime.Count("launch") != launches {
				t.Fatal("retry did not reuse the original warm resource", claimed, err)
			}
			ready, err := service.Data.DataReady(ctx, principal, claimed.Result.SandboxID)
			if err != nil || ready.State != "running" || ready.VMID != refill.Result.VMID {
				t.Fatal("retried claim did not complete activation", ready, err)
			}
		})
	}
}
