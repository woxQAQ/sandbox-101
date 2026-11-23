package sandbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

func TestHavenWarmClaimAndFallback(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, fallback := range []bool{
		false,
		true,
	} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			faults := &testfixture.SnapshotFaults{}
			service, runtime, guest := testapp.GoldenSupplyFixtureWithFault(t, 1, faults.Check)
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			refill, err := service.CommandAdmission.Execute(ctx, principal, "refill", operation.Intent{
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
				faults.Set(func(point string) error {
					if point == "admission_before_commit" {
						return errs.ErrInterrupted
					}
					return nil
				})
				guest.Before = func(name string, resource loom.Resource) error {
					if name == "activate" && resource.VMID == refill.Result.VMID {
						return fmt.Errorf("explicit rejected warm instance")
					}
					return nil
				}
			}
			command := operation.Intent{
				Action:     "claim",
				TemplateID: "golden",
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			admitted, err := service.CommandAdmission.Execute(ctx, principal, "claim", command)
			if fallback {
				if !errors.Is(err, errs.ErrInterrupted) || operation.Terminal(admitted.Status) {
					t.Fatalf("expected interruption after warm release, got %+v, %v", admitted, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			var slots int
			if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM pool_slots WHERE operation_id=?", refill.ID).Scan(&slots); err != nil || slots != 0 {
				t.Fatalf("expected retired refill slot, got %d, %v", slots, err)
			}
			published, err := service.TemplateCatalog.GetTemplate(ctx, principal, "golden")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.CommandAdmission.Execute(ctx, principal, "replace-golden", operation.Intent{
				Action: "template_put",
				Template: &types.Template{
					ID:    published.ID,
					Image: published.Image,
					Shape: published.Shape,
				},
			}); err != nil {
				t.Fatal(err)
			}
			service = testapp.ReopenCloneService(t, service, service.Options.Backend)
			claimed, err := service.CommandAdmission.Execute(ctx, principal, "claim", command)
			if err != nil || claimed.ID != admitted.ID {
				t.Fatal("claim replay after template replacement", claimed, err)
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
			db := service.State.Database()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{
				ReadOnly: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			usage, err := quota.TenantUsage(ctx, tx, principal.TenantID)
			if err != nil || usage.Slots != 1 || usage.StorageMiB != 256 {
				t.Fatal("claimed sandbox has unexpected quota usage", usage, err)
			}
			if err := supply.RequireRetiredWarm(ctx, tx, refill.ID); err != nil {
				t.Fatal(err)
			}
			warm, err := (supply.ResourceUsage{}).VMUsers(ctx, tx, refill.Result.VMID)
			if err != nil || warm != 0 {
				t.Fatal("refilled VM still available after claim", warm, err)
			}
			leases, err := machine.SourceLeases(ctx, tx)
			if err != nil || len(leases) != 0 {
				t.Fatal("claim retained snapshot source leases", leases, err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			released, err := service.CommandAdmission.Execute(ctx, principal, "release", operation.Intent{
				Action:    "release",
				SandboxID: sandbox.ID,
			})
			if err != nil {
				reason := service.Workers.State(released.ID).Err
				t.Fatal(released, reason, err)
			}
			if usage, err = quota.TenantUsage(ctx, db, principal.TenantID); err != nil || usage.Slots != 0 {
				t.Fatal("released sandbox retained a slot", usage, err)
			}
		})
	}
}
