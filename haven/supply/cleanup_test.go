package supply_test

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/supply"
)

func TestHavenSupplyCleanupRetainsPoolAfterChildRetirement(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private collector completion failure'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, _, _ := testapp.GoldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.CommandAdmission.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "refill", operation.Intent{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.State.Database().Exec("CREATE TRIGGER reject_collector_completion BEFORE UPDATE ON operation_steps WHEN OLD.step_key='supply_cleanup' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			principal := authorization.Principal{
				Root:     true,
				TenantID: "root",
			}
			request := supply.SupplyCleanupRequest{
				OperationID: original.ID,
			}
			accepted, err := service.SupplyCleanup.AdmitSupplyCleanup(ctx, principal, "collector", request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.Operations.WaitManagement(ctx, principal, accepted.ID); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("collector did not expose rejected completion", err)
			}
			step, err := supply.LoadSupplyCleanup(ctx, service.State.Database(), accepted.ID)
			if err != nil || step.Status != "pending" || step.Phase != "admitted" || step.Input.Pool == nil {
				t.Fatal("failed collector completion changed durable intent", step, err)
			}
			if count, err := machine.CurrentVMCount(ctx, service.State.Database(), step.Input.Source.Candidate); err != nil || count != 0 {
				t.Fatal("fixture did not cross child VM retirement", count, err)
			}
			link, status, err := machine.LoadResourceLink(ctx, service.State.Database(), accepted.ID, "resource")
			if err != nil || status != "succeeded" || link.Kind != "release" {
				t.Fatal("completed child receipt was not retained", link, status, err)
			}
			reservation, status, err := supply.LoadPoolReservation(ctx, service.State.Database(), original.ID)
			if err != nil || status != "cleanup" || !reflect.DeepEqual(reservation, *step.Input.Pool) {
				t.Fatal("failed collector completion lost pool custody", reservation, status, err)
			}
			if count, err := quota.CountResourceReference(ctx, service.State.Database(), quota.ResourceReference{
				Kind:       "image",
				ID:         step.Input.Source.Input.Resource.Config.Image,
				HolderKind: "step",
				HolderID:   accepted.ID + ":supply_cleanup",
			}); err != nil || count != 1 {
				t.Fatal("failed collector completion lost image custody", count, err)
			}
			var pendingCleanup int64
			if err := service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending'", "supply_cleanup", step.Input.Source.Candidate).Scan(&pendingCleanup); err != nil || pendingCleanup != 1 {
				t.Fatal("failed collector completion lost allocation fence", pendingCleanup, err)
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_collector_completion"); err != nil {
				t.Fatal(err)
			}
			replay, err := service.SupplyCleanup.AdmitSupplyCleanup(ctx, principal, "collector", request)
			if err != nil || replay.ID != accepted.ID {
				t.Fatal("collector replay changed original operation", replay, err)
			}
			if _, err = service.Operations.WaitManagement(ctx, principal, accepted.ID); err != nil {
				t.Fatal(err)
			}
			done, err := supply.LoadSupplyCleanup(ctx, service.State.Database(), accepted.ID)
			if err != nil || done.Status != "succeeded" || done.Phase != "complete" || done.Hash != step.Hash || !reflect.DeepEqual(done.Input, step.Input) {
				t.Fatal("collector replay changed frozen responsibility", done, err)
			}
			if _, _, err = supply.LoadPoolReservation(ctx, service.State.Database(), original.ID); !errors.Is(err, sql.ErrNoRows) {
				t.Fatal("completed collector retained pool reservation", err)
			}
			unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
			if err != nil || !reflect.DeepEqual(unchanged, original) {
				t.Fatal("collector rewrote original refill result", unchanged, err)
			}
		})
	}
}

func TestHavenSupplyCleanupConservesResponsibility(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, failed := range []bool{
		false,
		true,
	} {
		t.Run(map[bool]string{
			false: "ready",
			true:  "failed",
		}[failed], func(t *testing.T) {
			service, runtime, _ := testapp.GoldenSupplyFixture(t, 1)
			if failed {
				runtime.Mu.Lock()
				runtime.FailBoot = true
				runtime.Mu.Unlock()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.CommandAdmission.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "refill", operation.Intent{
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
	testutil.ParallelLifecycle(t)
	for _, boundary := range []string{
		"supply_cleanup_admitted",
		"supply_cleanup_completion_before_commit",
	} {
		t.Run(boundary, func(t *testing.T) {
			service, _, _ := testapp.GoldenSupplyFixtureWithFault(t, 1, func(point string) error {
				if point == boundary {
					return errs.ErrInterrupted
				}
				return nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			original, err := service.CommandAdmission.Execute(ctx, authorization.Principal{
				TenantID: "tenant",
			}, "refill", operation.Intent{
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
