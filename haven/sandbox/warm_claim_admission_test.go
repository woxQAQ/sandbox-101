package sandbox_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/supply"
)

func TestWarmClaimRechecksRequestedTemplate(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _ := testapp.GoldenSupplyFixture(t, 0)
	ctx := t.Context()
	template, err := service.TemplateCatalog.Owned(ctx, "tenant", "golden")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		tenantID   string
		templateID string
		want       error
	}{
		{"other", "golden", errs.ErrNotFound},
		{"tenant", "cold", errs.ErrConflict},
	} {
		t.Run(tc.tenantID+"/"+tc.templateID, func(t *testing.T) {
			prepared, err := service.AdmissionPolicy.PrepareCommand(operation.Intent{
				Action:     "claim",
				TemplateID: tc.templateID,
			})
			if err != nil {
				t.Fatal(err)
			}
			_, found, err := service.WarmClaims.Admit(ctx, sandbox.WarmClaimRequest{
				Principal: authorization.Principal{
					TenantID: tc.tenantID,
				},
				Key: "claim",
				Admission: sandbox.ClaimAdmission{
					PreparedCommand: *prepared,
				},
				Template: template,
			})
			if found || !errors.Is(err, tc.want) {
				t.Fatalf("admission with mismatched template returned found=%t, err=%v, want %v", found, err, tc.want)
			}
		})
	}
}

func TestConcurrentClaimsConsumeReadySlotOnce(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _ := testapp.GoldenSupplyFixture(t, 1)
	ctx := t.Context()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	refill, err := service.CommandAdmission.Execute(ctx, principal, "refill", operation.Intent{
		Action:     "refill",
		TemplateID: "golden",
	})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		claim operation.State
		err   error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, key := range []string{
		"first",
		"second",
	} {
		go func() {
			<-start
			claim, err := service.CommandAdmission.Execute(ctx, principal, key, operation.Intent{
				Action:     "claim",
				TemplateID: "golden",
			})
			results <- outcome{
				claim: claim,
				err:   err,
			}
		}()
	}
	close(start)
	ids := map[string]bool{}
	warm := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		ids[r.claim.Result.SandboxID] = true
		if r.claim.Result.VMID == refill.Result.VMID {
			warm++
		}
	}
	if len(ids) != 2 || warm != 1 || !ids[refill.Result.SandboxID] {
		t.Fatalf("claims returned sandbox IDs %v and used the ready VM %d times", ids, warm)
	}
	var claims int
	if err = service.State.Database().QueryRowContext(ctx, "SELECT COUNT(*) FROM warm_claims WHERE refill_operation_id=?", refill.ID).Scan(&claims); err != nil || claims != 1 {
		t.Fatalf("ready slot has %d claims: %v", claims, err)
	}
}

func TestHavenWarmSelectionRejectsSQL(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, scenario := range []struct {
		name      string
		rejection string
	}{
		{"ignore", "IGNORE"},
		{"abort", "ABORT,'pool-write-refused'"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			service, runtime, _ := testapp.GoldenSupplyFixture(t, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			refill, err := service.CommandAdmission.Execute(ctx, principal, "refill", operation.Intent{
				Action:     "refill",
				TemplateID: "golden",
			})
			if err != nil {
				t.Fatal(err)
			}
			original, status, err := supply.LoadPoolReservation(ctx, service.State.Database(), refill.ID)
			if err != nil || status != "ready" {
				t.Fatal(err)
			}
			const responsibility = `SELECT
			 (SELECT COUNT(*) FROM operations),
			 (SELECT COUNT(*) FROM resource_refs WHERE deleted_at IS NULL),
			 (SELECT COUNT(*) FROM quota_refs WHERE deleted_at IS NULL),
			 (SELECT SUM(sandbox_slots) FROM quota_resources WHERE deleted_at IS NULL),
			 (SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL)`
			var before, after [5]int64
			if err = service.State.Database().QueryRowContext(ctx, responsibility).Scan(&before[0], &before[1], &before[2], &before[3], &before[4]); err != nil {
				t.Fatal(err)
			}
			launches := runtime.Count("launch")
			trigger := fmt.Sprintf("CREATE TRIGGER reject_pool_selection BEFORE UPDATE OF status ON pool_slots WHEN NEW.status='selected' BEGIN SELECT RAISE(%s); END", scenario.rejection)
			if _, err = service.State.Database().ExecContext(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			command := operation.Intent{
				Action:     "claim",
				TemplateID: "golden",
				Policy: egress.Policy{
					Mode: "none",
				},
			}
			if _, err = service.CommandAdmission.Execute(ctx, principal, "claim", command); err == nil {
				t.Fatal("claim accepted without acquiring the selected pool reservation")
			}
			retained, status, err := supply.LoadPoolReservation(ctx, service.State.Database(), refill.ID)
			if err != nil || status != "ready" || !reflect.DeepEqual(retained, original) {
				t.Fatal("failed selection changed the original ready reservation", err)
			}
			if err = service.State.Database().QueryRowContext(ctx, responsibility).Scan(&after[0], &after[1], &after[2], &after[3], &after[4]); err != nil || before != after {
				t.Fatal("failed admission committed partial responsibility", before, after, err)
			}
			warm, err := (supply.ResourceUsage{}).VMUsers(ctx, service.State.Database(), refill.Result.VMID)
			if err != nil || warm != 1 {
				t.Fatal("failed selection removed warm availability", warm, err)
			}
			if runtime.Count("launch") != launches {
				t.Fatal("failed selection launched a replacement VM")
			}
			if _, err = service.State.Database().ExecContext(ctx, "DROP TRIGGER reject_pool_selection"); err != nil {
				t.Fatal(err)
			}
			claimed, err := service.CommandAdmission.Execute(ctx, principal, "claim", command)
			if err != nil || claimed.Result.VMID != refill.Result.VMID || runtime.Count("launch") != launches {
				t.Fatal("retry did not reuse the original warm resource", claimed, err)
			}
			ready, err := service.Dataplane.Ready(ctx, principal, claimed.Result.SandboxID)
			if err != nil || ready.State != "running" || ready.VMID != refill.Result.VMID {
				t.Fatal("retried claim did not complete activation", ready, err)
			}
		})
	}
}
