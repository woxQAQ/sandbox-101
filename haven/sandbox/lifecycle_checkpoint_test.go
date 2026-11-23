package sandbox_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenCheckpointResponsibilityRejectsSQL(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, mode := range []string{
		"running",
		"paused",
		"delete",
	} {
		for _, rejection := range []string{
			"IGNORE",
			"ABORT",
		} {
			t.Run(mode+"/"+rejection, func(t *testing.T) {
				service, backend, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
				principal := authorization.Principal{
					TenantID: "tenant",
				}
				claim := (testfixture.SandboxClaims{
					App: service,
				}).Claim(t, "claim", nil, egress.Policy{
					Mode: "none",
				})
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				command := operation.Intent{
					Action:    "checkpoint",
					SandboxID: claim.Result.SandboxID,
				}
				var original operation.State
				if mode == "paused" {
					if _, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
						Action:    "pause",
						SandboxID: claim.Result.SandboxID,
					}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "delete" {
					var err error
					original, err = service.CommandAdmission.Execute(ctx, principal, "checkpoint", command)
					if err != nil {
						t.Fatal(err)
					}
					command = operation.Intent{
						Action:       "checkpoint_delete",
						CheckpointID: original.Result.CheckpointID,
					}
				}
				captures := backend.Count("capture")
				var storageBefore int64
				if err := service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL").Scan(&storageBefore); err != nil {
					t.Fatal(err)
				}
				transition, row := "INSERT", "NEW"
				if mode == "delete" {
					transition, row = "UPDATE OF deleted_at", "OLD"
				}
				raise := "RAISE(IGNORE)"
				if rejection == "ABORT" {
					raise = "RAISE(ABORT,'checkpoint reference rejected')"
				}
				trigger := fmt.Sprintf("CREATE TRIGGER reject_checkpoint_reference BEFORE %s ON quota_refs WHEN %s.holder_kind='active_use' AND %s.holder_id LIKE 'checkpoint-binding:%%' BEGIN SELECT %s; END", transition, row, row, raise)
				if _, err := service.State.Database().Exec(trigger); err != nil {
					t.Fatal(err)
				}
				if _, err := service.CommandAdmission.Execute(ctx, principal, "blocked", command); err == nil {
					t.Fatal("checkpoint responsibility rejection was reported as success")
				}
				var bindings, resources, costs, requests int
				if err := service.State.Database().QueryRow(`SELECT
					(SELECT COUNT(*) FROM checkpoint_bindings WHERE deleted_at IS NULL),
					(SELECT COUNT(*) FROM resource_refs WHERE deleted_at IS NULL AND holder_kind='checkpoint_binding'),
					(SELECT COUNT(*) FROM quota_refs WHERE deleted_at IS NULL AND holder_id LIKE 'checkpoint-binding:%'),
					(SELECT COUNT(*) FROM operations WHERE scope_kind='tenant' AND scope_id='tenant' AND idem_key='blocked')`).Scan(&bindings, &resources, &costs, &requests); err != nil {
					t.Fatal(err)
				}
				wantBindings, wantRequests := 0, 0
				if mode == "delete" {
					wantBindings = 1
				}
				if mode == "running" {
					wantRequests = 1
					if backend.Count("capture") != captures+1 {
						t.Fatal("running checkpoint did not retain its completed capture")
					}
				} else if backend.Count("capture") != captures {
					t.Fatal("metadata checkpoint path captured the source")
				}
				if bindings != wantBindings || resources != wantBindings || costs != wantBindings || requests != wantRequests {
					t.Fatal("partial checkpoint responsibility committed", bindings, resources, costs, requests)
				}
				var retained int64
				if err := service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL").Scan(&retained); err != nil || retained < storageBefore {
					t.Fatal("failed handoff retired storage responsibility", retained, storageBefore, err)
				}
				if mode == "delete" {
					after, err := service.OperationMonitor.GetForTenant(ctx, principal.TenantID, original.ID)
					if err != nil || !reflect.DeepEqual(after, original) {
						t.Fatal("failed deletion rewrote the original checkpoint result", err)
					}
				}
				if _, err := service.State.Database().Exec("DROP TRIGGER reject_checkpoint_reference"); err != nil {
					t.Fatal(err)
				}
				captures = backend.Count("capture")
				recovered, err := service.CommandAdmission.Execute(ctx, principal, "blocked", command)
				if err != nil || recovered.Status != "succeeded" || backend.Count("capture") != captures {
					t.Fatal("checkpoint replay failed or repeated the capture", recovered, err)
				}
				if mode == "running" {
					var after int64
					if err = service.State.Database().QueryRow("SELECT SUM(storage_mib) FROM quota_resources WHERE deleted_at IS NULL").Scan(&after); err != nil || after != retained {
						t.Fatal("replay changed retained capture cost", after, retained, err)
					}
				}
			})
		}
	}
}
