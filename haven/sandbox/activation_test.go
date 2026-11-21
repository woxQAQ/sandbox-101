package sandbox_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenResumeActivationHandoffRejectsSQL(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, rejection := range []string{
		"ABORT,'private activation handoff refusal'",
		"IGNORE",
	} {
		t.Run(rejection, func(t *testing.T) {
			service, _, _, _ := testapp.SandboxFixtureWithBackend(t, nil, testfixture.RestorableBackend)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			principal := authorization.Principal{
				TenantID: "tenant",
			}
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "claim", nil, egress.Policy{
				Mode: "none",
			})
			paused, err := service.CommandAdmission.Execute(ctx, principal, "pause", operation.Intent{
				Action:    "pause",
				SandboxID: claim.Result.SandboxID,
			})
			if err != nil {
				t.Fatal(err)
			}
			before, err := service.Catalog.Get(ctx, principal, claim.Result.SandboxID)
			if err != nil || before.State != "paused" || before.SnapshotID == "" {
				t.Fatal("fixture lacks paused snapshot", before, err)
			}
			const responsibility = `SELECT
			 (SELECT COUNT(*) FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='sleep' AND holder_id=?),
			 (SELECT COUNT(*) FROM quota_refs WHERE tenant_id='tenant' AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?)`
			checkResponsibility := func(want int) {
				t.Helper()
				var references, charges int
				if err := service.State.Database().QueryRowContext(ctx, responsibility, before.SnapshotID, before.ID, before.SnapshotID, "sleep:"+before.ID).Scan(&references, &charges); err != nil || references != want || charges != want {
					t.Fatal("sleep snapshot custody and charges diverged", references, charges, want, err)
				}
			}
			checkResponsibility(1)
			if _, err = service.State.Database().Exec("CREATE TRIGGER reject_activation_completion BEFORE UPDATE OF status ON operation_steps WHEN OLD.action='activate' AND NEW.status='succeeded' BEGIN SELECT RAISE(" + rejection + "); END"); err != nil {
				t.Fatal(err)
			}
			command := operation.Intent{
				Action:    "resume",
				SandboxID: before.ID,
			}
			failed, err := service.CommandAdmission.Execute(ctx, principal, "resume", command)
			if err == nil || failed.Status != "failed" {
				t.Fatal("activation succeeded without its final handoff", failed, err)
			}
			testutil.WaitFixtureWorker(t, service, failed.ID)
			checkResponsibility(1)
			retained, err := service.Catalog.Get(ctx, principal, before.ID)
			if err != nil || retained.State != "paused" || retained.SnapshotID != before.SnapshotID {
				t.Fatal("failed activation lost recoverable paused source", retained, err)
			}
			if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err == nil {
				t.Fatal("failed activation admitted data")
			}
			if _, err = service.State.Database().Exec("DROP TRIGGER reject_activation_completion"); err != nil {
				t.Fatal(err)
			}
			repaired, err := service.CommandAdmission.Execute(ctx, principal, "repair", command)
			if err != nil || repaired.Status != "succeeded" {
				t.Fatal("explicit resume could not reuse retained sleep source", repaired, err)
			}
			checkResponsibility(0)
			if _, err = service.Dataplane.Ready(ctx, principal, before.ID); err != nil {
				t.Fatal("successful activation did not publish data readiness", err)
			}
			for _, original := range []operation.State{
				paused,
				failed,
			} {
				unchanged, err := service.Operations.GetOperation(ctx, "tenant", original.ID)
				if err != nil || !reflect.DeepEqual(unchanged, original) {
					t.Fatal("repair rewrote an original terminal", original, unchanged, err)
				}
			}
		})
	}
}

func TestHavenSecretRestart(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, guest, manager := testapp.SandboxFixture(t)
	secretFlag := false
	const secret = "fixture-host-value-never-a-real-credential"
	env := map[string]types.EnvVar{
		"TOKEN": {
			Value: secret,
			Guest: &secretFlag,
			Inject: &egress.Injection{
				Hosts: []string{
					"api.example.com",
				},
				Header:      "Authorization",
				Placeholder: "placeholder",
			},
		},
	}
	acceptedOperation := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "secret-claim", env, egress.Policy{
		Mode: "proxy",
	})
	for _, activation := range guest.Activations {
		for _, value := range activation.Env {
			if strings.Contains(value, secret) {
				t.Fatal("secret reached guest activation")
			}
		}
	}
	rows, err := service.State.Database().Query("SELECT request FROM operations UNION ALL SELECT input FROM operation_steps UNION ALL SELECT body FROM sandbox_settings UNION ALL SELECT body FROM journal")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var body []byte
		if err = rows.Scan(&body); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), secret) {
			t.Fatal("host secret entered durable metadata")
		}
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err = service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	opts := service.Options
	opts.Initialize = false
	opts.Backend = runtime
	opts.Guest = guest
	opts.Egress = manager
	restarted, err := application.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = restarted.Close(context.Background())
	})
	if _, err = restarted.Dataplane.Ready(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID); err == nil {
		t.Fatal("restart admitted missing host credentials")
	}
	configured, err := restarted.Sandboxes.AdmitConfigure(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "resupply", operation.Intent{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env:       env,
		Policy: egress.Policy{
			Mode: "proxy",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = restarted.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, configured.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Dataplane.Ready(ctx, authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID); err != nil {
		t.Fatal(err)
	}
	changed := env["TOKEN"]
	changed.Value = "different-useless-value"
	env["TOKEN"] = changed
	if _, err = restarted.Sandboxes.AdmitConfigure(ctx, authorization.Principal{
		TenantID: "tenant",
	}, "resupply", operation.Intent{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env:       env,
		Policy: egress.Policy{
			Mode: "proxy",
		},
	}); err == nil {
		t.Fatal("credential change reused fixed idempotency identity")
	}
	if configured.Result.SandboxID != acceptedOperation.Result.SandboxID {
		t.Fatal("configuration changed sandbox identity")
	}
}
