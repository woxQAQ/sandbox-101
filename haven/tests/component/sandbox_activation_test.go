package component

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
)

// sandboxClaims borrows the App used by both claim admission and its operation
// wait, keeping the test's execution target fixed across those steps.
type sandboxClaims struct {
	App *application.App
}

func (fixture sandboxClaims) claim(t *testing.T, key string, env map[string]operation.EnvVar, policy egress.Policy) operation.Record {
	service := fixture.App
	t.Helper()
	acceptedOperation, err := service.Sandboxes.AdmitClaim(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, key, operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Env:        env,
		Policy:     policy,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acceptedOperation, err = service.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.ID)
	if err != nil {
		reason := service.Workers.Snapshot(acceptedOperation.ID).Err
		t.Fatal("sandbox claim failed", acceptedOperation, err, reason)
	}
	return acceptedOperation
}

func TestHavenClaimDelivery(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, guest, _ := testapp.SandboxFixture(t)
	acceptedOperation := (sandboxClaims{
		App: service,
	}).claim(t, "claim", map[string]operation.EnvVar{
		"VISIBLE": {
			Value: "guest-value",
		},
	}, egress.Policy{
		Mode: "none",
	})
	sandboxView, err := service.Catalog.Get(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID)
	if err != nil || sandboxView.State != "running" || sandboxView.Instance == "" || !time.Now().Before(sandboxView.Deadline) {
		t.Fatal(sandboxView, err)
	}
	if _, err = service.Dataplane.Ready(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, sandboxView.ID); err != nil {
		t.Fatal(err)
	}
	if runtime.Count("launch") != 1 || len(guest.Activations) != 1 || guest.Activations[0].Env["VISIBLE"] != "guest-value" {
		t.Fatal("claim bypassed or repeated guest activation")
	}
	conn, err := service.Dataplane.Connect(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, sandboxView.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if _, err = service.Dataplane.Connect(context.Background(), authorization.Principal{
		TenantID: "other",
	}, sandboxView.ID); err == nil {
		t.Fatal("foreign workload admission")
	}
}

func TestHavenConfigureAdmission(t *testing.T) {
	parallelLifecycle(t)
	service, _, guest, _ := testapp.SandboxFixture(t)
	acceptedOperation := (sandboxClaims{
		App: service,
	}).claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	entered, finish := make(chan struct{}), make(chan struct{})
	guest.Before = func(name string, _ loom.Resource) error {
		if name == "activate" {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-finish
		}
		return nil
	}
	first, err := service.Sandboxes.AdmitConfigure(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "blocked-config", operation.Command{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env: map[string]operation.EnvVar{
			"VALUE": {
				Value: "old",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	if _, err = service.Dataplane.Ready(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID); err == nil {
		t.Fatal("configure fence allowed data")
	}
	newer, err := service.Sandboxes.AdmitConfigure(context.Background(), authorization.Principal{
		TenantID: "tenant",
	}, "new-config", operation.Command{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env: map[string]operation.EnvVar{
			"VALUE": {
				Value: "new",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(finish)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = service.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, first.ID); err == nil {
		t.Fatal("superseded configuration succeeded")
	}
	if _, err = service.Operations.Wait(ctx, authorization.Principal{
		TenantID: "tenant",
	}, newer.ID); err != nil {
		t.Fatal(err)
	}
	sandboxView, err := service.Dataplane.Ready(ctx, authorization.Principal{
		TenantID: "tenant",
	}, acceptedOperation.Result.SandboxID)
	if err != nil || sandboxView.Env["VALUE"] != "new" {
		t.Fatal(sandboxView, err)
	}
}

func TestHavenSecretRestart(t *testing.T) {
	parallelLifecycle(t)
	service, runtime, guest, manager := testapp.SandboxFixture(t)
	secretFlag := false
	const secret = "fixture-host-value-never-a-real-credential"
	env := map[string]operation.EnvVar{
		"TOKEN": {
			Value: secret,
			Guest: &secretFlag,
			Inject: &authorization.Injection{
				Hosts: []string{
					"api.example.com",
				},
				Header:      "Authorization",
				Placeholder: "placeholder",
			},
		},
	}
	acceptedOperation := (sandboxClaims{
		App: service,
	}).claim(t, "secret-claim", env, egress.Policy{
		Mode: "relay",
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
	}, "resupply", operation.Command{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env:       env,
		Policy: egress.Policy{
			Mode: "relay",
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
	}, "resupply", operation.Command{
		Action:    "configure",
		SandboxID: acceptedOperation.Result.SandboxID,
		Env:       env,
		Policy: egress.Policy{
			Mode: "relay",
		},
	}); err == nil {
		t.Fatal("credential change reused fixed idempotency identity")
	}
	if configured.Result.SandboxID != acceptedOperation.Result.SandboxID {
		t.Fatal("configuration changed sandbox identity")
	}
}
