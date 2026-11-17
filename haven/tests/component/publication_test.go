package component

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenPublicTerminalHistorySurvivesChangesAndReopen(t *testing.T) {
	parallelLifecycle(t)
	service, backend, guest, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	acceptedRecord := (sandboxClaims{
		App: service,
	}).claim(t, "public-claim", nil, egress.Policy{
		Mode: "none",
	})
	original, err := service.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if original.Result.Sandbox == nil || original.CreatedAt.IsZero() || original.FinishedAt.IsZero() || original.Result.Sandbox.CreatedAt.IsZero() || original.Result.Sandbox.Epoch == "" {
		t.Fatal("public completion lacks delivery metadata", original)
	}
	token := original.Result.Tokens[acceptedRecord.Result.SandboxID]
	if token == "" {
		t.Fatal("successful delivery lacks sandbox capability")
	}
	frozen := testfixture.Encode(original)
	var releaseID string
	for _, command := range []operation.Command{
		{
			Action:    "configure",
			SandboxID: acceptedRecord.Result.SandboxID,
			Env: map[string]operation.EnvVar{
				"VISIBLE": {
					Value: "new-policy",
				},
			},
			Policy: egress.Policy{
				Mode: "none",
			},
		},
		{
			Action:     "renew",
			SandboxID:  acceptedRecord.Result.SandboxID,
			TTLSeconds: 120,
		},
		{
			Action:    "release",
			SandboxID: acceptedRecord.Result.SandboxID,
		},
	} {
		changed, changeErr := service.SandboxRequests.Execute(ctx, principal, "public-"+command.Action, command)
		if changeErr != nil {
			t.Fatal(command.Action, changeErr)
		}
		if command.Action == "release" {
			releaseID = changed.ID
		}
		current, err := service.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
		if err != nil || !bytes.Equal(frozen, testfixture.Encode(current)) {
			t.Fatal("later workflow changed historical delivery", current, err)
		}
	}
	if _, err = service.Authentication.Authenticate(ctx, token); err == nil {
		t.Fatal("terminated capability grants ordinary API access")
	}
	retired, err := service.Authentication.AuthenticateOperation(ctx, token)
	if err != nil || retired.SandboxID != acceptedRecord.Result.SandboxID {
		t.Fatal(retired, err)
	}
	if _, err = service.Delivery.PublicOperation(ctx, retired, acceptedRecord.ID); err == nil {
		t.Fatal("retired capability read a tenant-only claim")
	}
	if view, err := service.Delivery.PublicOperation(ctx, retired, releaseID); err != nil || view.Result.Tokens != nil {
		t.Fatal("retired capability cannot read its release without minting new tokens", view, err)
	}
	if _, err = service.Delivery.PublicOperation(ctx, authorization.Principal{
		TenantID: "other",
	}, acceptedRecord.ID); err == nil {
		t.Fatal("foreign operation visible")
	}
	directory := service.State.Directory()
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := application.Open(application.Options{
		StateDir:     directory,
		Backend:      backend,
		Guest:        guest,
		MaxMemoryMiB: 8192,
		MaxVCPUs:     64,
		SafetyBytes:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = reopened.Close(context.Background())
	})
	current, err := reopened.Delivery.PublicOperation(ctx, principal, acceptedRecord.ID)
	if err != nil || !bytes.Equal(frozen, testfixture.Encode(current)) {
		t.Fatal("reopen recomputed terminal result", current, err)
	}
	if _, err = reopened.State.Database().Exec("UPDATE operation_publications SET body='{}' WHERE operation_id=?", acceptedRecord.ID); err == nil {
		t.Fatal("historical public result is mutable")
	}
}

func TestHavenPublicPublicationFailureKeepsConfigurationFenced(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, proxy := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acceptedRecord := (sandboxClaims{
		App: service,
	}).claim(t, "claim-before-publication-fault", nil, egress.Policy{
		Mode: "relay",
	})
	if _, err := service.State.Database().Exec(`CREATE TRIGGER reject_public_delivery BEFORE INSERT ON operation_publications BEGIN SELECT RAISE(ABORT,'publication blocked'); END`); err != nil {
		t.Fatal(err)
	}
	configure, err := service.Sandboxes.AdmitConfigure(ctx, principal, "configure-publication-fault", operation.Command{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "relay",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.Wait(ctx, principal, configure.ID); err == nil {
		t.Fatal("publication fault returned success")
	}
	if _, err = service.Dataplane.Ready(ctx, principal, acceptedRecord.Result.SandboxID); err == nil {
		t.Fatal("failed public completion reopened data")
	}
	proxy.Mu.Lock()
	bound := len(proxy.Binds)
	proxy.Mu.Unlock()
	if bound != 0 {
		t.Fatal("new proxy survived rejected public completion")
	}
	var status string
	if err = service.State.Database().QueryRow("SELECT status FROM operations WHERE id=?", configure.ID).Scan(&status); err != nil || terminal(status) {
		t.Fatal("terminal result committed without public delivery", status, err)
	}
	if _, err = service.State.Database().Exec("DROP TRIGGER reject_public_delivery"); err != nil {
		t.Fatal(err)
	}
	repaired, err := service.SandboxRequests.Execute(ctx, principal, "configure-after-publication-fault", operation.Command{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "relay",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.Delivery.PublicOperation(ctx, principal, repaired.ID)
	if err != nil || view.Status != "succeeded" {
		t.Fatal(view, err)
	}
	if err = service.Maintenance.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.Wait(ctx, principal, configure.ID); err == nil {
		t.Fatal("superseded intent returned success")
	}
	old, err := service.Delivery.PublicOperation(ctx, principal, configure.ID)
	if err != nil || old.Status != "failed" || old.Step != "failed" || old.Error != nil {
		t.Fatal("superseded failure was not frozen", old, err)
	}
}
