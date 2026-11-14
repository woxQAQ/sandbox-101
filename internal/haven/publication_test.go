package haven

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenPublicTerminalHistorySurvivesChangesAndReopen(t *testing.T) {
	service, backend, guest, _ := sandboxFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	operation := claimSandbox(t, service, "public-claim", nil, egress.Policy{
		Mode: "none",
	})
	original, err := service.PublicOperation(ctx, principal, operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if original.Result.Sandbox == nil || original.CreatedAt.IsZero() || original.FinishedAt.IsZero() || original.Result.Sandbox.CreatedAt.IsZero() || original.Result.Sandbox.Epoch == "" {
		t.Fatal("public completion lacks delivery metadata", original)
	}
	token := original.Result.Tokens[operation.Result.SandboxID]
	if token == "" {
		t.Fatal("successful delivery lacks sandbox capability")
	}
	frozen := encode(original)
	var releaseID string
	for _, command := range []Command{
		{
			Action:    "configure",
			SandboxID: operation.Result.SandboxID,
			Env: map[string]EnvVar{
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
			SandboxID:  operation.Result.SandboxID,
			TTLSeconds: 120,
		},
		{
			Action:    "release",
			SandboxID: operation.Result.SandboxID,
		},
	} {
		changed, changeErr := service.Execute(ctx, principal, "public-"+command.Action, command)
		if changeErr != nil {
			t.Fatal(command.Action, changeErr)
		}
		if command.Action == "release" {
			releaseID = changed.ID
		}
		current, err := service.PublicOperation(ctx, principal, operation.ID)
		if err != nil || !bytes.Equal(frozen, encode(current)) {
			t.Fatal("later workflow changed historical delivery", current, err)
		}
	}
	if _, err = service.Authenticate(ctx, token); err == nil {
		t.Fatal("terminated capability grants ordinary API access")
	}
	retired, err := service.AuthenticateOperation(ctx, token)
	if err != nil || retired.SandboxID != operation.Result.SandboxID {
		t.Fatal(retired, err)
	}
	if _, err = service.PublicOperation(ctx, retired, operation.ID); err == nil {
		t.Fatal("retired capability read a tenant-only claim")
	}
	if view, err := service.PublicOperation(ctx, retired, releaseID); err != nil || view.Result.Tokens != nil {
		t.Fatal("retired capability cannot read its release without minting new tokens", view, err)
	}
	if _, err = service.PublicOperation(ctx, Principal{
		TenantID: "other",
	}, operation.ID); err == nil {
		t.Fatal("foreign operation visible")
	}
	directory := service.store.directory
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Options{
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
	current, err := reopened.PublicOperation(ctx, principal, operation.ID)
	if err != nil || !bytes.Equal(frozen, encode(current)) {
		t.Fatal("reopen recomputed terminal result", current, err)
	}
	if _, err = reopened.store.db.Exec("UPDATE operation_publications SET body='{}' WHERE operation_id=?", operation.ID); err == nil {
		t.Fatal("historical public result is mutable")
	}
}

func TestHavenPublicPublicationFailureKeepsConfigurationFenced(t *testing.T) {
	service, _, _, proxy := sandboxFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	operation := claimSandbox(t, service, "claim-before-publication-fault", nil, egress.Policy{
		Mode: "relay",
	})
	if _, err := service.store.db.Exec(`CREATE TRIGGER reject_public_delivery BEFORE INSERT ON operation_publications BEGIN SELECT RAISE(ABORT,'publication blocked'); END`); err != nil {
		t.Fatal(err)
	}
	configure, err := service.AdmitConfigure(ctx, principal, "configure-publication-fault", Command{
		Action:    "configure",
		SandboxID: operation.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "relay",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Wait(ctx, principal, configure.ID); err == nil {
		t.Fatal("publication fault returned success")
	}
	if _, err = service.DataReady(ctx, principal, operation.Result.SandboxID); err == nil {
		t.Fatal("failed public completion reopened data")
	}
	proxy.mu.Lock()
	bound := len(proxy.binds)
	proxy.mu.Unlock()
	if bound != 0 {
		t.Fatal("new proxy survived rejected public completion")
	}
	var status string
	if err = service.store.db.QueryRow("SELECT status FROM operations WHERE id=?", configure.ID).Scan(&status); err != nil || terminal(status) {
		t.Fatal("terminal result committed without public delivery", status, err)
	}
	if _, err = service.store.db.Exec("DROP TRIGGER reject_public_delivery"); err != nil {
		t.Fatal(err)
	}
	repaired, err := service.Execute(ctx, principal, "configure-after-publication-fault", Command{
		Action:    "configure",
		SandboxID: operation.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "relay",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.PublicOperation(ctx, principal, repaired.ID)
	if err != nil || view.Status != "succeeded" {
		t.Fatal(view, err)
	}
	if err = service.Reconcile(); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Wait(ctx, principal, configure.ID); err == nil {
		t.Fatal("superseded intent returned success")
	}
	old, err := service.PublicOperation(ctx, principal, configure.ID)
	if err != nil || old.Status != "failed" || old.Error == nil {
		t.Fatal("superseded failure was not frozen", old, err)
	}
}
