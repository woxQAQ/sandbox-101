package workflows_test

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
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestHavenPublicTerminalHistorySurvivesChangesAndReopen(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, backend, guest, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	acceptedRecord := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "public-claim", nil, egress.Policy{
		Mode: "none",
	})
	original, err := service.OperationViews.Get(ctx, principal, acceptedRecord.ID)
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
	frozen := operation.Encode(original)
	var releaseID string
	for _, command := range []operation.Intent{
		{
			Action:    "configure",
			SandboxID: acceptedRecord.Result.SandboxID,
			Env: map[string]types.EnvVar{
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
		changed, changeErr := service.CommandAdmission.Execute(ctx, principal, "public-"+command.Action, command)
		if changeErr != nil {
			t.Fatal(command.Action, changeErr)
		}
		if command.Action == "release" {
			releaseID = changed.ID
		}
		current, err := service.OperationViews.Get(ctx, principal, acceptedRecord.ID)
		if err != nil || !bytes.Equal(frozen, operation.Encode(current)) {
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
	if _, err = service.OperationViews.Get(ctx, retired, acceptedRecord.ID); err == nil {
		t.Fatal("retired capability read a tenant-only claim")
	}
	if view, err := service.OperationViews.Get(ctx, retired, releaseID); err != nil || view.Result.Tokens != nil {
		t.Fatal("retired capability cannot read its release without minting new tokens", view, err)
	}
	if _, err = service.OperationViews.Get(ctx, authorization.Principal{
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
	current, err := reopened.OperationViews.Get(ctx, principal, acceptedRecord.ID)
	if err != nil || !bytes.Equal(frozen, operation.Encode(current)) {
		t.Fatal("reopen recomputed terminal result", current, err)
	}
}

func TestHavenRenewRollsBackWithoutSandboxDelivery(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	ctx := t.Context()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	claim := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim", nil, egress.Policy{
		Mode: "none",
	})
	id := claim.Result.SandboxID
	before, err := service.Catalog.Get(ctx, principal, id)
	if err != nil {
		t.Fatal(err)
	}
	db := service.State.Database()
	if _, err = db.Exec(`CREATE TRIGGER lose_settings AFTER UPDATE ON sandbox_settings BEGIN DELETE FROM sandbox_settings WHERE sandbox_id=NEW.sandbox_id; END`); err != nil {
		t.Fatal(err)
	}
	command := operation.Intent{
		Action:     "renew",
		SandboxID:  id,
		TTLSeconds: 120,
	}
	if _, err = service.CommandAdmission.Execute(ctx, principal, "renew", command); err == nil {
		t.Fatal("renew published success with no settings")
	}
	after, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("failed projection did not roll back the lease", after, err)
	}
	if _, err = db.Exec(`DROP TRIGGER lose_settings`); err != nil {
		t.Fatal(err)
	}
	retried, err := service.CommandAdmission.Execute(ctx, principal, "renew", command)
	if err != nil {
		t.Fatal(err)
	}
	published, err := service.OperationViews.Get(ctx, principal, retried.ID)
	if err != nil || published.Result.Sandbox == nil || published.Result.Sandbox.TTLSeconds != 120 {
		t.Fatal("retry did not publish the renewed lease", published, err)
	}
}

func TestHavenPublicPublicationFailureKeepsConfigurationFenced(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, _, _, proxy := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	acceptedRecord := (testfixture.SandboxClaims{
		App: service,
	}).Claim(t, "claim-before-publication-fault", nil, egress.Policy{
		Mode: "proxy",
	})
	if _, err := service.State.Database().Exec(`CREATE TRIGGER reject_public_delivery BEFORE INSERT ON operation_publications BEGIN SELECT RAISE(ABORT,'publication blocked'); END`); err != nil {
		t.Fatal(err)
	}
	configure, err := service.Sandboxes.AdmitConfigure(ctx, principal, "configure-publication-fault", operation.Intent{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "proxy",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.OperationMonitor.Wait(ctx, principal, configure.ID); err == nil {
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
	repository := operation.New(service.State.Database())
	status, _, err := repository.ResultState(ctx, configure.ID)
	if err != nil || testutil.Terminal(status) {
		t.Fatal("terminal result committed without public delivery", status, err)
	}
	createdAt, finishedAt, err := repository.Times(ctx, configure.ID)
	if err != nil || createdAt.IsZero() || !finishedAt.IsZero() {
		t.Fatal("rejected publication changed the operation clock", createdAt, finishedAt, err)
	}
	if _, err = service.State.Database().Exec("DROP TRIGGER reject_public_delivery"); err != nil {
		t.Fatal(err)
	}
	repaired, err := service.CommandAdmission.Execute(ctx, principal, "configure-after-publication-fault", operation.Intent{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "proxy",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := service.OperationViews.Get(ctx, principal, repaired.ID)
	if err != nil || view.Status != "succeeded" {
		t.Fatal(view, err)
	}
	if err = service.Reconcile.ResumePending(); err != nil {
		t.Fatal(err)
	}
	if _, err = service.OperationMonitor.Wait(ctx, principal, configure.ID); err == nil {
		t.Fatal("superseded intent returned success")
	}
	old, err := service.OperationViews.Get(ctx, principal, configure.ID)
	if err != nil || old.Status != "failed" || old.Step != "failed" || old.Error != nil || !old.CreatedAt.Equal(createdAt) || old.FinishedAt.IsZero() {
		t.Fatal("superseded failure was not frozen", old, err)
	}
}
