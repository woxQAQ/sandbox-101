package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/internal/testutil"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func TestHavenStartupRejectsChangedOperationHistoryBeforeEffects(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, corruption := range []string{
		"identity",
		"sandbox_epoch",
		"missing_sandbox",
		"accepted_command",
		"missing_private_request",
	} {
		t.Run(corruption, func(t *testing.T) {
			service, runtime, _, _ := testapp.SandboxFixture(t)
			claim := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "public-corruption", nil, egress.Policy{
				Mode: "none",
			})
			id := claim.ID
			var statement string
			switch corruption {
			case "missing_private_request":
				id, _ = testfixture.SeedPendingImageImport(t, service.State)
				statement = "DELETE FROM operation_import_requests WHERE operation_id=?"
			case "accepted_command":
				statement = "UPDATE operation_commands SET ttl_seconds=ttl_seconds+1 WHERE operation_id=?"
			case "identity":
				statement = "UPDATE operations SET idem_key='changed-request-identity' WHERE id=?"
			case "missing_sandbox":
				statement = "DELETE FROM published_sandboxes WHERE operation_id=?"
			case "sandbox_epoch":
				statement = "UPDATE published_sandboxes SET epoch='changed' WHERE operation_id=?"
			}
			_, err := service.State.Database().Exec(statement, id)
			if err != nil {
				t.Fatal(err)
			}
			options := service.Options
			options.Initialize = false
			if err = service.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			before := testutil.StoreSnapshot(t, options.StateDir)
			launches := runtime.Count("launch")
			opened, err := application.Open(options)
			if opened != nil {
				_ = opened.Close(context.Background())
			}
			if !errors.Is(err, store.ErrRecovery) {
				t.Fatal("corrupt delivery passed startup", err)
			}
			if after := testutil.StoreSnapshot(t, options.StateDir); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected delivery changed private state")
			}
			if runtime.Count("launch") != launches {
				t.Fatal("rejected delivery launched effects")
			}
		})
	}
}

func TestHavenStartupRejectsMalformedWarmStepBeforeEffects(t *testing.T) {
	testutil.ParallelLifecycle(t)
	service, runtime, _ := testapp.GoldenSupplyFixtureWithFault(t, 1, func(point string) error {
		if point == "activation_off_intent" {
			return errs.ErrInterrupted
		}
		return nil
	})
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := service.CommandAdmission.Execute(ctx, principal, "refill", operation.Intent{
		Action:     "refill",
		TemplateID: "golden",
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := service.Sandboxes.AdmitClaim(ctx, principal, "warm", operation.Intent{
		Action:     "claim",
		TemplateID: "golden",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.OperationMonitor.Wait(ctx, principal, claim.ID); !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	testutil.WaitFixtureWorker(t, service, claim.ID)
	if _, err = service.State.Database().Exec("DELETE FROM warm_claims WHERE operation_id=?", claim.ID); err != nil {
		t.Fatal(err)
	}
	options := service.Options
	options.Initialize = false
	options.Fault = nil
	if err = service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	before := testutil.StoreSnapshot(t, options.StateDir)
	launches := runtime.Count("launch")
	opened, err := application.Open(options)
	if opened != nil {
		_ = opened.Close(ctx)
	}
	if !errors.Is(err, store.ErrRecovery) {
		t.Fatal("malformed durable step passed startup", err)
	}
	if after := testutil.StoreSnapshot(t, options.StateDir); !reflect.DeepEqual(before, after) {
		for path, value := range before {
			if !reflect.DeepEqual(after[path], value) {
				t.Log("changed", path, value, after[path])
			}
		}
		for path := range after {
			if _, ok := before[path]; !ok {
				t.Log("created", path)
			}
		}
		t.Fatal("rejected startup changed private durable state")
	}
	if runtime.Count("launch") != launches {
		t.Fatal("rejected step started runtime effects")
	}
}
