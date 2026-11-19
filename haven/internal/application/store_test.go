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
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

func TestHavenStartupRejectsChangedDeliveryBeforeEffects(t *testing.T) {
	testutil.ParallelLifecycle(t)
	for _, corruption := range []string{
		"identity",
		"http_error",
	} {
		t.Run(corruption, func(t *testing.T) {
			service, runtime, _, _ := testapp.SandboxFixture(t)
			operation := (testfixture.SandboxClaims{
				App: service,
			}).Claim(t, "public-corruption", nil, egress.Policy{
				Mode: "none",
			})
			view, err := service.Delivery.PublicOperation(context.Background(), authorization.Principal{
				TenantID: "tenant",
			}, operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			// Model corrupt persisted bytes in an owned fixture. Startup must
			// reject changed delivery facts before opening a writable DB.
			view.Result.Tokens = nil
			switch corruption {
			case "identity":
				view.Key = "changed-request-identity"
			case "http_error":
				view.Error = &types.ApiError{
					Code:    types.CodeInternal,
					Message: "transport data must not be stored",
				}
			}
			if _, err = service.State.Database().Exec("UPDATE operation_publications SET body=? WHERE operation_id=?", testfixture.Encode(view), operation.ID); err != nil {
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
	if _, err := service.SandboxRequests.Execute(ctx, principal, "refill", operation.Command{
		Action:     "refill",
		TemplateID: "golden",
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := service.Sandboxes.AdmitClaim(ctx, principal, "warm", operation.Command{
		Action:     "claim",
		TemplateID: "golden",
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Operations.Wait(ctx, principal, claim.ID); !errors.Is(err, errs.ErrInterrupted) {
		t.Fatal(err)
	}
	testutil.WaitFixtureWorker(t, service, claim.ID)
	if _, err = service.State.Database().Exec("UPDATE operation_steps SET body=? WHERE operation_id=? AND step_key='warm_claim'", testfixture.Encode(sandbox.WarmClaimState{
		Phase: "unsupported-stage",
	}), claim.ID); err != nil {
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
