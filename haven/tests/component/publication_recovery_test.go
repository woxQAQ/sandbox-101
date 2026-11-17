package component

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

func TestHavenStartupRejectsChangedDeliveryBeforeEffects(t *testing.T) {
	parallelLifecycle(t)
	for _, corruption := range []string{
		"identity",
		"http_error",
	} {
		t.Run(corruption, func(t *testing.T) {
			service, runtime, _, _ := testapp.SandboxFixture(t)
			operation := claimSandbox(t, service, "public-corruption", nil, egress.Policy{
				Mode: "none",
			})
			view, err := service.Delivery.PublicOperation(context.Background(), authorization.Principal{
				TenantID: "tenant",
			}, operation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = service.State.Database().Exec("UPDATE operation_metadata SET created_at='2000-01-01T00:00:00Z' WHERE operation_id=?", operation.ID); err == nil {
				t.Fatal("accepted operation time can be rewritten")
			}
			// Model corrupt persisted bytes in an owned fixture. Startup must
			// reject changed delivery facts before opening a writable DB.
			if _, err = service.State.Database().Exec(`DROP TRIGGER publication_immutable; CREATE TRIGGER publication_immutable BEFORE UPDATE ON operation_publications BEGIN SELECT 1; END`); err != nil {
				t.Fatal(err)
			}
			view.Result.Tokens = nil
			switch corruption {
			case "identity":
				view.Key = "changed-request-identity"
			case "http_error":
				view.Error = &types.ApiError{
					Code:    types.ErrInternal,
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
			before := storeSnapshot(t, options.StateDir)
			launches := runtime.Count("launch")
			opened, err := application.Open(options)
			if opened != nil {
				_ = opened.Close(context.Background())
			}
			if !errors.Is(err, store.ErrRecovery) {
				t.Fatal("corrupt delivery passed startup", err)
			}
			if after := storeSnapshot(t, options.StateDir); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected delivery changed private state")
			}
			if runtime.Count("launch") != launches {
				t.Fatal("rejected delivery launched effects")
			}
		})
	}
}
