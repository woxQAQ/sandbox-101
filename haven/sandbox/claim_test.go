package sandbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/tests/fixtures"
)

// The published network vocabulary is none/proxy/direct. AdmitClaim rejects the
// retired "relay" spelling rather than aliasing it, and an omitted mode settles
// on proxy so callers that never chose a mode keep the host-proxied path.
func TestHavenClaimNetworkModeVocabulary(t *testing.T) {
	fixtures.ParallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	admit := func(key, mode string) (operation.Record, error) {
		return service.Sandboxes.AdmitClaim(context.Background(), principal, key, operation.Command{
			Action:     "claim",
			TemplateID: "cold",
			TTLSeconds: 60,
			Policy: egress.Policy{
				Mode: mode,
			},
		})
	}
	if _, err := admit("retired-relay", "relay"); !errors.Is(err, errs.ErrInvalid) {
		t.Fatalf("retired relay spelling was not rejected as invalid: %v", err)
	}
	// An empty mode resolves to proxy: the durable settings must record proxy,
	// not the caller's blank sentinel.
	accepted, err := admit("default-mode", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err = service.Operations.Wait(ctx, principal, accepted.ID); err != nil {
		t.Fatal(err)
	}
	view, err := service.Catalog.Get(ctx, principal, accepted.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Policy.Mode != "proxy" {
		t.Fatalf("default network mode did not settle on proxy: %q", view.Policy.Mode)
	}
}
