package testfixture

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/application"
	"github.com/woxQAQ/haven/operation"
)

// sandboxClaims borrows the App used by both claim admission and its operation
// wait, keeping the test's execution target fixed across those steps.
type SandboxClaims struct {
	App *application.App
}

func (fixture SandboxClaims) Claim(t *testing.T, key string, env map[string]operation.EnvVar, policy egress.Policy) operation.Record {
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
		reason := service.Workers.State(acceptedOperation.ID).Err
		t.Fatal("sandbox claim failed", acceptedOperation, err, reason)
	}
	return acceptedOperation
}
