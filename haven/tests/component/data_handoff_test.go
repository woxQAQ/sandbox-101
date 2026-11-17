package component

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenDownstreamHandoffRetainsOriginalConfiguration(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	acceptedRecord := (sandboxClaims{
		App: service,
	}).claim(t, "handoff", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	upstream, err := service.Dataplane.Connect(ctx, principal, acceptedRecord.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = upstream.Close()
	}()
	if _, err = service.SandboxRequests.Execute(ctx, principal, "handoff-new-configuration", operation.Command{
		Action:    "configure",
		SandboxID: acceptedRecord.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
		Env: map[string]operation.EnvVar{
			"VERSION": {
				Value: "next",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	accepted := false
	downstream, err := service.Dataplane.Attach(ctx, principal, dataplane.HandoffRequest{
		SandboxID: acceptedRecord.Result.SandboxID,
		Upstream:  upstream,
		Accept: func() (net.Conn, error) {
			accepted = true
			one, two := net.Pipe()
			_ = two.Close()
			return one, nil
		},
	})
	if !errors.Is(err, errs.ErrConflict) || accepted || downstream != nil {
		t.Fatal("late HTTP handoff accepted a superseded upstream", accepted, err)
	}
}
