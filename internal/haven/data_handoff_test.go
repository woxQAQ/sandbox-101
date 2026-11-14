package haven

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
	"github.com/woxQAQ/haven/loom"
)

func TestHavenDownstreamHandoffRetainsOriginalConfiguration(t *testing.T) {
	service, _, _, _ := sandboxFixture(t)
	principal := Principal{
		TenantID: "tenant",
	}
	operation := claimSandbox(t, service, "handoff", nil, egress.Policy{
		Mode: "none",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	upstream, err := service.Connect(ctx, principal, operation.Result.SandboxID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = upstream.Close()
	}()
	if _, err = service.Execute(ctx, principal, "handoff-new-configuration", Command{
		Action:    "configure",
		SandboxID: operation.Result.SandboxID,
		Policy: egress.Policy{
			Mode: "none",
		},
		Env: map[string]EnvVar{
			"VERSION": {
				Value: "next",
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	accepted := false
	downstream, err := service.AttachData(ctx, principal, operation.Result.SandboxID, upstream, func() (net.Conn, error) {
		accepted = true
		one, two := net.Pipe()
		_ = two.Close()
		return one, nil
	})
	if !isEffectCode(err, loom.Conflict) || accepted || downstream != nil {
		t.Fatal("late HTTP handoff accepted a superseded upstream", accepted, err)
	}
}
