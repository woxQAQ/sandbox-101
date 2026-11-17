package component

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
)

func TestHavenDataReadinessRenewsWithoutRenewingAttachedStreams(t *testing.T) {
	parallelLifecycle(t)
	service, _, _, _ := testapp.SandboxFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	principal := authorization.Principal{
		TenantID: "tenant",
	}
	accepted, err := service.SandboxRequests.Execute(ctx, principal, "auto-renew-stream", operation.Command{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		AutoRenew:  true,
		Policy: egress.Policy{
			Mode: "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	id := accepted.Result.SandboxID
	initial, err := service.Catalog.Get(ctx, principal, id)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := service.Dataplane.Ready(ctx, principal, id)
	if err != nil || !ready.Deadline.After(initial.Deadline) {
		t.Fatal("fresh readiness did not renew the lease", initial.Deadline, ready.Deadline, err)
	}
	upstream, err := service.Dataplane.Connect(ctx, principal, id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = upstream.Close()
	}()
	connected, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !connected.Deadline.After(ready.Deadline) {
		t.Fatal("fresh connection readiness did not renew the lease", connected.Deadline, err)
	}
	one, two := net.Pipe()
	defer func() {
		_ = one.Close()
	}()
	defer func() {
		_ = two.Close()
	}()
	downstream, err := service.Dataplane.Attach(ctx, principal, dataplane.HandoffRequest{
		SandboxID: id,
		Upstream:  upstream,
		Accept: func() (net.Conn, error) {
			return one, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = downstream.Close()
	}()
	attached, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !attached.Deadline.Equal(connected.Deadline) {
		t.Fatal("downstream handoff renewed the original upstream lease", attached.Deadline, err)
	}
	// Keep both endpoints registered across multiple enforcement intervals.
	select {
	case <-upstream.(*access.Connection).Done():
		t.Fatal("valid upstream was revoked during lease enforcement")
	case <-downstream.(*access.Connection).Done():
		t.Fatal("valid downstream was revoked during lease enforcement")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(350 * time.Millisecond):
	}
	observed, err := service.Catalog.Get(ctx, principal, id)
	if err != nil || !observed.Deadline.Equal(connected.Deadline) {
		t.Fatal("an attached stream renewed its lease during enforcement", observed.Deadline, err)
	}
}
