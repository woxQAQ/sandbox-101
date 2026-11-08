package sandboxd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestRestartReactivatesProxyBeforeAdmissionWithoutRenewing(t *testing.T) {
	s, _, agent, principal := fixture(t)
	result, err := s.Execute(
		context.Background(),
		principal,
		"claim-relay",
		Command{
			Action:     "claim",
			TemplateID: "python",
			Policy:     egress.Policy{Mode: "relay"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	before := *result.Sandbox
	opts := s.opts
	opts.Initialize = false
	opts.Egress = &fakeEgress{}
	opts.CAPEM = "rotated-proxy-ca"
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	after, err := restarted.DataReady(
		context.Background(),
		principal,
		before.ID,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Deadline.Equal(before.Deadline) ||
		after.Instance != before.Instance ||
		after.Epoch != before.Epoch {
		t.Fatal("proxy recovery changed the lease or identity")
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	activation := agent.activations[len(agent.activations)-1]
	if activation.CAPEM != opts.CAPEM || activation.ResetIdentity {
		t.Fatal(
			"admission did not install the new CA while preserving identity",
		)
	}
}

func TestAutomaticLeaseRenewalAndStaleExpiry(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, engine, _, principal := fixture(t)
			result, err := s.Execute(
				context.Background(),
				principal,
				"claim",
				Command{
					Action:     "claim",
					TemplateID: "python",
					TTLSeconds: 300,
					AutoRenew:  enabled,
					Policy:     egress.Policy{Mode: "none"},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			b := *result.Sandbox
			b.Deadline = time.Now().UTC().Add(2 * time.Second)
			if err = saveSandbox(
				context.Background(),
				s.store.db,
				b,
			); err != nil {
				t.Fatal(err)
			}
			ready, err := s.DataReady(context.Background(), principal, b.ID)
			if err != nil {
				t.Fatal(err)
			}
			if enabled && !ready.Deadline.After(b.Deadline.Add(time.Minute)) {
				t.Fatal("enabled activity renewal did not extend the lease")
			}
			if !enabled && !ready.Deadline.Equal(b.Deadline) {
				t.Fatal("disabled activity renewal changed the lease")
			}
			// A reaper decision made before a renewal must be rechecked under the
			// sandbox lock, even if the old deadline has already elapsed.
			stale := time.Now().UTC().Add(-time.Second)
			if _, err = s.Execute(
				context.Background(),
				principal,
				"stale-expiry",
				Command{
					Action:        "release",
					SandboxID:     b.ID,
					LeaseDeadline: stale,
				},
			); err != nil {
				t.Fatal(err)
			}
			if engine.count("delete") != 0 {
				t.Fatal("stale expiry deleted an active sandbox")
			}
			ready.Deadline = stale
			if err = saveSandbox(
				context.Background(),
				s.store.db,
				ready,
			); err != nil {
				t.Fatal(err)
			}
			if _, err = s.DataReady(
				context.Background(),
				principal,
				b.ID,
			); !isCode(
				err,
				api.Conflict,
			) {
				t.Fatalf("expired lease revived by activity: %v", err)
			}
			if _, err = s.Execute(
				context.Background(),
				principal,
				"current-expiry",
				Command{
					Action:        "release",
					SandboxID:     b.ID,
					LeaseDeadline: stale,
				},
			); err != nil {
				t.Fatal(err)
			}
			if engine.count("delete") != 1 {
				t.Fatal("current expiry did not delete the VM")
			}
		})
	}
}

func TestReconcileQueryFailureDoesNotEraseState(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	f.mu.Lock()
	f.unavailable = true
	f.mu.Unlock()
	s.reconcile()
	b, err := s.Get(context.Background(), p, r.Sandbox.ID)
	if err != nil || b.State != "running" {
		t.Fatal("query failure treated as resource absence")
	}
}
