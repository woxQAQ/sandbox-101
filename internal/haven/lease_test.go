package haven

import (
	"context"
	"testing"
	"time"

	"github.com/woxQAQ/haven/internal/egress"
)

func TestHavenRenewPreservesConfiguration(t *testing.T) {
	service, _, _, _ := sandboxFixture(t)
	principal := Principal{TenantID: "tenant"}
	claim := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "none"})
	id := claim.Result.SandboxID
	before, err := loadSettings(context.Background(), service.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := service.AdmitRenew(context.Background(), Principal{TenantID: "tenant", SandboxID: id}, "renew", Command{Action: "renew", SandboxID: id, TTLSeconds: 120})
	if err != nil {
		t.Fatal(err)
	}
	after, err := loadSettings(context.Background(), service.store.db, id)
	if err != nil || !after.Deadline.After(before.Deadline) || after.Generation != before.Generation || after.ActiveEpoch != before.ActiveEpoch || after.Instance != before.Instance {
		t.Fatal(before, after, err)
	}
	replay, err := service.AdmitRenew(context.Background(), Principal{TenantID: "tenant", SandboxID: id}, "renew", Command{Action: "renew", SandboxID: id, TTLSeconds: 120})
	if err != nil || replay.ID != operation.ID {
		t.Fatal(replay, err)
	}
	if _, err = service.AdmitRenew(context.Background(), Principal{TenantID: "other"}, "renew", Command{Action: "renew", SandboxID: id}); err == nil {
		t.Fatal("foreign renew accepted")
	}
	if _, err = service.AdmitLifecycle(context.Background(), principal, "stale-expiration", Command{Action: "release", SandboxID: id, LeaseDeadline: before.Deadline}); err == nil {
		t.Fatal("stale deadline revoked renewed lease")
	}
	if _, err = service.DataReady(context.Background(), principal, id); err != nil {
		t.Fatal(err)
	}
}

func TestHavenLeaseReaper(t *testing.T) {
	for _, state := range []string{"kill", "pause", "paused"} {
		t.Run(state, func(t *testing.T) {
			service, _, _, _ := sandboxFixture(t)
			principal := Principal{TenantID: "tenant"}
			claim := claimSandbox(t, service, "claim", nil, egress.Policy{Mode: "none"})
			id := claim.Result.SandboxID
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if state == "paused" {
				if _, err := service.Execute(ctx, principal, "pause", Command{Action: "pause", SandboxID: id}); err != nil {
					t.Fatal(err)
				}
			}
			settings, err := loadSettings(ctx, service.store.db, id)
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().UTC().Add(-time.Second)
			if state == "paused" {
				settings.PausedUntil = deadline
			} else {
				settings.Deadline = deadline
				settings.OnExpire = state
			}
			if err = requireWrite(service.store.db.ExecContext(ctx, "UPDATE sandbox_settings SET body=? WHERE sandbox_id=?", encode(settings), id)); err != nil {
				t.Fatal(err)
			}
			if _, err = service.AdmitRenew(ctx, principal, "late-renew", Command{Action: "renew", SandboxID: id}); err == nil {
				t.Fatal("expired lease revived")
			}
			if err = service.Reap(ctx); err != nil {
				t.Fatal(err)
			}
			var operation string
			if err = service.store.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE scope_id='tenant' AND idem_key=?", "expire:"+id+":"+deadline.Format(time.RFC3339Nano)).Scan(&operation); err != nil {
				t.Fatal(err)
			}
			if _, err = service.Wait(ctx, principal, operation); err != nil {
				t.Fatal(err)
			}
			sandbox, err := service.Get(ctx, principal, id)
			want := "terminated"
			if state == "pause" {
				want = "paused"
			}
			if err != nil || sandbox.State != want {
				t.Fatal(sandbox, err)
			}
		})
	}
}
