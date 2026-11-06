package sandboxd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestRestartReactivatesProxyBeforeAdmissionWithoutRenewing(t *testing.T) {
	s, _, agent, principal := fixture(t)
	result, err := s.Execute(context.Background(), principal, "claim-relay", Command{Action: "claim", TemplateID: "python", Policy: egress.Policy{Mode: "relay"}})
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
	after, err := restarted.DataReady(context.Background(), principal, before.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !after.Deadline.Equal(before.Deadline) || after.Instance != before.Instance || after.Epoch != before.Epoch {
		t.Fatal("proxy recovery changed the lease or identity")
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	activation := agent.activations[len(agent.activations)-1]
	if activation.CAPEM != opts.CAPEM || activation.ResetIdentity {
		t.Fatal("admission did not install the new CA while preserving identity")
	}
}

func TestAutomaticLeaseRenewalAndStaleExpiry(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			s, engine, _, principal := fixture(t)
			result, err := s.Execute(context.Background(), principal, "claim", Command{Action: "claim", TemplateID: "python", TTLSeconds: 300, AutoRenew: enabled, Policy: egress.Policy{Mode: "none"}})
			if err != nil {
				t.Fatal(err)
			}
			b := *result.Sandbox
			b.Deadline = time.Now().UTC().Add(2 * time.Second)
			if err = saveSandbox(context.Background(), s.store.db, b); err != nil {
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
			if _, err = s.Execute(context.Background(), principal, "stale-expiry", Command{Action: "release", SandboxID: b.ID, LeaseDeadline: stale}); err != nil {
				t.Fatal(err)
			}
			if engine.count("delete") != 0 {
				t.Fatal("stale expiry deleted an active sandbox")
			}
			ready.Deadline = stale
			if err = saveSandbox(context.Background(), s.store.db, ready); err != nil {
				t.Fatal(err)
			}
			if _, err = s.DataReady(context.Background(), principal, b.ID); !isCode(err, api.Conflict) {
				t.Fatalf("expired lease revived by activity: %v", err)
			}
			if _, err = s.Execute(context.Background(), principal, "current-expiry", Command{Action: "release", SandboxID: b.ID, LeaseDeadline: stale}); err != nil {
				t.Fatal(err)
			}
			if engine.count("delete") != 1 {
				t.Fatal("current expiry did not delete the VM")
			}
		})
	}
}

func TestRefillBackoffSurvivesRestartAndSuccessResets(t *testing.T) {
	s, _, _, _ := fixture(t)
	now := time.Now().UTC()
	for n := 1; n <= 10; n++ {
		o := Operation{ID: fmt.Sprint("refill-", n), TenantID: "alice", Key: fmt.Sprint("refill-", n), Hash: "test", Status: "failed", Command: Command{Action: "refill", TemplateID: "python"}, CreatedAt: now.Add(-time.Hour), FinishedAt: now}
		if err := saveOp(context.Background(), s.store.db, o); err != nil {
			t.Fatal(err)
		}
		delay := time.Second << min(n-1, 8)
		if blocked, err := s.refillBackoff("python", now.Add(delay-time.Nanosecond)); err != nil || !blocked {
			t.Fatalf("failure %d lost its backoff: %v", n, err)
		}
		if blocked, err := s.refillBackoff("python", now.Add(delay)); err != nil || blocked {
			t.Fatalf("failure %d backoff never ended: %v", n, err)
		}
	}
	opts := s.opts
	opts.Initialize = false
	s.Close()
	restarted, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if blocked, err := restarted.refillBackoff("python", now.Add(time.Second)); err != nil || !blocked {
		t.Fatalf("restart reset the failure streak: %v", err)
	}
	o := Operation{ID: "refill-success", TenantID: "alice", Key: "refill-success", Hash: "test", Status: "succeeded", Command: Command{Action: "refill", TemplateID: "python"}, CreatedAt: now, FinishedAt: now}
	if err = saveOp(context.Background(), restarted.store.db, o); err != nil {
		t.Fatal(err)
	}
	if blocked, err := restarted.refillBackoff("python", now); err != nil || blocked {
		t.Fatalf("successful refill did not reset the streak: %v", err)
	}
}

func TestFutureStoreSchemaIsRejectedBeforeDDL(t *testing.T) {
	dir := t.TempDir()
	store, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil || info.Mode().Perm() != 0700 {
		t.Fatal("state directory was not made private", err)
	}
	if _, err = store.db.Exec("UPDATE version SET version=2; DROP TABLE checkpoints"); err != nil {
		t.Fatal(err)
	}
	store.close()
	if _, err = openStore(dir, false); !isCode(err, api.Incompatible) {
		t.Fatalf("future schema accepted: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "sandboxd.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var tables, version int
	if err = db.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='checkpoints'").Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow("SELECT version FROM version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if tables != 0 || version != 2 {
		t.Fatal("opening an incompatible database changed its schema")
	}
}
