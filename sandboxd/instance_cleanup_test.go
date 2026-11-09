package sandboxd

import (
	"bytes"
	"testing"
)

func TestQuotaLedgerHistoricalHandoffMergesCleanupHolder(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	ledgerExecute(t, s, p, "pause", Command{Action: "pause", SandboxID: b.ID})
	old := recoverySandbox(t, s, b.ID).SnapshotID
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	ledgerExecute(t, s, p, "resume", Command{Action: "resume", SandboxID: b.ID})
	b = recoverySandbox(t, s, b.ID)
	b.SnapshotID = old
	if err := s.saveSandboxReference(b); err != nil {
		t.Fatal(err)
	}
	if _, err := s.DataReady(s.ctx, p, b.ID); err != nil {
		t.Fatal("duplicate cleanup holder prevented atomic handoff", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
}

func TestQuotaLedgerOrdinaryReleaseCompensationSurvivesFreshRelease(
	t *testing.T,
) {
	s, f, _, p := recoveryFixture(t)
	ledgerLimit(t, s, p, 1, 64)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	f.control.Lock()
	f.failDeletes = 3
	f.control.Unlock()
	first, err := s.Execute(
		s.ctx,
		p,
		"release-A",
		Command{Action: "release", SandboxID: b.ID},
	)
	if err == nil {
		t.Fatal("public A failure missing")
	}
	origin, err := s.originalRelease(recoverySandbox(t, s, b.ID))
	if err != nil || origin == nil {
		t.Fatal("missing A intent", err)
	}
	frozen := encode(origin.Release.Request)
	second, err := s.Execute(
		s.ctx,
		p,
		"release-R2",
		Command{Action: "release", SandboxID: b.ID},
	)
	if err == nil || second.OperationID == first.OperationID {
		t.Fatal("fresh failed compensation not reached")
	}
	s = ledgerRestart(t, s)
	for range 4 {
		_ = s.recoverResources()
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	f.mu.Lock()
	deletes := f.physicalDeletes
	f.mu.Unlock()
	if deletes != 1 {
		t.Fatal("ordinary A cleanup did not finish exactly once")
	}
	all, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range all {
		if r.Release != nil &&
			(!bytes.Equal(frozen, encode(r.Release.Request)) || r.Source != origin.Source) {
			t.Fatal("fresh release created another A sequence")
		}
		if r.Kind == "instance_cleanup" {
			t.Fatal("ordinary A cleanup invented B proof")
		}
	}
}

func TestQuotaLedgerReleaseIntentAtomicity(t *testing.T) {
	for _, boundary := range []string{"intent", "marker"} {
		t.Run(boundary, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			trigger := `CREATE TRIGGER r3_release BEFORE INSERT ON resource_recovery WHEN json_extract(NEW.body,'$.release_intent') IS NOT NULL BEGIN SELECT RAISE(ABORT,'release intent'); END`
			if boundary == "marker" {
				trigger = `CREATE TRIGGER r3_release BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.release_intent.dispatched')=1 BEGIN SELECT RAISE(ABORT,'release marker'); END`
			}
			if _, err := s.store.db.Exec(trigger); err != nil {
				t.Fatal(err)
			}
			_, err := s.Execute(
				s.ctx,
				p,
				"release",
				Command{Action: "release", SandboxID: b.ID},
			)
			if err == nil {
				t.Fatal("SQL fault missing")
			}
			f.mu.Lock()
			physical := f.physicalDeletes
			f.mu.Unlock()
			if physical != 0 {
				t.Fatal("delete preceded durable intent/marker")
			}
			ledgerMoney(t, s, p.TenantID, 1, 64)
			s = ledgerRestart(t, s)
			if _, err = s.store.db.Exec(`DROP TRIGGER r3_release`); err != nil {
				t.Fatal(err)
			}
			if boundary == "intent" {
				ledgerExecute(
					t,
					s,
					p,
					"release-retry",
					Command{Action: "release", SandboxID: b.ID},
				)
			} else {
				for range 3 {
					_ = s.recoverResources()
				}
			}
			ledgerMoney(t, s, p.TenantID, 0, 0)
		})
	}
}
