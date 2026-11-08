package sandboxd

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestRejectedWarmFallsBackAndKeepsDeterministicReplay(t *testing.T) {
	for _, golden := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "cold", true: "golden"}[golden],
			func(t *testing.T) {
				s, engine, principal, _ := warmFallbackFixture(t, golden)
				result := claimTest(t, s, principal, "claim", nil)
				if result.Sandbox.ProvisionSource == "warm" ||
					result.Sandbox.State != "running" ||
					result.Sandbox.VMID == engine.warmCandidate {
					t.Fatalf(
						"rejected warm instance was delivered: %+v",
						result.Sandbox,
					)
				}
				if _, err := engine.GetVM(
					context.Background(),
					engine.warmCandidate,
				); !isCode(
					err,
					api.NotFound,
				) {
					t.Fatal("rejected warm instance was retained", err)
				}
				if engine.count("delete") != 1 {
					t.Fatal("warm instance was not deleted exactly once")
				}
				if golden &&
					(engine.count("clone") != 1 || engine.count("create") != 0) {
					t.Fatal("golden fallback did not use one clone")
				}
				if !golden &&
					(engine.count("create") != 1 || engine.count("start") != 1) {
					t.Fatal("cold fallback did not use one create/start")
				}
				replay := claimTest(t, s, principal, "claim", nil)
				if replay.Sandbox.VMID != result.Sandbox.VMID ||
					engine.count("delete") != 1 {
					t.Fatal("claim retry supplied another VM")
				}
			},
		)
	}
}

func TestWarmFallbackRecoversDeletionBeforeReferenceCommit(t *testing.T) {
	s, engine, principal, template := warmFallbackFixture(t, true)
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	o, err := s.Submit(
		s.ctx,
		principal,
		"recovering-claim",
		Command{
			Action:     "claim",
			TemplateID: template.ID,
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b := recoverySandbox(t, s, o.IDs[0])
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`CREATE TRIGGER fallback_metadata_fault BEFORE UPDATE ON sandboxes WHEN NEW.id='`+b.ID+`' AND json_extract(NEW.body,'$.vm_id') IS NULL BEGIN SELECT RAISE(ABORT,'fallback metadata fault'); END`,
	); err != nil {
		t.Fatal(err)
	}
	if err = s.claim(&o); err == nil {
		t.Fatal("fallback metadata fault was not observed")
	}
	if engine.count("delete") != 1 {
		t.Fatal("fault did not follow physical warm deletion")
	}
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`DROP TRIGGER fallback_metadata_fault`,
	); err != nil {
		t.Fatal(err)
	}
	// The durable sandbox still names the rejected VM, as after a crash between
	// confirmed engine deletion and the sandbox reference commit.
	if err := s.claim(&o); err != nil {
		t.Fatal(err)
	}
	if o.Result.Sandbox.ProvisionSource != "golden" ||
		engine.count("clone") != 1 {
		t.Fatal("replay failed to finish the original fallback")
	}
	if err := s.claim(&o); err != nil || engine.count("clone") != 1 {
		t.Fatal("replay duplicated the replacement", err)
	}
}

func TestWarmFallbackPreservesChangedInstance(t *testing.T) {
	s, engine, principal, template := warmFallbackFixture(t, true)
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	o, err := s.Submit(
		s.ctx,
		principal,
		"review",
		Command{Action: "claim", TemplateID: template.ID},
	)
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b := recoverySandbox(t, s, o.IDs[0])
	b.Instance = "old-instance"
	if err := s.discardWarm(
		o,
		&b,
		template,
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatalf("changed warm instance was not rejected: %v", err)
	}
	if engine.count("delete") != 0 || b.VMID == "" {
		t.Fatal("changed instance was destroyed or forgotten")
	}
}

func TestTemplateQuotaFallbackDestinationQualification(t *testing.T) {
	for _, damage := range []string{"none", "owner", "namespace", "shape", "key", "source", "reservation", "duplicate", "missing-selection"} {
		t.Run(damage, func(t *testing.T) {
			s, base, p, tpl := warmFallbackFixture(t, true)
			f := &recoveryTestEngine{fakeEngine: base}
			hidden := &templateQuotaEngine{
				Client:    f,
				attempts:  map[string]int{},
				hideClone: true,
			}
			e := &templateQuotaSelectionClient{Client: hidden}
			s.opts.Engine = e
			o, err := s.Submit(
				s.ctx,
				p,
				"destination-pending",
				Command{Action: "claim", TemplateID: tpl.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, api.Unknown) {
				t.Fatal("missing real unknown replacement", err)
			}
			r, err := s.loadRecovery(allocationGroupID(o))
			if err != nil {
				t.Fatal(err)
			}
			if r.Group.Selection.Stage != "fallback_reserved" ||
				r.Group.Identity != nil ||
				len(r.Allocations) != 1 ||
				!r.Group.Steps["provision"].Dispatched {
				t.Fatal("missing fallback pending destination")
			}
			public, _ := s.store.operation(s.ctx, o.ID)
			if damage == "none" {
				s = ledgerRestart(t, s)
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 1, 256)
				hidden.mu.Lock()
				hidden.hideClone = false
				hidden.mu.Unlock()
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				s = ledgerRestart(t, s)
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 0, 192)
				now, _ := s.store.operation(s.ctx, o.ID)
				if !bytes.Equal(encode(public), encode(now)) {
					t.Fatal("late receipt changed failed public bytes")
				}
				return
			}
			a := &r.Allocations[0]
			switch damage {
			case "owner":
				a.ChargeTenantID = "bob"
			case "namespace":
				a.TenantID = "bob"
			case "shape":
				a.Shape.VCPUs++
			case "key":
				a.Request.Key += ":other"
			case "source":
				a.Request.SnapshotID = "other-SID"
			case "reservation":
				if _, err = s.store.db.Exec(
					`DELETE FROM quota_resources WHERE kind='pending_storage' AND resource_key=?`,
					a.Request.Key,
				); err != nil {
					t.Fatal(err)
				}
			case "duplicate":
				r.Allocations = append(r.Allocations, *a)
			case "missing-selection":
				r.Group.Selection = nil
			}
			if _, err = s.store.db.Exec(
				`UPDATE resource_recovery SET body=? WHERE id=?`,
				encode(r),
				r.ID,
			); err != nil {
				t.Fatal(err)
			}
			before := templateQuotaState(t, s)
			n, finds := e.counts()
			if err = s.recoverResources(); !isCode(err, api.ManualRecovery) {
				t.Fatal("unqualified pending destination consumed", err)
			}
			if !bytes.Equal(before, templateQuotaState(t, s)) {
				t.Fatal("runtime repaired corrupt destination")
			}
			opts := s.opts
			opts.Initialize = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := Open(opts)
			if next != nil {
				next.Close()
			}
			if !isCode(err, api.ManualRecovery) ||
				!bytes.Equal(before, templateQuotaReadOnlyState(t, opts)) {
				t.Fatal("startup accepted or repaired destination", err)
			}
			if nn, ff := e.counts(); nn != n || ff != finds {
				t.Fatal("corruption reached Engine", nn-n, ff-finds)
			}
		})
	}
}

func TestTemplateQuotaFallbackCancelledAndUnknownStep(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(fmt.Sprint(unknown), func(t *testing.T) {
			s, base, p, tpl := warmFallbackFixture(t, true)
			e := &templateQuotaEngine{
				Client:      &recoveryTestEngine{fakeEngine: base},
				attempts:    map[string]int{},
				hideNetwork: unknown,
			}
			s.opts.Engine = e
			if !unknown {
				templateQuotaSQLFault(
					t,
					s,
					"resource_recovery",
					"UPDATE",
					"json_extract(NEW.body,'$.allocation_group.steps.provision.dispatched')=1",
					"abort",
				)
			}
			o, err := s.Submit(
				s.ctx,
				p,
				"destination-unfinished",
				Command{Action: "claim", TemplateID: tpl.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
				t.Fatal("expected unfinished original responsibility")
			}
			public, _ := s.store.operation(s.ctx, o.ID)
			if !unknown {
				if _, err = s.store.db.Exec(
					`DROP TRIGGER handoff_fault`,
				); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.recoverResources(); err != nil {
				t.Fatal(err)
			}
			r, err := s.loadRecovery(allocationGroupID(o))
			if err != nil {
				t.Fatal(err)
			}
			if unknown {
				if r.Done || groupStepsTerminal(r) {
					t.Fatal("unknown original step lost responsibility")
				}
				ledgerMoney(t, s, p.TenantID, 1, 192)
				s = ledgerRestart(t, s)
				e.mu.Lock()
				e.hideNetwork = false
				e.mu.Unlock()
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
			} else if r.Allocations[0].Resolution.State != "cancelled" { //nolint:misspell // Durable token.
				t.Fatal("undispatched replacement did not cancel")
			}
			s = ledgerRestart(t, s)
			if err = s.recoverResources(); err != nil {
				t.Fatal(err)
			}
			ledgerMoney(t, s, p.TenantID, 0, 192)
			now, _ := s.store.operation(s.ctx, o.ID)
			if !bytes.Equal(encode(public), encode(now)) {
				t.Fatal("private retirement changed public terminal bytes")
			}
		})
	}
}

func TestTemplateQuotaFallbackHistoricalContextRequired(t *testing.T) {
	s, _, p, tpl := warmFallbackFixture(t, true)
	claim := ledgerExecute(
		t,
		s,
		p,
		"historical-fallback-context",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	o, err := s.store.byKey(s.ctx, p.TenantID, "historical-fallback-context")
	if err != nil {
		t.Fatal(err)
	}
	ledgerExecute(
		t,
		s,
		p,
		"historical-context-pause",
		Command{Action: "pause", SandboxID: claim.Sandbox.ID},
	)
	ledgerExecute(
		t,
		s,
		p,
		"historical-context-release",
		Command{Action: "release", SandboxID: claim.Sandbox.ID},
	)
	s = ledgerRestart(t, s)
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	if r.Group.Selection == nil || r.Group.Identity == nil ||
		r.Group.Selection.Identity.Instance == r.Group.Identity.Instance {
		t.Fatal("missing distinct historical generations")
	}
	r.Group.Selection = nil
	if _, err = s.store.db.Exec(
		`UPDATE resource_recovery SET body=? WHERE id=?`,
		encode(r),
		r.ID,
	); err != nil {
		t.Fatal(err)
	}
	e := &templateQuotaSelectionClient{Client: s.opts.Engine}
	s.opts.Engine = e
	before := templateQuotaState(t, s)
	if err = s.recoverResources(); !isCode(err, api.ManualRecovery) {
		t.Fatal("later lifecycle hid required selection", err)
	}
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal("historical context was repaired")
	}
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(opts)
	if next != nil {
		next.Close()
	}
	if !isCode(err, api.ManualRecovery) ||
		!bytes.Equal(before, templateQuotaReadOnlyState(t, opts)) {
		t.Fatal("startup lost historical transfer", err)
	}
	if n, f := e.counts(); n != 0 || f != 0 {
		t.Fatal("historical corruption reached Engine")
	}
}
