package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestTemplateQuotaSelectedReaderRetirement(t *testing.T) {
	s, f, p, tpl := templateQuotaGolden(t)
	oldSID := tpl.SnapshotID
	tpl = templateQuotaTarget(t, s, p, tpl, 1)
	s.pools()
	s.wg.Wait()
	claim := ledgerExecute(
		t,
		s,
		p,
		"reader-claim",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	if claim.Sandbox.ProvisionSource != "warm" {
		t.Fatal("ready warm not selected")
	}
	o, err := s.store.byKey(s.ctx, p.TenantID, "reader-claim")
	if err != nil {
		t.Fatal(err)
	}
	public := encode(o)
	templateQuotaTarget(t, s, p, tpl, 0)
	ledgerExecute(
		t,
		s,
		p,
		"reader-release",
		Command{Action: "release", SandboxID: claim.Sandbox.ID},
	)
	newer := ledgerExecute(
		t,
		s,
		p,
		"reader-rebuild",
		templateQuotaBuild(tpl.ID),
	)
	for round := 0; round < 2; round++ {
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		s = ledgerRestart(t, s)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	var readers int
	if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, oldSID, "operation:"+o.ID).
		Scan(&readers); err != nil {
		t.Fatal(err)
	}
	if readers != 0 {
		t.Fatal("successful selection kept old reader")
	}
	if _, err = f.GetSnapshot(s.ctx, oldSID); !isCode(err, api.NotFound) {
		t.Fatal("obsolete SID retained", err)
	}
	if _, err = f.GetSnapshot(s.ctx, newer.Template.SnapshotID); err != nil {
		t.Fatal(err)
	}
	now, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(public, encode(now)) {
		t.Fatal("old successful public bytes changed")
	}
	ledgerLimit(t, s, p, 2, 447)
	next := ledgerExecute(
		t,
		s,
		p,
		"reader-new-claim",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	if next.Sandbox.VMID == claim.Sandbox.VMID {
		t.Fatal("reused released VM")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
}

func TestTemplateQuotaReadyWarmStartup(t *testing.T) {
	for _, damage := range []string{"disk-cost", "disk-source", "warm-holder", "backing-holder"} {
		t.Run(damage, func(t *testing.T) {
			s, f, p, tpl := templateQuotaGolden(t)
			templateQuotaTarget(t, s, p, tpl, 1)
			s.pools()
			s.wg.Wait()
			var vm string
			if err := s.store.db.QueryRow(`SELECT vm_id FROM warm`).
				Scan(&vm); err != nil {
				t.Fatal(err)
			}
			statement := map[string]string{
				"disk-cost":      `UPDATE quota_resources SET storage_mib=1 WHERE kind='vm_disk' AND resource_key=?`,
				"disk-source":    `UPDATE quota_resources SET source_operation_id='foreign' WHERE kind='vm_disk' AND resource_key=?`,
				"warm-holder":    `DELETE FROM quota_refs WHERE kind='vm_disk' AND holder_kind='active_use' AND holder_id='warm:'||?`,
				"backing-holder": `DELETE FROM quota_refs WHERE kind='snapshot' AND holder_kind='active_use' AND holder_id='backing:'||?`,
			}[damage]
			if _, err := s.store.db.Exec(statement, vm); err != nil {
				t.Fatal(err)
			}
			before := templateQuotaState(t, s)
			e := &templateQuotaSelectionClient{Client: f}
			s.opts.Engine = e
			opts := s.opts
			opts.Initialize = false
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			next, err := Open(opts)
			if next != nil {
				next.Close()
			}
			if !isCode(err, api.ManualRecovery) {
				t.Fatal("corrupt ready warm accepted", err)
			}
			if !bytes.Equal(before, templateQuotaReadOnlyState(t, opts)) {
				t.Fatal("startup repaired corrupted responsibility")
			}
			if n, finds := e.counts(); n != 0 || finds != 0 {
				t.Fatal("startup called Engine")
			}
			if _, err = f.GetVM(context.Background(), vm); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("old-version-and-claimed-history", func(t *testing.T) {
		s, f, p, tpl := templateQuotaGolden(t)
		tpl = templateQuotaTarget(t, s, p, tpl, 1)
		s.pools()
		s.wg.Wait()
		var vm string
		if err := s.store.db.QueryRow(`SELECT vm_id FROM warm`).
			Scan(&vm); err != nil {
			t.Fatal(err)
		}
		command := templateQuotaBuild(tpl.ID)
		command.Template.TargetWarm = tpl.TargetWarm
		newer := ledgerExecute(t, s, p, "ready-new-version", command).Template
		if newer.SnapshotID == tpl.SnapshotID {
			t.Fatal("same SID")
		}
		s = ledgerRestart(t, s)
		// The old ready version remains valid while waiting for a pool sweep.
		if _, err := f.GetVM(s.ctx, vm); err != nil {
			t.Fatal(err)
		}
		s.pools()
		s.wg.Wait()
		claim := ledgerExecute(
			t,
			s,
			p,
			"ready-claim",
			Command{Action: "claim", TemplateID: tpl.ID},
		)
		s = ledgerRestart(t, s)
		ledgerExecute(
			t,
			s,
			p,
			"ready-pause",
			Command{Action: "pause", SandboxID: claim.Sandbox.ID},
		)
		ledgerExecute(
			t,
			s,
			p,
			"ready-release",
			Command{Action: "release", SandboxID: claim.Sandbox.ID},
		)
		s = ledgerRestart(t, s)
		if err := s.recoverResources(); err != nil {
			t.Fatal(err)
		}
	})
}

func templateQuotaRejectedSelection(
	t *testing.T,
	history bool,
) (*Service, *recoveryTestEngine, *templateQuotaSelectionClient, Principal, Operation, resourceRecovery) {
	t.Helper()
	s, base, p, tpl := warmFallbackFixture(t, true)
	f := &recoveryTestEngine{fakeEngine: base}
	e := &templateQuotaSelectionClient{Client: f, unknown: !history}
	if history {
		f.failDeletes = 1
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"qualified-selection",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(ctx, p, o.ID); err == nil {
		t.Fatal("expected rejected warm failure")
	}
	if history {
		e.mu.Lock()
		e.unknown = true
		e.mu.Unlock()
		if err = s.recoverResources(); !isCode(err, api.Unknown) {
			t.Fatal(err)
		}
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	if r.Group.Selection.Stage != "rejected" ||
		!r.Group.Selection.Deletion.Dispatched {
		t.Fatal("no durable deletion marker")
	}
	if history &&
		(r.Group.Selection.Attempt != 1 || len(r.Group.Selection.History) != 1) {
		t.Fatal("terminal failure history missing")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	return s, f, e, p, o, r
}

func TestTemplateQuotaSelectionReplayBoundaries(t *testing.T) {
	for _, damage := range []string{"receipt-vm", "receipt-key", "receipt-status", "receipt-request", "receipt-result"} {
		t.Run(damage, func(t *testing.T) {
			s, _, e, p, o, r := templateQuotaRejectedSelection(t, false)
			q := r.Group.Selection
			proof := api.Operation{
				ID:      "engine-failed-delete",
				Key:     q.Deletion.Request.Key,
				Request: q.Deletion.Request,
				VMID:    q.Identity.VM,
				Status:  "failed",
				Error:   &api.Error{Code: api.Internal, Message: "failed"},
			}
			switch damage {
			case "receipt-vm":
				proof.VMID = "foreign-V"
			case "receipt-key":
				proof.Key = "foreign-key"
			case "receipt-status":
				proof.Status = "invented"
			case "receipt-request":
				proof.Request.Policy = &api.NetworkPolicy{}
			case "receipt-result":
				proof.ResultInstance = "foreign-B"
			}
			e.mu.Lock()
			e.forged = &proof
			e.mu.Unlock()
			before := templateQuotaState(t, s)
			n, _ := e.counts()
			selected := recoverySandbox(t, s, r.Group.Subject.ID)
			if err := s.discardWarm(
				o,
				&selected,
				r.Group.Template,
			); !isCode(
				err,
				api.ManualRecovery,
			) {
				t.Fatal("malformed receipt was adopted", err)
			}
			if after, _ := e.counts(); after != n {
				t.Fatal("malformed receipt authorized new dispatch")
			}
			if !bytes.Equal(before, templateQuotaState(t, s)) {
				t.Fatal("malformed receipt changed history or fee")
			}
			ledgerMoney(t, s, p.TenantID, 1, 256)
		})
	}
	t.Run("fresh-read-after-find-not-found", func(t *testing.T) {
		s, _, e, p, o, r := templateQuotaRejectedSelection(t, false)
		var changed []byte
		var once sync.Once
		e.mu.Lock()
		e.findHook = func(key string) {
			if key != r.Group.Selection.Deletion.Request.Key {
				return
			}
			once.Do(func() {
				r.Group.Selection.Deletion.Request.ExpectedInstance = ""
				if _, err := s.store.db.Exec(
					`UPDATE resource_recovery SET body=? WHERE id=?`,
					encode(r),
					r.ID,
				); err != nil {
					t.Error(err)
				}
				changed = templateQuotaState(t, s)
			})
		}
		e.mu.Unlock()
		n, _ := e.counts()
		b := recoverySandbox(t, s, r.Group.Subject.ID)
		if err := s.discardWarm(
			o,
			&b,
			r.Group.Template,
		); !isCode(
			err,
			api.ManualRecovery,
		) {
			t.Fatal("post-Find corruption reached dispatch", err)
		}
		if len(changed) == 0 ||
			!bytes.Equal(changed, templateQuotaState(t, s)) {
			t.Fatal("corruption was repaired or consumed")
		}
		if after, _ := e.counts(); after != n {
			t.Fatal("Find NotFound allowed invalid dispatch")
		}
		ledgerMoney(t, s, p.TenantID, 1, 256)
	})
	t.Run("fallback-history-keeps-original-generation", func(t *testing.T) {
		s, _, p, tpl := warmFallbackFixture(t, true)
		claim := ledgerExecute(
			t,
			s,
			p,
			"history-fallback",
			Command{Action: "claim", TemplateID: tpl.ID},
		)
		o, err := s.store.byKey(s.ctx, p.TenantID, "history-fallback")
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.loadRecovery(allocationGroupID(o))
		if err != nil {
			t.Fatal(err)
		}
		if r.Group.Selection.Stage != "fallback_reserved" ||
			r.Group.Identity == nil ||
			r.Group.Identity.Instance == r.Group.Selection.Identity.Instance {
			t.Fatal("replacement did not retain distinct original warm history")
		}
		old := templateQuotaCopy(r.Group.Selection)
		s = ledgerRestart(t, s)
		ledgerExecute(
			t,
			s,
			p,
			"history-pause",
			Command{Action: "pause", SandboxID: claim.Sandbox.ID},
		)
		ledgerExecute(
			t,
			s,
			p,
			"history-release",
			Command{Action: "release", SandboxID: claim.Sandbox.ID},
		)
		s = ledgerRestart(t, s)
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
		r, err = s.loadRecovery(r.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encode(old), encode(r.Group.Selection)) {
			t.Fatal("normal lifecycle rewrote original warm history")
		}
	})
}

func TestTemplateQuotaSelectionQualification(t *testing.T) {
	permits := make(chan struct{}, 4)
	cases := []struct {
		name    string
		history bool
		damage  func(*resourceRecovery)
	}{
		{
			"empty-predicate",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Request.ExpectedInstance = "" },
		},
		{
			"wrong-generation",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Request.ExpectedInstance = "foreign-B" },
		},
		{
			"foreign-vm",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Request.VMID = "foreign-V" },
		},
		{
			"wrong-action",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Request.Action = "start" },
		},
		{"foreign-key", false, func(r *resourceRecovery) {
			r.Group.Selection.Deletion.Request.Key = "sbx-op:foreign:step:warm-reject:" + r.Group.Subject.ID
		}},
		{"extra-request", false, func(r *resourceRecovery) {
			r.Group.Selection.Deletion.Request.Config = &api.Config{
				Shape: r.Group.Template.Shape,
			}
			r.Group.Selection.Deletion.Request.NetworkEnabled = true
		}},
		{
			"wrong-origin",
			false,
			func(r *resourceRecovery) { r.Source = "foreign-origin" },
		},
		{
			"wrong-refill",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Refill = "allocation:foreign" },
		},
		{"owner-shape", false, func(r *resourceRecovery) {
			r.Group.Selection.Identity.Owner = "bob"
			r.Group.Selection.Identity.Shape.VCPUs++
		}},
		{
			"generation-receipt",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Identity.Generation.ResultInstance = "different-B" },
		},
		{
			"stage",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Stage = "active" },
		},
		{
			"unbound-dispatched",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Bound = false },
		},
		{
			"undispatched-request",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Deletion.Dispatched = false },
		},
		{"proof", false, func(r *resourceRecovery) {
			q := r.Group.Selection
			q.Deletion.Proof = &api.Operation{
				ID:      "wrong-receipt",
				Key:     q.Deletion.Request.Key,
				Request: q.Deletion.Request,
				VMID:    "foreign-V",
				Status:  "succeeded",
			}
		}},
		{
			"attempt-without-history",
			false,
			func(r *resourceRecovery) { r.Group.Selection.Attempt++ },
		},
		{
			"history-request",
			true,
			func(r *resourceRecovery) { r.Group.Selection.History[0].Request.ExpectedInstance = "foreign-B" },
		},
		{
			"history-not-terminal-failed",
			true,
			func(r *resourceRecovery) { r.Group.Selection.History[0].Receipt.Status = "running" },
		},
		{
			"history-receipt-identity",
			true,
			func(r *resourceRecovery) { r.Group.Selection.History[0].Receipt.VMID = "foreign-V" },
		},
		{"history-order", true, func(r *resourceRecovery) {
			r.Group.Selection.History[0].Request.Key = r.Group.Selection.Deletion.Request.Key
		}},
	}
	for index, tc := range cases {
		for _, startup := range []bool{true, false} {
			t.Run(
				fmt.Sprintf("%s/startup-%t", tc.name, startup),
				func(t *testing.T) {
					t.Parallel()
					permits <- struct{}{}
					defer func() { <-permits }()
					s, f, e, p, o, r := templateQuotaRejectedSelection(
						t,
						tc.history,
					)
					public, _ := s.store.operation(s.ctx, o.ID)
					original := encode(public)
					tc.damage(&r)
					if _, err := s.store.db.Exec(
						`UPDATE resource_recovery SET body=? WHERE id=?`,
						encode(r),
						r.ID,
					); err != nil {
						t.Fatal(err)
					}
					before := templateQuotaState(t, s)
					n, finds := e.counts()
					if startup {
						opts := s.opts
						opts.Initialize = false
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						next, err := Open(opts)
						if next != nil {
							next.Close()
						}
						if !isCode(err, api.ManualRecovery) {
							t.Fatal("startup accepted invalid selection", err)
						}
						if !bytes.Equal(
							before,
							templateQuotaReadOnlyState(t, opts),
						) {
							t.Fatal("startup changed durable rows")
						}
					} else {
						var err error
						if index%2 == 0 {
							err = s.recoverResources()
						} else {
							b := recoverySandbox(t, s, r.Group.Subject.ID)
							err = s.discardWarm(o, &b, r.Group.Template)
						}
						if !isCode(err, api.ManualRecovery) {
							t.Fatal("runtime accepted invalid selection", err)
						}
						if !bytes.Equal(before, templateQuotaState(t, s)) {
							t.Fatal("runtime changed durable responsibility")
						}
						now, _ := s.store.operation(s.ctx, o.ID)
						if !bytes.Equal(original, encode(now)) {
							t.Fatal("public terminal bytes changed")
						}
						ledgerMoney(t, s, p.TenantID, 1, 256)
					}
					if after, afterFind := e.counts(); after != n ||
						afterFind != finds {
						t.Fatalf(
							"unqualified Engine attempt: submit %d->%d find %d->%d",
							n,
							after,
							finds,
							afterFind,
						)
					}
					if _, err := f.GetVM(
						context.Background(),
						r.Group.Selection.Identity.VM,
					); err != nil {
						t.Fatal("resource disappeared", err)
					}
				},
			)
		}
	}
	t.Run("valid-unknown-and-terminal-failed-history", func(t *testing.T) {
		for _, history := range []bool{false, true} {
			t.Run(fmt.Sprint(history), func(t *testing.T) {
				s, f, e, p, o, r := templateQuotaRejectedSelection(t, history)
				public, _ := s.store.operation(s.ctx, o.ID)
				original := encode(public)
				fixed := templateQuotaCopy(r.Group.Selection)
				s = ledgerRestart(t, s)
				if err := s.recoverResources(); !isCode(err, api.Unknown) {
					t.Fatal(err)
				}
				latest, err := s.loadRecovery(r.ID)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(encode(fixed), encode(latest.Group.Selection)) {
					t.Fatal("unknown changed key/history")
				}
				e.mu.Lock()
				e.unknown = false
				requests := append([]api.Request(nil), e.requests...)
				e.mu.Unlock()
				for _, request := range requests {
					if request.Action == "delete" &&
						(request.VMID != fixed.Identity.VM || request.ExpectedInstance != fixed.Identity.Instance) {
						t.Fatal("retry changed V/B")
					}
				}
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				s = ledgerRestart(t, s)
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 0, 192)
				expected := 1
				if history {
					expected++
				}
				if f.count("delete") != expected {
					t.Fatal("unexpected physical delete sequence")
				}
				now, _ := s.store.operation(s.ctx, o.ID)
				if !bytes.Equal(original, encode(now)) {
					t.Fatal("failed public bytes changed")
				}
			})
		}
	})
	t.Run("absence-without-delete-marker", func(t *testing.T) {
		s, base, p, tpl := warmFallbackFixture(t, true)
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()
		o, err := s.Submit(
			s.ctx,
			p,
			"selected-absence",
			Command{Action: "claim", TemplateID: tpl.ID},
		)
		if err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.closing = false
		s.mu.Unlock()
		r, err := s.loadRecovery(allocationGroupID(o))
		if err != nil {
			t.Fatal(err)
		}
		i := r.Group.Selection.Identity
		if _, err = api.Wait(
			s.ctx,
			base,
			api.Request{
				Key:              "external-selected-delete",
				Action:           "delete",
				VMID:             i.VM,
				ExpectedInstance: i.Instance,
			},
		); err != nil {
			t.Fatal(err)
		}
		e := &templateQuotaSelectionClient{Client: base}
		s.opts.Engine = e
		ledgerExecute(
			t,
			s,
			p,
			"selected-release",
			Command{Action: "release", SandboxID: r.Group.Subject.ID},
		)
		s = ledgerRestart(t, s)
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
		if n, _ := e.counts(); n != 0 {
			t.Fatal("absence invented delete/replacement")
		}
		r, err = s.loadRecovery(r.ID)
		if err != nil || r.Group.Selection.Deletion.Bound ||
			r.Group.Selection.Stage != "cleanup_done" {
			t.Fatal("unbound absence destination invalid", err)
		}
	})
}

func TestTemplateQuotaSelectionDestinationRequired(t *testing.T) {
	for _, damage := range []string{"fallback_reserved", "cleanup_done", "missing-selection"} {
		for _, startup := range []bool{true, false} {
			t.Run(
				fmt.Sprintf("%s/startup-%t", damage, startup),
				func(t *testing.T) {
					s, f, e, p, _, r := templateQuotaRejectedSelection(t, false)
					i := r.Group.Selection.Identity
					if damage == "missing-selection" {
						r.Group.Selection = nil
					} else {
						r.Group.Selection.Stage = damage
					}
					result, err := s.store.db.Exec(
						`UPDATE resource_recovery SET body=? WHERE id=?`,
						encode(r),
						r.ID,
					)
					if err = requireSQLWrite(result, err); err != nil {
						t.Fatal(err)
					}
					before := templateQuotaState(t, s)
					n, finds := e.counts()
					opts := s.opts
					opts.Initialize = false
					if !startup {
						if err = s.recoverResources(); !isCode(
							err,
							api.ManualRecovery,
						) {
							t.Fatal("runtime consumed invalid destination", err)
						}
						if !bytes.Equal(before, templateQuotaState(t, s)) {
							t.Fatal("runtime changed full responsibility")
						}
						if _, err = s.loadRecovery(
							r.ID,
						); !isCode(
							err,
							api.ManualRecovery,
						) {
							t.Fatal(
								"durable read accepted incomplete context",
								err,
							)
						}
						ledgerMoney(t, s, p.TenantID, 1, 256)
					}
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
					next, err := Open(opts)
					if next != nil {
						next.Close()
					}
					if !isCode(err, api.ManualRecovery) {
						t.Fatal("startup consumed invalid destination", err)
					}
					if !bytes.Equal(
						before,
						templateQuotaReadOnlyState(t, opts),
					) {
						t.Fatal("startup repaired or retired responsibility")
					}
					if nn, ff := e.counts(); nn != n || ff != finds {
						t.Fatal(
							"invalid context reached Engine",
							nn-n,
							ff-finds,
						)
					}
					v, err := f.GetVM(context.Background(), i.VM)
					if err != nil || v.Instance != i.Instance {
						t.Fatal("original V/B changed", err)
					}
					t.Log(
						"actual startup/runtime rejection preserves full DB, reader, public bytes and slot1/D64/T192; no Submit/Find",
					)
				},
			)
		}
	}
}

// Each destination is reached through claim, receipt adoption and recovery.
// Change only one actual reference after the producer has committed it.
func TestTemplateQuotaDestinationReferenceKinds(t *testing.T) {
	for _, tc := range []struct{ phase, damage string }{
		{"pending", "subject"},
		{"pending", "slot"},
		{"pending", "reader"},
		{"known", "subject"},
		{"cleanup", "collector"},
		{"active", "subject"},
		{"active", "backing"},
	} {
		t.Run(tc.phase+"/"+tc.damage, func(t *testing.T) {
			s, base, p, tpl := warmFallbackFixture(t, true)
			hidden := &templateQuotaEngine{
				Client:   &recoveryTestEngine{fakeEngine: base},
				attempts: map[string]int{}, hideClone: tc.phase != "active",
			}
			e := &templateQuotaSelectionClient{Client: hidden}
			s.opts.Engine = e
			o, err := s.Submit(
				s.ctx,
				p,
				"typed-destination",
				Command{Action: "claim", TemplateID: tpl.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.Wait(s.ctx, p, o.ID)
			if (tc.phase == "active" && err != nil) ||
				(tc.phase != "active" && !isCode(err, api.Unknown)) {
				t.Fatal("unexpected claim outcome", err)
			}
			if tc.phase == "known" || tc.phase == "cleanup" {
				hidden.mu.Lock()
				hidden.hideClone = false
				hidden.mu.Unlock()
				if tc.phase == "known" {
					templateQuotaSQLFault(
						t,
						s,
						"resource_recovery",
						"INSERT",
						"json_extract(NEW.body,'$.kind')='allocation_cleanup'",
						"abort",
					)
				} else {
					e.mu.Lock()
					e.unknown = true
					e.mu.Unlock()
				}
				if err = s.recoverResources(); err == nil {
					t.Fatal("expected retained known or cleanup destination")
				}
				if tc.phase == "known" {
					if _, err = s.store.db.Exec(
						`DROP TRIGGER handoff_fault`,
					); err != nil {
						t.Fatal(err)
					}
				}
			}
			public, err := s.store.operation(s.ctx, o.ID)
			if err != nil {
				t.Fatal(err)
			}
			r, err := s.loadRecovery(allocationGroupID(o))
			if err != nil {
				t.Fatal(err)
			}
			a := r.Allocations[0]
			states := map[string]string{
				"pending": "unresolved",
				"known":   "result_known",
				"cleanup": "cleanup",
				"active":  "active",
			}
			if r.Group.Selection.Stage != "fallback_reserved" ||
				a.Resolution.State != states[tc.phase] {
				t.Fatal("missing actual destination", encode(r))
			}
			// Reopen each valid phase before corrupting it. Failed unknown pending
			// has already transferred its sandbox subject reference to cleanup.
			s = ledgerRestart(t, s)
			kind, key := "pending_storage", a.Request.Key
			from := quotaRef{Kind: "cleanup", ID: "sandbox:" + r.SandboxID}
			to := "active_use"
			if r.Group.Identity != nil {
				kind, key = "vm_disk", r.Group.Identity.VM
			}
			switch tc.damage {
			case "slot":
				kind, key, to = "sandbox_slot", r.SandboxID, "engine_dependency"
			case "reader":
				kind, key = "snapshot", tpl.SnapshotID
				from = quotaRef{
					Kind: "engine_dependency",
					ID:   "operation:" + o.ID,
				}
				to = "cleanup"
			case "collector":
				from.ID = "recovery:" + a.Resolution.Destination
			case "backing":
				kind, key = "snapshot", tpl.SnapshotID
				from = quotaRef{
					Kind: "active_use",
					ID:   "backing:" + r.Group.Identity.VM,
				}
				to = "cleanup"
			}
			var result sql.Result
			if tc.phase == "active" && tc.damage == "subject" {
				from.Kind = "active_use"
				result, err = s.store.db.Exec(
					`DELETE FROM quota_refs WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?`,
					r.TenantID,
					kind,
					key,
					from.Kind,
					from.ID,
				)
			} else {
				result, err = s.store.db.Exec(
					`UPDATE quota_refs SET holder_kind=? WHERE tenant_id=? AND kind=? AND resource_key=? AND holder_kind=? AND holder_id=?`,
					to,
					r.TenantID,
					kind,
					key,
					from.Kind,
					from.ID,
				)
			}
			if err != nil {
				t.Fatal(err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatal(
					"reference mutation must affect one producer row",
					n,
					err,
				)
			}
			before := templateQuotaState(t, s)
			n, finds := e.counts()
			if err = s.recoverResources(); !isCode(err, api.ManualRecovery) {
				t.Fatal("wrong reference kind accepted at runtime", err)
			}
			if !bytes.Equal(before, templateQuotaState(t, s)) {
				t.Fatal("runtime changed incomplete destination")
			}
			now, err := s.store.operation(s.ctx, o.ID)
			if err != nil || !bytes.Equal(encode(public), encode(now)) {
				t.Fatal("public history changed", err)
			}
			ledgerMoney(t, s, p.TenantID, 1, 256)
			opts := s.opts
			opts.Initialize = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			next, openErr := Open(opts)
			if next != nil {
				next.Close()
			}
			if !isCode(openErr, api.ManualRecovery) ||
				!bytes.Equal(before, templateQuotaReadOnlyState(t, opts)) {
				t.Fatal(
					"startup accepted or repaired incomplete destination",
					openErr,
				)
			}
			if nn, ff := e.counts(); nn != n || ff != finds {
				t.Fatal("incomplete destination reached Engine", nn-n, ff-finds)
			}
			hidden.mu.Lock()
			proof := hidden.capture
			hidden.mu.Unlock()
			vm, instance := "", ""
			if r.Group.Identity != nil {
				vm, instance = r.Group.Identity.VM, r.Group.Identity.Instance
			} else if proof != nil {
				vm, instance = proof.VMID, proof.ResultInstance
			}
			v, err := base.GetVM(context.Background(), vm)
			if err != nil || v.Instance != instance {
				t.Fatal("original replacement V/B changed", err)
			}
			t.Log(
				"typed destination rejected at runtime and actual Open; complete state, public bytes, slot1/D64/T192 and actual V/B preserved; zero Submit/Find",
			)
		})
	}
}

func TestTemplateQuotaActiveAndCleanupReferenceLifecycle(t *testing.T) {
	s, base, p, tpl := warmFallbackFixture(t, true)
	claim := ledgerExecute(
		t,
		s,
		p,
		"typed-positive",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	o, err := s.store.byKey(s.ctx, p.TenantID, "typed-positive")
	if err != nil {
		t.Fatal(err)
	}
	original := encode(o)
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	i := r.Group.Identity
	for _, kind := range []string{"active_use", "cleanup"} {
		var count int
		if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='vm_disk' AND resource_key=? AND holder_kind=? AND holder_id=?`, p.TenantID, i.VM, kind, "sandbox:"+claim.Sandbox.ID).
			Scan(&count); err != nil ||
			count != 1 {
			t.Fatal("missing natural coexisting holder", kind, count, err)
		}
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	// Release revokes access and moves the subject holder to cleanup before
	// an unknown delete. Historical active delivery must accept that handoff.
	e := &templateQuotaSelectionClient{
		Client:  &recoveryTestEngine{fakeEngine: base},
		unknown: true,
	}
	s.opts.Engine = e
	release, err := s.Submit(
		s.ctx,
		p,
		"typed-positive-release",
		Command{Action: "release", SandboxID: claim.Sandbox.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, release.ID); !isCode(err, api.Unknown) {
		t.Fatal("expected unknown release", err)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil && !isCode(err, api.Unknown) {
		t.Fatal("legitimate terminated cleanup holder rejected", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	e.mu.Lock()
	e.unknown = false
	e.mu.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	now, err := s.store.operation(s.ctx, o.ID)
	if err != nil || !bytes.Equal(original, encode(now)) {
		t.Fatal("later cleanup changed original successful public bytes", err)
	}
}
