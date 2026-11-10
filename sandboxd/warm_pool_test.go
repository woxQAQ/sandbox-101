package sandboxd

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestRefillBackoffSurvivesRestartAndSuccessResets(t *testing.T) {
	s, _, _, _ := fixture(t)
	now := time.Now().UTC()
	for n := 1; n <= 10; n++ {
		o := Operation{
			ID:         fmt.Sprint("refill-", n),
			TenantID:   "alice",
			Key:        fmt.Sprint("refill-", n),
			hash:       "test",
			Status:     "failed",
			Command:    Command{Action: "refill", TemplateID: "python"},
			CreatedAt:  now.Add(-time.Hour),
			FinishedAt: now,
		}
		if err := saveOp(context.Background(), s.store.db, o); err != nil {
			t.Fatal(err)
		}
		delay := time.Second << min(n-1, 8)
		if blocked, err := s.refillBackoff(
			"python",
			now.Add(delay-time.Nanosecond),
		); err != nil ||
			!blocked {
			t.Fatalf("failure %d lost its backoff: %v", n, err)
		}
		if blocked, err := s.refillBackoff(
			"python",
			now.Add(delay),
		); err != nil ||
			blocked {
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
	if blocked, err := restarted.refillBackoff(
		"python",
		now.Add(time.Second),
	); err != nil ||
		!blocked {
		t.Fatalf("restart reset the failure streak: %v", err)
	}
	o := Operation{
		ID:         "refill-success",
		TenantID:   "alice",
		Key:        "refill-success",
		hash:       "test",
		Status:     "succeeded",
		Command:    Command{Action: "refill", TemplateID: "python"},
		CreatedAt:  now,
		FinishedAt: now,
	}
	if err = saveOp(context.Background(), restarted.store.db, o); err != nil {
		t.Fatal(err)
	}
	if blocked, err := restarted.refillBackoff(
		"python",
		now,
	); err != nil ||
		blocked {
		t.Fatalf("successful refill did not reset the streak: %v", err)
	}
}

func templateQuotaWarmMatrix(t *testing.T) {
	for _, storage := range []int64{255, 256, 320} {
		t.Run(fmt.Sprintf("budget-%d", storage), func(t *testing.T) {
			s, f, p, template := templateQuotaGolden(t)
			ledgerLimit(t, s, p, 2, storage)
			template = templateQuotaTarget(t, s, p, template, 1)
			s.pools()
			s.wg.Wait()
			var ready int
			if err := s.store.db.QueryRow(`SELECT count(*) FROM warm`).
				Scan(&ready); err != nil {
				t.Fatal(err)
			}
			if storage == 255 {
				if ready != 0 || f.count("clone") != 0 {
					t.Fatal("low owner refill reached Engine")
				}
				ledgerMoney(t, s, p.TenantID, 0, 192)
				before, _ := s.resourceOperations()
				s.pools()
				after, _ := s.resourceOperations()
				if len(before) != len(after) {
					t.Fatal("refill backoff generated operation spam")
				}
				return
			}
			if ready != 1 {
				t.Fatal("exact owner refill missing")
			}
			ledgerMoney(t, s, p.TenantID, 0, 256)
			var warm, source string
			if err := s.store.db.QueryRow(`SELECT vm_id FROM warm`).
				Scan(&warm); err != nil {
				t.Fatal(err)
			}
			if err := s.store.db.QueryRow(`SELECT source_operation_id FROM quota_resources WHERE kind='vm_disk' AND resource_key=?`, warm).
				Scan(&source); err != nil {
				t.Fatal(err)
			}
			first := ledgerExecute(
				t,
				s,
				p,
				"warm-claim",
				Command{Action: "claim", TemplateID: template.ID},
			)
			if first.Sandbox.VMID != warm ||
				first.Sandbox.ProvisionSource != "warm" {
				t.Fatal("claim failed to transfer existing disk")
			}
			var pending int
			if err := s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE kind='pending_storage' AND source_operation_id=?`, first.OperationID).
				Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if pending != 0 {
				t.Fatal("warm claim reserved another D")
			}
			ledgerMoney(t, s, p.TenantID, 1, 256)
			var currentSource string
			_ = s.store.db.QueryRow(`SELECT source_operation_id FROM quota_resources WHERE kind='vm_disk' AND resource_key=?`, warm).
				Scan(&currentSource)
			if source != currentSource || source == first.OperationID {
				t.Fatal("warm source provenance was overwritten")
			}
			before := f.count("clone")
			second, err := s.Submit(
				s.ctx,
				p,
				"second-claim",
				Command{Action: "claim", TemplateID: template.ID},
			)
			if storage == 256 {
				if !isCode(err, ErrExhausted) || f.count("clone") != before {
					t.Fatal(
						"second claim reused one V or allocated over budget",
						err,
					)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				result, e := s.Wait(s.ctx, p, second.ID)
				if e != nil {
					t.Fatal(e)
				}
				if result.Sandbox.VMID == warm {
					t.Fatal("two claims reused one warm VM")
				}
				ledgerMoney(t, s, p.TenantID, 2, 320)
			}
		})
	}
	t.Run("missing-owner-row", func(t *testing.T) {
		s, f, p, template := templateQuotaGolden(t)
		templateQuotaTarget(t, s, p, template, 1)
		if _, err := s.store.db.Exec(
			`DELETE FROM tenants WHERE id=?`,
			p.TenantID,
		); err != nil {
			t.Fatal(err)
		}
		s.pools()
		s.wg.Wait()
		if f.count("clone") != 0 {
			t.Fatal("maintenance bootstrapped missing owner")
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
	})
	for _, direction := range [][2]int{{0, 1}, {1, 0}, {1, 2}} {
		t.Run(
			fmt.Sprintf("target-%d-%d", direction[0], direction[1]),
			func(t *testing.T) {
				s, _, p, template := templateQuotaGolden(t)
				if direction[0] != 0 {
					template = templateQuotaTarget(
						t,
						s,
						p,
						template,
						direction[0],
					)
				}
				changed := template
				changed.TargetWarm = direction[1]
				if _, err := s.Submit(
					s.ctx,
					p,
					"ordinary-change",
					Command{Action: "template_put", Template: &changed},
				); !isCode(
					err,
					ErrForbidden,
				) {
					t.Fatal("ordinary pool target mutation allowed", err)
				}
				ledgerExecute(
					t,
					s,
					p,
					"ordinary-preserve",
					Command{Action: "template_put", Template: &template},
				)
				templateQuotaTarget(t, s, p, template, direction[1])
			},
		)
	}
	for _, mode := range []string{"missing-witness", "unknown-clone"} {
		t.Run(mode, func(t *testing.T) {
			s, f, p, template := templateQuotaGolden(t)
			templateQuotaTarget(t, s, p, template, 1)
			e := &templateQuotaEngine{
				Client:              f,
				attempts:            map[string]int{},
				missingCloneWitness: mode == "missing-witness",
				hideClone:           mode == "unknown-clone",
			}
			s.opts.Engine = e
			s.pools()
			s.wg.Wait()
			for range 3 {
				if err := s.recoverResources(); err != nil &&
					!isCode(err, ErrManualRecovery) {
					t.Fatal(err)
				}
				s.pools()
				s.wg.Wait()
			}
			ledgerMoney(t, s, p.TenantID, 0, 256)
			if f.count("clone") != 1 {
				t.Fatal("unresolved refill did not occupy target")
			}
			if mode == "missing-witness" {
				groups, err := s.resourceRecoveries()
				if err != nil {
					t.Fatal(err)
				}
				var group resourceRecovery
				for _, r := range groups {
					if r.Group != nil && r.Group.Subject.Kind == "warm" {
						group = r
					}
				}
				if group.Group == nil || group.Group.Gate == nil || group.Done {
					t.Fatal("witnessless warm has no durable observation gate")
				}
				e.mu.Lock()
				deletes := e.attempts["delete"]
				e.mu.Unlock()
				if deletes != 0 {
					t.Fatal("witnessless clone attempted delete")
				}
				s = ledgerRestart(t, s)
				ledgerMoney(t, s, p.TenantID, 0, 256)
			} else {
				e.mu.Lock()
				e.hideClone = false
				e.mu.Unlock()
				if err := s.recoverResources(); err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 0, 192)
			}
		})
	}
}

func templateQuotaReplacementWitnessless(t *testing.T) {
	s, engine, p, template := warmFallbackFixture(t, false)
	f := &recoveryTestEngine{fakeEngine: engine}
	e := &templateQuotaEngine{
		Client:    f,
		attempts:  map[string]int{},
		failStart: true,
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"cold-replacement",
		Command{Action: "claim", TemplateID: template.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
		t.Fatal("replacement start failure missing")
	}
	for range 2 {
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	if r.Group.Subject.Kind != "sandbox" || r.Group.Subject.ID != o.IDs[0] ||
		r.Group.Gate == nil ||
		r.Group.Identity == nil ||
		r.Group.Identity.Instance != "" {
		t.Fatal("replacement escaped real sandbox responsibility")
	}
	var builds int
	if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE kind='build_slot'`).
		Scan(&builds); err != nil ||
		builds != 0 {
		t.Fatal("replacement manufactured a build subject", err)
	}
	e.mu.Lock()
	deletes := e.attempts["delete"]
	e.mu.Unlock()
	if deletes != 1 {
		t.Fatal("witnessless replacement attempted another delete")
	}
	state := r.Group.Identity.VM
	original, _ := s.store.operation(s.ctx, o.ID)
	if _, err = s.Execute(
		s.ctx,
		p,
		"release-witnessless",
		Command{Action: "release", SandboxID: o.IDs[0]},
	); !isCode(
		err,
		ErrManualRecovery,
	) {
		t.Fatal("release bypassed witnessless replacement", err)
	}
	started, err := api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:    "replacement-external-start",
			Action: "start",
			VMID:   state,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "replacement-external-delete",
			Action:           "delete",
			VMID:             state,
			ExpectedInstance: started.ResultInstance,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	ledgerExecute(
		t,
		s,
		p,
		"release-after-replacement-retirement",
		Command{Action: "release", SandboxID: o.IDs[0]},
	)
	current, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(encode(original), encode(current)) {
		t.Fatal("cleanup changed failed replacement public bytes")
	}
}

func TestTemplateQuotaWarmPoolOwnership(t *testing.T) {
	for _, fault := range []string{"abort", "ignore", "commit"} {
		for _, stage := range []string{"ready", "selection", "fallback", "fallback-destination"} {
			t.Run(
				stage+"-"+fault,
				func(t *testing.T) { templateQuotaHandoffFault(t, stage, fault) },
			)
		}
	}
	t.Run("empty-clone-dependency", func(t *testing.T) {
		s, f, p, template := templateQuotaGolden(t)
		templateQuotaTarget(t, s, p, template, 1)
		e := &templateQuotaEngine{
			Client:         f,
			attempts:       map[string]int{},
			failCloneEmpty: true,
		}
		s.opts.Engine = e
		s.pools()
		s.wg.Wait()
		var refill Operation
		ops, err := s.resourceOperations()
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range ops {
			if o.Command.Action == "refill" {
				refill = o
			}
		}
		if refill.Status != "failed" {
			t.Fatal("empty clone failure missing")
		}
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		var dependencies int
		if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?`, "operation:"+refill.ID).
			Scan(&dependencies); err != nil ||
			dependencies != 0 {
			t.Fatal("terminal no-VM clone stranded its source dependency", err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
		created, err := api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:    "new-source-create",
				Action: "create",
				Config: &api.Config{
					Image: template.Image,
					Shape: template.Shape,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		started, err := api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:    "new-source-start",
				Action: "start",
				VMID:   created.VMID,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		captured, err := api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:              "new-source-capture",
				Action:           "snapshot",
				VMID:             created.VMID,
				ExpectedInstance: started.ResultInstance,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		old := template.SnapshotID
		template.SnapshotID = captured.SnapshotID
		template.TargetWarm = 0
		ledgerExecute(
			t,
			s,
			Principal{Root: true, TenantID: p.TenantID},
			"replace-empty-source",
			Command{Action: "template_put", Template: &template},
		)
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		if _, err = f.GetSnapshot(
			s.ctx,
			old,
		); !isEngineCode(
			err,
			api.NotFound,
		) {
			t.Fatal("obsolete source snapshot remained pinned", err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
		if _, err = api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:              "new-source-delete",
				Action:           "delete",
				VMID:             created.VMID,
				ExpectedInstance: started.ResultInstance,
			},
		); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("selected-warm-external-C", func(t *testing.T) {
		s, f, p, template := templateQuotaGolden(t)
		templateQuotaTarget(t, s, p, template, 1)
		s.pools()
		s.wg.Wait()
		var vm string
		if err := s.store.db.QueryRow(`SELECT vm_id FROM warm`).
			Scan(&vm); err != nil {
			t.Fatal(err)
		}
		original, err := f.GetVM(s.ctx, vm)
		if err != nil {
			t.Fatal(err)
		}
		gate := make(chan struct{})
		var once sync.Once
		release := func() { once.Do(func() { close(gate) }) }
		defer release()
		e := &templateQuotaEngine{
			Client:       f,
			attempts:     map[string]int{},
			queryEntered: make(chan struct{}),
			queryGate:    gate,
		}
		s.opts.Engine = e
		o, err := s.Submit(
			s.ctx,
			p,
			"selected-C",
			Command{Action: "claim", TemplateID: template.ID},
		)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-e.queryEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("selected activation did not query its fixed VM")
		}
		if _, err = api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:              "external-warm-stop",
				Action:           "stop",
				VMID:             vm,
				ExpectedInstance: original.Instance,
			},
		); err != nil {
			t.Fatal(err)
		}
		c, err := api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:    "external-warm-start-C",
				Action: "start",
				VMID:   vm,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if c.ResultInstance == "" || c.ResultInstance == original.Instance {
			t.Fatal("external start did not produce a new immutable C witness")
		}
		release()
		if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, ErrManualRecovery) {
			t.Fatal("changed warm instance was accepted", err)
		}
		current, err := f.GetVM(s.ctx, vm)
		if err != nil || current.Instance != c.ResultInstance {
			t.Fatal("external C changed", err)
		}
		e.mu.Lock()
		deletes, clones, networks := e.attempts["delete"], e.attempts["clone"], e.attempts["network"]
		e.mu.Unlock()
		if deletes != 0 || clones != 0 || networks != 0 {
			t.Fatalf(
				"Service touched C or allocated fallback: delete=%d clone=%d network=%d",
				deletes,
				clones,
				networks,
			)
		}
		ledgerMoney(t, s, p.TenantID, 1, 256)
		if _, err = api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:              "external-warm-delete-C",
				Action:           "delete",
				VMID:             vm,
				ExpectedInstance: c.ResultInstance,
			},
		); err != nil {
			t.Fatal(err)
		}
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 192)
	})
	t.Run("fallback-paused-zero-wake-release", func(t *testing.T) {
		for _, golden := range []bool{false, true} {
			t.Run(fmt.Sprint(golden), func(t *testing.T) {
				s, engine, p, template := warmFallbackFixture(t, golden)
				result := ledgerExecute(
					t,
					s,
					p,
					"fallback-lifecycle",
					Command{Action: "claim", TemplateID: template.ID},
				)
				ledgerExecute(
					t,
					s,
					p,
					"fallback-pause",
					Command{Action: "pause", SandboxID: result.Sandbox.ID},
				)
				before := engine.count(
					"create",
				) + engine.count(
					"clone",
				) + engine.count(
					"start",
				)
				ledgerExecute(
					t,
					s,
					p,
					"fallback-zero-wake",
					Command{Action: "release", SandboxID: result.Sandbox.ID},
				)
				if after := engine.count(
					"create",
				) + engine.count(
					"clone",
				) + engine.count(
					"start",
				); after != before {
					t.Fatal("fallback release woke a paused VM")
				}
				storage := int64(0)
				if golden {
					storage = 192
				}
				ledgerMoney(t, s, p.TenantID, 0, storage)
			})
		}
	})
	t.Run("cold-replacement-witnessless", templateQuotaReplacementWitnessless)
	t.Run("owner-and-selection", templateQuotaWarmMatrix)
	s, _, _, p := recoveryFixture(t)
	c := templateQuotaBuild("ordinary-target")
	c.Action = "template_put"
	c.Template.TargetWarm = 1
	_, err := s.Submit(s.ctx, p, "ordinary-target", c)
	if !isCode(err, ErrForbidden) {
		t.Fatalf("ordinary first nonzero pool target must be refused: %v", err)
	}
}
