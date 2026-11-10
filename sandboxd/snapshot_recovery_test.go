package sandboxd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestRetiredSnapshotCleanupHonorsLiveReferences(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	for _, action := range []string{"pause", "checkpoint", "resume"} {
		if _, err := s.Execute(
			s.ctx,
			p,
			action,
			Command{Action: action, SandboxID: b.ID},
		); err != nil {
			t.Fatal(err)
		}
	}
	cps, err := s.Checkpoints(s.ctx, p)
	if err != nil || len(cps) != 1 {
		t.Fatal(cps, err)
	}
	cp := cps[0]
	if _, err = f.GetSnapshot(s.ctx, cp.SnapshotID); err != nil {
		t.Fatal("shared capture deleted", err)
	}
	if _, err = s.Execute(
		s.ctx,
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.GetSnapshot(
		s.ctx,
		cp.SnapshotID,
	); !isEngineCode(
		err,
		api.NotFound,
	) {
		t.Fatal("unreferenced capture retained", err)
	}
	// Historical records require a receipt; arbitrary identifiers are not adopted.
	current := recoverySandbox(t, s, b.ID)
	current.SnapshotID = "unproven"
	if err = saveSandbox(s.ctx, s.store.db, current); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Execute(
		s.ctx,
		p,
		"unsafe-pause",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		ErrManualRecovery,
	) {
		t.Fatal("unproven historical capture accepted", err)
	}
}

func TestRetiredSnapshotCleanupRetriesAfterRestart(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 1
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	var retired resourceRecovery
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			retired = r
		}
	}
	if retired.Attempt != 1 || retired.Done {
		t.Fatal("failed attempt not persisted", retired)
	}
	duplicate := retired
	duplicate.ID += "duplicate"
	duplicate.Attempt = 99
	if err = s.saveRecovery(duplicate); err != nil {
		t.Fatal(err)
	}
	f.control.Lock()
	f.snapshotDeleteUnknown = true
	f.control.Unlock()
	if err = s.recoverResources(); err == nil {
		t.Fatal("unknown deletion hidden")
	}
	intents, err = s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.ID == retired.ID && r.Attempt != 1 {
			t.Fatal("unknown response changed key", r)
		}
	}
	f.control.Lock()
	for _, request := range f.requests {
		if request.Action == "delete_snapshot" &&
			request.SnapshotID == retired.Snapshot &&
			!strings.Contains(request.Key, "recovery:"+retired.ID+":") {
			f.control.Unlock()
			t.Fatal("orphan collector bypassed retired deletion responsibility")
		}
	}
	f.snapshotDeleteUnknown = false
	f.control.Unlock()
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if err = reopened.recoverResources(); err != nil {
		t.Fatal(err)
	}
	intents, err = reopened.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range intents {
		if r.ID == retired.ID && r.Done && r.Attempt == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("retry not completed", intents)
	}
}

func TestRetiredSnapshotCleanupDoesNotOverwriteNewPause(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	old := recoverySandbox(t, s, b.ID).SnapshotID
	f.control.Lock()
	f.failSnapshotDeletes = 1
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	entered := make(chan struct{}, 1)
	captured := make(chan struct{}, 1)
	f.control.Lock()
	f.snapshotDeleteGate = gate
	f.snapshotDeleteEntered = entered
	captureGate := make(chan struct{})
	f.snapshotEntered = captured
	f.snapshotGate = captureGate
	f.control.Unlock()
	paused := make(chan error, 1)
	go func() {
		_, err := s.Execute(
			s.ctx,
			p,
			"pause-new",
			Command{Action: "pause", SandboxID: b.ID},
		)
		paused <- err
	}()
	select {
	case <-captured:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("new capture did not enter")
	}
	cleanup := make(chan error, 1)
	go func() { cleanup <- s.recoverResources() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(captureGate)
		close(gate)
		t.Fatal("cleanup did not enter")
	}
	close(captureGate)
	close(gate)
	if err := <-cleanup; err != nil {
		t.Fatal(err)
	}
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	current := recoverySandbox(t, s, b.ID)
	if current.State != "paused" || current.SnapshotID == "" ||
		current.SnapshotID == old ||
		current.VMID != "" {
		t.Fatal("cleanup overwrote pause", current)
	}
}

func TestRetiredSnapshotCleanupKeepsQuotaUntilReclaimed(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 448,
		},
	); err != nil {
		t.Fatal(err)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	for _, key := range []string{"one", "two"} {
		for _, action := range []string{"pause", "resume"} {
			if _, err := s.Execute(
				s.ctx,
				p,
				key+action,
				Command{Action: action, SandboxID: b.ID},
			); err != nil {
				t.Fatal(err)
			}
		}
	}
	snapshots := f.count("snapshot")
	if _, err := s.Execute(
		s.ctx,
		p,
		"over-quota",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		ErrExhausted,
	) {
		t.Fatal("cleanup charge lost", err)
	}
	if f.count("snapshot") != snapshots ||
		recoverySandbox(t, s, b.ID).State != "running" {
		t.Fatal("quota rejected after side effect")
	}
	f.control.Lock()
	f.failSnapshotDeletes = 0
	f.control.Unlock()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"after-cleanup",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
}

func TestRetiredSnapshotHistoricalRunningCaptureAdmission(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.Execute(
		s.ctx,
		p,
		"pause",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	old := recoverySandbox(t, s, b.ID).SnapshotID
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"resume",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	b = recoverySandbox(t, s, b.ID)
	b.SnapshotID = old
	if err := saveSandbox(s.ctx, s.store.db, b); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 447,
		},
	); err != nil {
		t.Fatal(err)
	}
	count := f.count("snapshot")
	if _, err := s.Execute(
		s.ctx,
		p,
		"unsafe-admission",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		ErrExhausted,
	) {
		t.Fatal("historical capture skipped reservation", err)
	}
	ready, err := s.DataReady(s.ctx, p, b.ID)
	if err != nil || ready.SnapshotID != "" {
		t.Fatal("trusted historical handoff failed", ready, err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"after-handoff",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		ErrExhausted,
	) {
		t.Fatal("handoff released charge", err)
	}
	if f.count("snapshot") != count ||
		recoverySandbox(t, s, b.ID).State != "running" {
		t.Fatal("rejected capture changed running state")
	}
}

func TestRetiredSnapshotQuotaSurvivesRestartAndDeduplicates(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  50,
			MaxStorageMiB: 448,
		},
	); err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"pause", "checkpoint", "resume"} {
		if _, err := s.Execute(
			s.ctx,
			p,
			action,
			Command{Action: action, SandboxID: b.ID},
		); err != nil {
			t.Fatal(err)
		}
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			r.ID += "duplicate"
			r.Attempt = 4
			if err = s.saveRecovery(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	// 64 active VM disk + 192 shared checkpoint/backing snapshot + 192 new
	// pause capture = 448. Cleanup and backing refs do not duplicate the SID.
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"boundary",
		Command{Action: "pause", SandboxID: b.ID},
	); err != nil {
		t.Fatal("shared charge duplicated", err)
	}
	f.control.Lock()
	f.failSnapshotDeletes = 100
	f.control.Unlock()
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"resume-boundary",
		Command{Action: "resume", SandboxID: b.ID},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = reopened.Execute(
		reopened.ctx,
		p,
		"over-boundary",
		Command{Action: "pause", SandboxID: b.ID},
	); !isCode(
		err,
		ErrExhausted,
	) {
		t.Fatal("persistent charge missing", err)
	}
}

func TestSnapshotOwnershipSurvivesReplayAndRestart(t *testing.T) {
	for _, action := range []string{"template_build", "pause", "checkpoint", "fork"} {
		t.Run(action, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			cmd := Command{Action: action, SandboxID: b.ID, Count: 2}
			if action == "template_build" {
				cmd = Command{
					Action: action,
					Template: &Template{
						ID:    "built",
						Image: "sha256:" + strings.Repeat("1", 64),
						Shape: b.Shape,
					},
				}
			}
			entered := make(chan struct{}, 1)
			gate := make(chan struct{})
			f.control.Lock()
			f.snapshotEntered = entered
			f.snapshotGate = gate
			f.control.Unlock()
			o, err := s.Submit(s.ctx, p, "interrupted", cmd)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not complete")
			}
			s.cancel()
			s.wg.Wait()
			step := map[string]string{"template_build": "build-snapshot", "pause": "hibernate-snapshot", "checkpoint": "checkpoint-capture", "fork": "fork-snapshot"}[action]
			receipt, err := f.FindOperation(
				context.Background(),
				stepKey(o, step),
			)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			if err = s.store.db.QueryRow("SELECT count(*) FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&count); err != nil ||
				count != 0 {
				t.Fatal("published before receipt handoff", count, err)
			}
			opts := s.opts
			opts.Initialize = false
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			f.control.Lock()
			f.snapshotEntered = nil
			f.snapshotGate = nil
			f.control.Unlock()
			reopened, err := Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { reopened.Close() })
			if _, err = reopened.Wait(
				context.Background(),
				p,
				o.ID,
			); err != nil {
				t.Fatal("replay failed", err)
			}
			var owner, source, kind string
			if err = reopened.store.db.QueryRow("SELECT tenant_id,source_operation_id,source_kind FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&owner, &source, &kind); err != nil ||
				owner != p.TenantID ||
				source != o.ID ||
				kind != "captured" {
				t.Fatal(owner, source, kind, err)
			}
			if action == "pause" {
				reused, err := reopened.Execute(
					reopened.ctx,
					p,
					"paused-checkpoint",
					Command{Action: "checkpoint", SandboxID: b.ID},
				)
				if err != nil ||
					reused.Checkpoint.SnapshotID != receipt.SnapshotID {
					t.Fatal("paused checkpoint did not reuse capture", err)
				}
			}
			if _, err = reopened.Execute(
				reopened.ctx,
				Principal{Root: true, TenantID: "other"},
				"transfer",
				Command{
					Action: "template_put",
					Template: &Template{
						ID:         "foreign",
						Shape:      b.Shape,
						SnapshotID: receipt.SnapshotID,
					},
				},
			); !isCode(
				err,
				ErrNotFound,
			) {
				t.Fatal("replay provenance reassigned", err)
			}
			if err = reopened.store.db.QueryRow("SELECT count(*) FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
				Scan(&count); err != nil ||
				count != 1 {
				t.Fatal("duplicate ownership", count, err)
			}
		})
	}
}

func TestRootCaptureReceiptReplayKeepsSourceTenant(t *testing.T) {
	for _, action := range []string{"pause", "checkpoint"} {
		for _, requestTenant := range []string{"root", "bob"} {
			t.Run(action+"/"+requestTenant, func(t *testing.T) {
				s, f, _, alice := recoveryFixture(t)
				b := *claimTest(t, s, alice, "source", nil).Sandbox
				root := Principal{Root: true, TenantID: requestTenant}
				entered := make(chan struct{}, 1)
				gate := make(chan struct{})
				f.control.Lock()
				f.snapshotEntered = entered
				f.snapshotGate = gate
				f.control.Unlock()
				o, err := s.Submit(
					s.ctx,
					root,
					"root-capture",
					Command{Action: action, SandboxID: b.ID},
				)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("capture did not enter")
				}
				s.cancel()
				s.wg.Wait()
				step := "checkpoint-capture"
				if action == "pause" {
					step = "hibernate-snapshot"
				}
				receipt, err := f.FindOperation(
					context.Background(),
					stepKey(o, step),
				)
				if err != nil {
					t.Fatal(err)
				}
				if action == "pause" {
					// Terminal client failure is compensated from its original capture receipt.
					original, err := s.store.operation(
						context.Background(),
						o.ID,
					)
					if err != nil {
						t.Fatal(err)
					}
					original.Status = "failed"
					original.Error = &Error{
						Code:    ErrUnknown,
						Message: "capture response lost",
					}
					if err = saveOp(
						context.Background(),
						s.store.db,
						original,
					); err != nil {
						t.Fatal(err)
					}
				}
				opts := s.opts
				opts.Initialize = false
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				f.control.Lock()
				f.snapshotEntered = nil
				f.snapshotGate = nil
				f.control.Unlock()
				reopened, err := Open(opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { reopened.Close() })
				if action == "pause" {
					if err = reopened.recoverResources(); err != nil {
						t.Fatal(err)
					}
				} else {
					if _, err = reopened.Wait(
						context.Background(),
						root,
						o.ID,
					); err != nil {
						t.Fatal(err)
					}
				}
				var owner string
				if err = reopened.store.db.QueryRow("SELECT tenant_id FROM snapshot_owners WHERE snapshot_id=?", receipt.SnapshotID).
					Scan(&owner); err != nil ||
					owner != alice.TenantID {
					t.Fatalf(
						"replayed root capture owner=%s err=%v",
						owner,
						err,
					)
				}
				if _, err = reopened.Execute(
					reopened.ctx,
					Principal{TenantID: "bob"},
					"foreign-import",
					Command{
						Action: "template_put",
						Template: &Template{
							ID:         "foreign",
							Shape:      b.Shape,
							SnapshotID: receipt.SnapshotID,
						},
					},
				); !isCode(
					err,
					ErrNotFound,
				) {
					t.Fatal("Bob imported replayed Alice capture", err)
				}
				if action == "pause" {
					if _, err = reopened.Execute(
						reopened.ctx,
						alice,
						"alice-resume",
						Command{Action: "resume", SandboxID: b.ID},
					); err != nil {
						t.Fatal("Alice resume after root receipt recovery", err)
					}
				}
			})
		}
	}
}

func TestRootSnapshotGrantSurvivesRestart(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	f.mu.Lock()
	f.snaps["external"] = api.Snapshot{
		ID:        "external",
		Config:    api.Config{Shape: shape},
		Published: true,
	}
	f.mu.Unlock()
	cmd := Command{
		Action:   "template_put",
		Template: &Template{ID: "golden", Shape: shape, SnapshotID: "external"},
	}
	if _, err := s.Submit(
		s.ctx,
		p,
		"tenant-import",
		cmd,
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatal("tenant created grant", err)
	}
	unlock := s.lock("template:golden")
	o, err := s.Submit(
		s.ctx,
		Principal{Root: true, TenantID: p.TenantID},
		"root-import",
		cmd,
	)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	s.cancel()
	unlock()
	s.wg.Wait()
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	if _, err = reopened.Wait(context.Background(), p, o.ID); err != nil {
		t.Fatal(err)
	}
	result, err := reopened.Execute(
		reopened.ctx,
		p,
		"clone",
		Command{
			Action:     "claim",
			TemplateID: "golden",
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil || result.Sandbox == nil {
		t.Fatal("durable grant lost", err)
	}
	var kind, source string
	if err = reopened.store.db.QueryRow("SELECT source_kind,source_operation_id FROM snapshot_owners WHERE snapshot_id='external'").
		Scan(&kind, &source); err != nil ||
		kind != "external_import" ||
		source != o.ID {
		t.Fatal(kind, source, err)
	}
}
