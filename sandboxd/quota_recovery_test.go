package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func ledgerLimit(
	t *testing.T,
	s *Service,
	p Principal,
	slots int,
	storage int64,
) {
	t.Helper()
	if _, err := s.PutTenant(
		s.ctx,
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			Token:         "alice-token",
			MaxSandboxes:  slots,
			MaxStorageMiB: storage,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func ledgerRejectClaim(
	t *testing.T,
	s *Service,
	f *recoveryTestEngine,
	p Principal,
	key string,
) {
	t.Helper()
	before := f.count("create") + f.count("clone")
	ctx, cancel := context.WithTimeout(s.ctx, time.Second)
	defer cancel()
	if _, err := s.Submit(
		ctx,
		p,
		key,
		Command{Action: "claim", TemplateID: "python"},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatalf("wanted Exhausted before engine effects, got %v", err)
	}
	if after := f.count("create") + f.count("clone"); after != before {
		t.Fatal("rejected admission reached engine")
	}
}

func TestQuotaLedgerReleaseKeepsCharge(t *testing.T) {
	for _, paused := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "running", true: "sleeping"}[paused],
			func(t *testing.T) {
				s, f, _, p := recoveryFixture(t)
				ledgerLimit(t, s, p, 1, 1<<20)
				b := *claimTest(t, s, p, "source", nil).Sandbox
				if paused {
					if _, err := s.Execute(
						s.ctx,
						p,
						"pause",
						Command{Action: "pause", SandboxID: b.ID},
					); err != nil {
						t.Fatal(err)
					}
				}
				entered, gate := make(chan struct{}, 1), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(gate) }) }
				defer unblock()
				f.control.Lock()
				if paused {
					f.snapshotDeleteEntered = entered
					f.snapshotDeleteGate = gate
					f.failSnapshotDeletes = 1
				} else {
					f.deleteEntered = entered
					f.deleteGate = gate
					f.failDeletes = 1
				}
				f.control.Unlock()
				o, err := s.Submit(
					s.ctx,
					p,
					"release",
					Command{Action: "release", SandboxID: b.ID},
				)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					unblock()
					current, _ := s.store.operation(s.ctx, o.ID)
					t.Fatalf(
						"delete did not enter: %+v error=%v",
						current,
						current.Error,
					)
				}
				if !paused {
					ledgerRejectClaim(t, s, f, p, "while-delete-blocked")
				}
				unblock()
				if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
					t.Fatal("injected delete failure lost")
				}
				ledgerRejectClaim(t, s, f, p, "after-delete-failure")
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
				// Serialize with startup maintenance before observing its recovery result.
				reopened.cancel()
				reopened.wg.Wait()
				reopened.ctx, reopened.cancel = context.WithCancel(
					context.Background(),
				)
				if err = reopened.recoverResources(); err != nil {
					t.Fatal(err)
				}
				claimTest(t, reopened, p, "after-confirmed-reclamation", nil)
			},
		)
	}
}

func TestQuotaLedgerLostDeleteResponse(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	ledgerLimit(t, s, p, 1, 1<<20)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	f.control.Lock()
	f.dropDeleteResponses = 1
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	); err == nil {
		t.Fatal("lost response accepted")
	}
	ledgerRejectClaim(t, s, f, p, "unknown-stays-charged")
	f.mu.Lock()
	f.unavailable = true
	f.mu.Unlock()
	if err := s.recoverResources(); err == nil {
		t.Fatal("query error hidden")
	}
	ledgerRejectClaim(t, s, f, p, "query-error-stays-charged")
	f.mu.Lock()
	f.unavailable = false
	for key, o := range f.ops {
		if o.Request.Action == "delete" && o.VMID == b.VMID {
			o.Status = "succeeded"
			f.ops[key] = o
			delete(f.vms, b.VMID)
		}
	}
	f.mu.Unlock()
	before := f.count("delete")
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete") != before {
		t.Fatal("absence caused second physical deletion")
	}
	claimTest(t, s, p, "after-absence", nil)
}

func ledgerMoney(
	t *testing.T,
	s *Service,
	tenant string,
	slots, storage int64,
) {
	t.Helper()
	var actualSlots, actualStorage int64
	if err := s.store.db.QueryRowContext(s.ctx, `SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?`, tenant).
		Scan(&actualSlots, &actualStorage); err != nil {
		t.Fatal(err)
	}
	if actualSlots != slots || actualStorage != storage {
		t.Fatalf(
			"%s responsibility: got slots=%d storage=%d; want slots=%d storage=%d",
			tenant,
			actualSlots,
			actualStorage,
			slots,
			storage,
		)
	}
}

func ledgerExecute(
	t *testing.T,
	s *Service,
	p Principal,
	key string,
	c Command,
) Result {
	t.Helper()
	r, err := s.Execute(s.ctx, p, key, c)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func ledgerRestart(t *testing.T, s *Service) *Service {
	t.Helper()
	opts := s.opts
	opts.Initialize = false
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.Close() })
	next.cancel()
	next.wg.Wait()
	next.ctx, next.cancel = context.WithCancel(context.Background())
	return next
}

func TestQuotaLedgerPauseResumeCheckpointSharing(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	for cycle := 0; cycle < 3; cycle++ {
		b := *claimTest(t, s, p, fmt.Sprintf("claim-%d", cycle), nil).Sandbox
		ledgerMoney(t, s, p.TenantID, 1, 64)
		key := fmt.Sprintf("cycle-%d-", cycle)
		ledgerExecute(
			t,
			s,
			p,
			key+"pause",
			Command{Action: "pause", SandboxID: b.ID},
		)
		ledgerMoney(t, s, p.TenantID, 1, 192)
		cp1 := ledgerExecute(
			t,
			s,
			p,
			key+"cp1",
			Command{Action: "checkpoint", SandboxID: b.ID},
		).Checkpoint
		cp2 := ledgerExecute(
			t,
			s,
			p,
			key+"cp2",
			Command{Action: "checkpoint", SandboxID: b.ID},
		).Checkpoint
		if cp1.SnapshotID != cp2.SnapshotID {
			t.Fatal("sleep checkpoints did not share identity")
		}
		ledgerMoney(t, s, p.TenantID, 1, 192)
		ledgerExecute(
			t,
			s,
			p,
			key+"resume",
			Command{Action: "resume", SandboxID: b.ID},
		)
		ledgerMoney(t, s, p.TenantID, 1, 256)
		for _, cp := range []*Checkpoint{cp1, cp2} {
			ledgerExecute(
				t,
				s,
				p,
				key+cp.ID,
				Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
			)
		}
		// Catalog deletion cannot retire the hardlink memory backing of the resumed VM.
		ledgerMoney(t, s, p.TenantID, 1, 256)
		ledgerExecute(
			t,
			s,
			p,
			key+"release",
			Command{Action: "release", SandboxID: b.ID},
		)
		if err := s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 0)
	}
	if f.count("clone") != 3 {
		t.Fatal("sharing caused extra VM effects")
	}
}

func TestQuotaLedgerAdmissionIsAtomic(t *testing.T) {
	t.Run("claims", TestConcurrentQuotaAdmission)
	t.Run("forks", func(t *testing.T) {
		s, f, _, p := recoveryFixture(t)
		b := *claimTest(t, s, p, "source", nil).Sandbox
		ledgerLimit(t, s, p, 3, 1<<20)
		start := make(chan struct{})
		results := make(chan error, 8)
		var workers sync.WaitGroup
		for n := 0; n < 8; n++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				_, err := s.Execute(
					s.ctx,
					p,
					fmt.Sprintf("fork-%d", n),
					Command{Action: "fork", SandboxID: b.ID, Count: 1},
				)
				results <- err
			}()
		}
		close(start)
		workers.Wait()
		close(results)
		accepted := 0
		for err := range results {
			if err == nil {
				accepted++
			} else if !isCode(err, api.Exhausted) {
				t.Fatal(err)
			}
		}
		if accepted != 2 || f.count("clone") != 2 {
			t.Fatalf(
				"admitted forks=%d clone effects=%d",
				accepted,
				f.count("clone"),
			)
		}
		ledgerMoney(t, s, p.TenantID, 3, 3*64+2*192)
	})
}

func TestQuotaLedgerBackupAndRestart(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	a.mu.Lock()
	a.fail = true
	a.mu.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"configure-failure",
		Command{Action: "configure", SandboxID: b.ID, Env: map[string]EnvVar{}},
	); err == nil {
		t.Fatal("configure fault lost")
	}
	a.mu.Lock()
	a.fail = false
	a.mu.Unlock()
	f.control.Lock()
	f.failDeletes = 100
	f.control.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	); err == nil {
		t.Fatal("delete fault lost")
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(s.ctx, destination); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := RestoreStateBackup(restored, destination); err != nil {
		t.Fatal(err)
	}
	opts := s.opts
	opts.StateDir = restored
	opts.Initialize = false
	before := f.count("delete")
	if _, err := Open(opts); !isCode(err, api.ManualRecovery) {
		t.Fatal("backup recovery fence missing", err)
	}
	if f.count("delete") != before {
		t.Fatal("fenced restore reached engine")
	}
	if err := AcknowledgeStateRecovery(
		restored,
		"Reviewed test inventory; retained failed cleanup responsibility",
	); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { next.Close() })
	next.cancel()
	next.wg.Wait()
	next.ctx, next.cancel = context.WithCancel(context.Background())
	ledgerMoney(t, next, p.TenantID, 1, 64)
	if fence, err := configFence(
		next.ctx,
		next.store.db,
		b.ID,
	); err != nil ||
		fence == "" {
		t.Fatal("configuration fence lost", err)
	}
	ledgerLimit(t, next, p, 1, 1<<20)
	ledgerRejectClaim(t, next, f, p, "restored-quota")
	f.control.Lock()
	f.failDeletes = 0
	f.control.Unlock()
	if err = next.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, next, p.TenantID, 0, 0)
	old, err := next.store.byKey(next.ctx, p.TenantID, "release")
	if err != nil || old.Status != "failed" {
		t.Fatal("original public result rewritten", err)
	}
}

func TestQuotaLedgerRootUsesResourceOwner(t *testing.T) {
	for _, requestTenant := range []string{"root", "bob"} {
		t.Run(requestTenant, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			ledgerLimit(t, s, p, 1, 64)
			root := Principal{Root: true, TenantID: requestTenant}
			entered, gate := make(chan struct{}, 1), make(chan struct{})
			f.control.Lock()
			f.snapshotEntered = entered
			f.snapshotGate = gate
			f.control.Unlock()
			var once sync.Once
			unblock := func() { once.Do(func() { close(gate) }) }
			defer unblock()
			o, err := s.Submit(
				s.ctx,
				root,
				"root-pause",
				Command{Action: "pause", SandboxID: b.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not enter")
			}
			ledgerMoney(t, s, p.TenantID, 1, 256)
			ledgerMoney(t, s, requestTenant, 0, 0)
			persisted, err := s.store.operation(s.ctx, o.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persisted.TenantID != requestTenant ||
				persisted.ChargeTenantID != p.TenantID {
				t.Fatal("request identity and resource owner conflated")
			}
			unblock()
			if _, err = s.Wait(s.ctx, root, o.ID); err != nil {
				t.Fatal(err)
			}
			ledgerMoney(t, s, p.TenantID, 1, 192)
			beforeAuto := f.count("clone")
			if _, err = s.DataReady(
				s.ctx,
				root,
				b.ID,
			); !isCode(
				err,
				api.Exhausted,
			) {
				t.Fatal("automatic owner admission inherited root bypass", err)
			}
			if f.count("clone") != beforeAuto {
				t.Fatal("automatic admission rejected after engine allocation")
			}
			ledgerExecute(
				t,
				s,
				root,
				"root-checkpoint",
				Command{Action: "checkpoint", SandboxID: b.ID},
			)
			ledgerExecute(
				t,
				s,
				root,
				"root-resume",
				Command{Action: "resume", SandboxID: b.ID},
			)
			ledgerMoney(t, s, p.TenantID, 1, 256)
			f.control.Lock()
			f.failDeletes = 100
			f.control.Unlock()
			if _, err = s.Execute(
				s.ctx,
				root,
				"root-release",
				Command{Action: "release", SandboxID: b.ID},
			); err == nil {
				t.Fatal("delete failure lost")
			}
			next := ledgerRestart(t, s)
			ledgerMoney(t, next, p.TenantID, 1, 256)
			ledgerMoney(t, next, requestTenant, 0, 0)
			ledgerRejectClaim(t, next, f, p, "owner-still-exhausted")
		})
	}
}

func TestQuotaLedgerSlotRetirementBoundaries(t *testing.T) {
	for _, mode := range []string{"vm-delete-failed", "exclusive-sleep-failed", "shared-checkpoint"} {
		t.Run(mode, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			ledgerLimit(t, s, p, 1, 1<<20)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			if mode != "vm-delete-failed" {
				ledgerExecute(
					t,
					s,
					p,
					"pause",
					Command{Action: "pause", SandboxID: b.ID},
				)
			}
			if mode == "shared-checkpoint" {
				ledgerExecute(
					t,
					s,
					p,
					"cp",
					Command{Action: "checkpoint", SandboxID: b.ID},
				)
			}
			f.control.Lock()
			switch mode {
			case "vm-delete-failed":
				f.failDeletes = 1
			case "exclusive-sleep-failed":
				f.failSnapshotDeletes = 1
			}
			f.control.Unlock()
			_, err := s.Execute(
				s.ctx,
				p,
				"release",
				Command{Action: "release", SandboxID: b.ID},
			)
			if mode == "shared-checkpoint" {
				if err != nil {
					t.Fatal(err)
				}
				ledgerMoney(t, s, p.TenantID, 0, 192)
				claimTest(t, s, p, "shared-takes-over", nil)
				ledgerMoney(t, s, p.TenantID, 1, 256)
			} else {
				if err == nil {
					t.Fatal("delete failure lost")
				}
				storage := int64(64)
				if mode == "exclusive-sleep-failed" {
					storage = 192
				}
				ledgerMoney(t, s, p.TenantID, 1, storage)
				ledgerRejectClaim(t, s, f, p, "slot-remains-owned")
			}
		})
	}
}

func TestQuotaLedgerSnapshotChargeExactBoundaries(t *testing.T) {
	t.Run("claim-64-63", func(t *testing.T) {
		s, f, _, p := recoveryFixture(t)
		ledgerLimit(t, s, p, 1, 63)
		ledgerRejectClaim(t, s, f, p, "lower")
		ledgerLimit(t, s, p, 1, 64)
		claimTest(t, s, p, "exact", nil)
		ledgerMoney(t, s, p.TenantID, 1, 64)
	})
	t.Run("shared-pause-448-447", func(t *testing.T) {
		s, f, _, p := recoveryFixture(t)
		b := *claimTest(t, s, p, "source", nil).Sandbox
		ledgerExecute(
			t,
			s,
			p,
			"pause",
			Command{Action: "pause", SandboxID: b.ID},
		)
		ledgerExecute(
			t,
			s,
			p,
			"checkpoint",
			Command{Action: "checkpoint", SandboxID: b.ID},
		)
		ledgerExecute(
			t,
			s,
			p,
			"resume",
			Command{Action: "resume", SandboxID: b.ID},
		)
		ledgerMoney(t, s, p.TenantID, 1, 256)
		ledgerLimit(t, s, p, 10, 447)
		before := f.count("snapshot") + f.count("network") + f.count("delete")
		if _, err := s.Execute(
			s.ctx,
			p,
			"lower",
			Command{Action: "pause", SandboxID: b.ID},
		); !isCode(
			err,
			api.Exhausted,
		) {
			t.Fatal(err)
		}
		if f.count("snapshot")+f.count("network")+f.count("delete") != before {
			t.Fatal("lower boundary reached engine")
		}
		ledgerLimit(t, s, p, 10, 448)
		ledgerExecute(
			t,
			s,
			p,
			"exact",
			Command{Action: "pause", SandboxID: b.ID},
		)
		ledgerMoney(t, s, p.TenantID, 1, 384)
	})
	for _, action := range []string{"checkpoint", "fork", "resume"} {
		t.Run(action, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			boundary := int64(256)
			if action == "fork" {
				boundary = 320
			}
			if action == "resume" {
				ledgerExecute(
					t,
					s,
					p,
					"pause",
					Command{Action: "pause", SandboxID: b.ID},
				)
			}
			ledgerLimit(t, s, p, 10, boundary-1)
			before := f.count(
				"snapshot",
			) + f.count(
				"clone",
			) + f.count(
				"network",
			)
			c := Command{Action: action, SandboxID: b.ID, Count: 1}
			if _, err := s.Execute(
				s.ctx,
				p,
				"lower",
				c,
			); !isCode(
				err,
				api.Exhausted,
			) {
				t.Fatal(err)
			}
			if f.count(
				"snapshot",
			)+f.count(
				"clone",
			)+f.count(
				"network",
			) != before {
				t.Fatal("rejection reached engine")
			}
			ledgerLimit(t, s, p, 10, boundary)
			ledgerExecute(t, s, p, "exact", c)
			slots := int64(1)
			if action == "fork" {
				slots = 2
			}
			ledgerMoney(t, s, p.TenantID, slots, boundary)
		})
	}
}

func TestQuotaLedgerUnknownInstanceKeepsCharge(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	ledgerLimit(t, s, p, 1, 1<<20)
	f.mu.Lock()
	vm := f.vms[b.VMID]
	vm.Instance = "foreign-instance"
	f.vms[b.VMID] = vm
	f.mu.Unlock()
	if _, err := s.Execute(
		s.ctx,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatal("unproven instance accepted", err)
	}
	if err := s.recoverResources(); err == nil {
		t.Fatal("unknown instance cleared responsibility")
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	ledgerRejectClaim(t, s, f, p, "foreign-stays-owned")
}

func TestQuotaLedgerSchemaBoundaryAndCorruption(t *testing.T) {
	for _, version := range []int{4, storeSchemaVersion + 1} {
		t.Run(fmt.Sprintf("schema-%d", version), func(t *testing.T) {
			dir := t.TempDir()
			st, err := openStore(dir, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.db.Exec(
				`UPDATE version SET version=?; DROP TABLE quota_refs; DROP TABLE quota_resources`,
				version,
			); err != nil {
				t.Fatal(err)
			}
			if err = st.close(); err != nil {
				t.Fatal(err)
			}
			f := newFakeEngine()
			if _, err = Open(
				Options{
					StateDir:  dir,
					RootToken: "root-secret",
					Engine:    f,
					Agent:     &fakeAgent{},
				},
			); !isCode(
				err,
				api.Incompatible,
			) {
				t.Fatal("incompatible schema accepted", err)
			}
			db, err := sql.Open("sqlite", filepath.Join(dir, "sandboxd.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var tables, actual int
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('quota_resources','quota_refs')`).
				Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`SELECT version FROM version`).
				Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if tables != 0 || actual != version || f.count("create") != 0 {
				t.Fatal("rejected schema was changed or reached engine")
			}
		})
	}
	for _, damage := range []string{"negative", "kind", "dangling-ref"} {
		t.Run(damage, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			switch damage {
			case "negative":
				_, err := s.store.db.Exec(
					`PRAGMA ignore_check_constraints=ON; UPDATE quota_resources SET storage_mib=-1 WHERE kind='vm_disk'; PRAGMA ignore_check_constraints=OFF`,
				)
				if err != nil {
					t.Fatal(err)
				}
			case "kind":
				_, err := s.store.db.Exec(
					`PRAGMA foreign_keys=OFF; PRAGMA ignore_check_constraints=ON; UPDATE quota_resources SET kind='future'; PRAGMA ignore_check_constraints=OFF; PRAGMA foreign_keys=ON`,
				)
				if err != nil {
					t.Fatal(err)
				}
			case "dangling-ref":
				_, err := s.store.db.Exec(
					`PRAGMA foreign_keys=OFF; DELETE FROM quota_resources WHERE kind='vm_disk'; PRAGMA foreign_keys=ON`,
				)
				if err != nil {
					t.Fatal(err)
				}
			}
			opts := s.opts
			opts.Initialize = false
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			before := f.count("delete")
			if _, err := Open(opts); !isCode(err, api.ManualRecovery) {
				t.Fatal("inconsistent ledger accepted", damage, err)
			}
			if f.count("delete") != before {
				t.Fatal("corrupt ledger reached engine")
			}
			if _, err := f.GetVM(context.Background(), b.VMID); err != nil {
				t.Fatal("resource removed on corrupt open", err)
			}
		})
	}
}

type quotaUnknownEngine struct {
	*recoveryTestEngine

	muReceipt sync.Mutex
	hide      bool
	dropped   bool
}

func (f *quotaUnknownEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	result, err := f.recoveryTestEngine.Submit(ctx, r)
	f.muReceipt.Lock()
	defer f.muReceipt.Unlock()
	if r.Action == "create" && !f.dropped && err == nil {
		f.dropped = true
		return api.Operation{}, api.Fail(
			api.Unknown,
			"allocation receipt unavailable",
		)
	}
	return result, err
}

func (f *quotaUnknownEngine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	f.muReceipt.Lock()
	hide := f.hide
	f.muReceipt.Unlock()
	if hide {
		return api.Operation{}, api.Fail(api.NotFound, "receipt not visible")
	}
	return f.recoveryTestEngine.FindOperation(ctx, key)
}

func TestQuotaLedgerUnknownAllocationKeepsReservation(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	ledgerLimit(t, s, p, 1, 64)
	engine := &quotaUnknownEngine{recoveryTestEngine: f, hide: true}
	s.opts.Engine = engine
	result, err := s.Execute(
		s.ctx,
		p,
		"lost-create",
		Command{Action: "claim", TemplateID: "python"},
	)
	if !isCode(err, api.Unknown) {
		t.Fatal("original failure changed", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	ledgerRejectClaim(t, s, f, p, "unknown-allocation")
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	engine.muReceipt.Lock()
	engine.hide = false
	engine.muReceipt.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	old, err := s.store.operation(s.ctx, result.OperationID)
	if err != nil || old.Status != "failed" || old.Error.Code != api.Unknown {
		t.Fatal("receipt recovery rewrote failed public result", err)
	}
	if f.count("create") != 1 || f.count("delete") != 1 {
		t.Fatal("allocation recovery duplicated physical effects")
	}
}

func TestQuotaLedgerRollbackRetainsIndependentVersions(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	ledgerMoney(t, s, p.TenantID, 1, 256)
	ledgerLimit(t, s, p, 10, 319)
	before := f.count("stop") + f.count("restore")
	if _, err := s.Execute(
		s.ctx,
		p,
		"lower",
		Command{Action: "rollback", SandboxID: b.ID, CheckpointID: cp.ID},
	); !isCode(
		err,
		api.Exhausted,
	) {
		t.Fatal(err)
	}
	if f.count("stop")+f.count("restore") != before {
		t.Fatal("rollback rejected after engine effect")
	}
	ledgerLimit(t, s, p, 10, 320)
	ledgerExecute(
		t,
		s,
		p,
		"exact",
		Command{Action: "rollback", SandboxID: b.ID, CheckpointID: cp.ID},
	)
	// Original disk D + independent restore disk D + shared snapshot D+M.
	ledgerMoney(t, s, p.TenantID, 1, 320)
	ledgerExecute(
		t,
		s,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	ledgerExecute(
		t,
		s,
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	)
	ledgerMoney(t, s, p.TenantID, 0, 0)
}

func TestQuotaLedgerCleanupCollectors(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	ledgerExecute(t, s, p, "pause", Command{Action: "pause", SandboxID: b.ID})
	cp := ledgerExecute(
		t,
		s,
		p,
		"cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	ledgerExecute(t, s, p, "resume", Command{Action: "resume", SandboxID: b.ID})
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range intents {
		if r.Kind == "retired_snapshot" {
			r.ID += "duplicate"
			r.Attempt = 99
			if err = s.saveRecovery(r); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := f.count("delete_snapshot")
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete_snapshot") != before {
		t.Fatal("cleanup collector deleted an active checkpoint")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	// Release transfers the last sandbox usage to the independent checkpoint.
	ledgerExecute(
		t,
		s,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	f.control.Lock()
	f.failSnapshotDeletes = 1
	f.control.Unlock()
	if _, err = s.Execute(
		s.ctx,
		p,
		"delete-cp-fault",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	); err == nil {
		t.Fatal("delete failure lost")
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	// Deleting the checkpoint removes the active holder. Duplicate cleanup
	// intents converge on the same identity and never duplicate its cost.
	ledgerExecute(
		t,
		s,
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	)
	after := f.count("delete_snapshot")
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if f.count("delete_snapshot") != after {
		t.Fatal("duplicate cleanup repeated physical deletion")
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
}

func TestQuotaLedgerWarmBackingKeepsSharedCharge(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		p,
		"cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	ledgerExecute(
		t,
		s,
		p,
		"golden",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "golden",
				Shape:      b.Shape,
				SnapshotID: cp.SnapshotID,
				TargetWarm: 1,
			},
		},
	)
	s.pools()
	s.wg.Wait()
	ledgerMoney(t, s, p.TenantID, 1, 256)
	ledgerExecute(
		t,
		s,
		p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	)
	ledgerExecute(
		t,
		s,
		p,
		"release",
		Command{Action: "release", SandboxID: b.ID},
	)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	f.mu.Lock()
	f.snaps["new-golden"] = api.Snapshot{
		ID: "new-golden",
		Config: api.Config{
			Shape: b.Shape,
			Image: "sha256:" + strings.Repeat("2", 64),
		},
		Published: true,
	}
	f.mu.Unlock()
	ledgerExecute(
		t,
		s,
		Principal{Root: true, TenantID: p.TenantID},
		"replace-golden",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "golden",
				Shape:      b.Shape,
				SnapshotID: "new-golden",
			},
		},
	)
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	if _, err := f.GetSnapshot(s.ctx, cp.SnapshotID); err != nil {
		t.Fatal("warm source deleted", err)
	}
	s.pools()
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
}

func TestQuotaLedgerMissingResumeVMUsesAbsenceProof(t *testing.T) {
	s, f, a, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	ledgerExecute(t, s, p, "pause", Command{Action: "pause", SandboxID: b.ID})
	a.mu.Lock()
	a.fail = true
	a.mu.Unlock()
	f.control.Lock()
	f.failDeletes = 1
	f.control.Unlock()
	result, err := s.Execute(
		s.ctx,
		p,
		"resume-fault",
		Command{Action: "resume", SandboxID: b.ID},
	)
	if err == nil {
		t.Fatal("resume failure lost")
	}
	failed := recoverySandbox(t, s, b.ID)
	ledgerMoney(t, s, p.TenantID, 1, 256)
	// The owned engine VM disappears before the recovery metadata write.
	f.mu.Lock()
	delete(f.vms, failed.VMID)
	f.mu.Unlock()
	before := f.count("delete")
	if err = s.recoverResources(); err != nil {
		t.Fatal("absence was treated as an unverified zero-value VM", err)
	}
	if f.count("delete") != before {
		t.Fatal("absence caused another physical delete")
	}
	after := recoverySandbox(t, s, b.ID)
	if after.State != "paused" || after.VMID != "" {
		t.Fatal("sleep reference was not restored")
	}
	ledgerMoney(t, s, p.TenantID, 1, 192)
	old, err := s.store.operation(s.ctx, result.OperationID)
	if err != nil || old.Status != "failed" {
		t.Fatal("failed client result changed", err)
	}
}

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

// Delivery faults leave the strict fixture's deterministic engine receipt intact.
// An ordinary restore can fail terminally before allocation so compensation owns
// the single actual restore effect.
type ledgerRecoveryDeliveryEngine struct {
	*recoveryTestEngine

	muDelivery          sync.Mutex
	action              string
	lost                bool
	rejectPublicRestore bool
}

func (f *ledgerRecoveryDeliveryEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.muDelivery.Lock()
	reject := f.rejectPublicRestore && r.Action == "restore" &&
		!strings.HasPrefix(r.Key, "sbx-op:recovery:")
	f.muDelivery.Unlock()
	if reject {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			return old, nil
		}
		o := api.Operation{
			ID:      r.Key,
			Key:     r.Key,
			Request: r,
			Status:  "failed",
			Error: &api.Error{
				Code:    api.Internal,
				Message: "restore failed before allocation",
			},
		}
		f.ops[r.Key] = o
		return o, nil
	}
	o, err := f.recoveryTestEngine.Submit(ctx, r)
	f.muDelivery.Lock()
	defer f.muDelivery.Unlock()
	if r.Action == f.action && !f.lost && err == nil {
		f.lost = true
		return api.Operation{}, api.Fail(
			api.Unknown,
			"completed recovery allocation response lost",
		)
	}
	return o, err
}

func TestQuotaLedgerRecoveryCaptureReconcilesIndependently(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, order := range []string{"recover-first", "release-first"} {
			t.Run(fault+"/"+order, func(t *testing.T) {
				s, f, _, p := recoveryFixture(t)
				ledgerLimit(t, s, p, 1, 1<<20)
				b := *claimTest(t, s, p, "source", nil).Sandbox
				f.control.Lock()
				f.failNetwork = true
				f.control.Unlock()
				actor := p
				if order == "release-first" {
					actor = Principal{Root: true, TenantID: "bob"}
				}
				result, err := s.Execute(
					s.ctx,
					actor,
					"pause",
					Command{Action: "pause", SandboxID: b.ID},
				)
				if err == nil {
					t.Fatal("missing initial pause failure")
				}
				var original []byte
				if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
					Scan(&original); err != nil {
					t.Fatal(err)
				}
				f.control.Lock()
				f.failNetwork = false
				f.control.Unlock()
				if fault == "transport" {
					s.opts.Engine = &ledgerRecoveryDeliveryEngine{
						recoveryTestEngine: f,
						action:             "snapshot",
					}
				} else if _, err = s.store.db.Exec(`CREATE TRIGGER reject_recovery_capture BEFORE INSERT ON quota_resources WHEN NEW.kind='snapshot' BEGIN SELECT RAISE(FAIL,'ledger write failed after capture'); END`); err != nil {
					t.Fatal(err)
				}
				if err = s.recoverResources(); err == nil {
					t.Fatal("recovery fault did not execute")
				}
				if f.count("snapshot") != 1 {
					t.Fatal("capture effect was not exactly one")
				}
				if got := recoverySandbox(
					t,
					s,
					b.ID,
				); got.State != "running" ||
					got.OperationID != "" {
					t.Fatal("capture failure did not clear lifecycle ownership")
				}
				ledgerMoney(t, s, p.TenantID, 1, 256)
				f.control.Lock()
				f.findError = api.Fail(api.Unknown, "receipt query unavailable")
				f.control.Unlock()
				if err = s.recoverResources(); err == nil {
					t.Fatal("receipt uncertainty hidden")
				}
				ledgerMoney(t, s, p.TenantID, 1, 256)
				var pending int
				if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE kind='pending_storage'`).
					Scan(&pending); err != nil ||
					pending != 1 {
					t.Fatal(
						"unknown capture lost its pending responsibility",
						err,
					)
				}
				ledgerRejectClaim(t, s, f, p, "unknown-capture")
				var foreign int
				if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE tenant_id!=?`, p.TenantID).
					Scan(&foreign); err != nil ||
					foreign != 0 {
					t.Fatal(
						"recovery charged request tenant instead of resource owner",
						err,
					)
				}
				if order == "release-first" {
					ledgerExecute(
						t,
						s,
						p,
						"release",
						Command{Action: "release", SandboxID: b.ID},
					)
					ledgerMoney(t, s, p.TenantID, 1, 192)
				}
				if fault == "sqlite" {
					if _, err = s.store.db.Exec(
						`DROP TRIGGER reject_recovery_capture`,
					); err != nil {
						t.Fatal(err)
					}
				}
				// Reopen while the receipt is still unavailable; the durable descriptor
				// must survive independently of public state and its operation pointer.
				s = ledgerRestart(t, s)
				f.control.Lock()
				f.findError = nil
				f.control.Unlock()
				for i := 0; i < 3; i++ {
					if err = s.recoverResources(); err != nil {
						t.Fatal(err)
					}
				}
				if order == "recover-first" {
					ledgerExecute(
						t,
						s,
						p,
						"release",
						Command{Action: "release", SandboxID: b.ID},
					)
				}
				s = ledgerRestart(t, s)
				for i := 0; i < 3; i++ {
					if err = s.recoverResources(); err != nil {
						t.Fatal(err)
					}
				}
				ledgerMoney(t, s, p.TenantID, 0, 0)
				snapshots, err := f.ListSnapshots(s.ctx)
				if err != nil || len(snapshots) != 0 ||
					f.count("delete") != 1 ||
					f.count("snapshot") != 1 ||
					f.count("delete_snapshot") != 1 {
					t.Fatalf(
						"cleanup effects: snapshots=%d capture=%d delete=%d delete_snapshot=%d err=%v",
						len(snapshots),
						f.count("snapshot"),
						f.count("delete"),
						f.count("delete_snapshot"),
						err,
					)
				}
				var after []byte
				if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
					Scan(&after); err != nil ||
					!bytes.Equal(original, after) {
					t.Fatal("terminal public result changed", err)
				}
				ledgerExecute(
					t,
					s,
					p,
					"after-cleanup",
					Command{Action: "claim", TemplateID: "python"},
				)
			})
		}
	}
}

func TestQuotaLedgerRecoveryRestoreReconcilesIndependently(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		t.Run(fault, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			ledgerLimit(t, s, p, 1, 1<<20)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			cp := ledgerExecute(
				t,
				s,
				p,
				"checkpoint",
				Command{Action: "checkpoint", SandboxID: b.ID},
			).Checkpoint
			adapter := &ledgerRecoveryDeliveryEngine{
				recoveryTestEngine:  f,
				rejectPublicRestore: true,
			}
			s.opts.Engine = adapter
			result, err := s.Execute(
				s.ctx,
				p,
				"rollback",
				Command{
					Action:       "rollback",
					SandboxID:    b.ID,
					CheckpointID: cp.ID,
				},
			)
			if err == nil {
				t.Fatal("ordinary restore did not fail")
			}
			var original []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
				Scan(&original); err != nil {
				t.Fatal(err)
			}
			if fault == "transport" {
				adapter.action = "restore"
			} else if _, err = s.store.db.Exec(`CREATE TRIGGER reject_recovery_restore BEFORE INSERT ON quota_resources WHEN NEW.kind='vm_disk' AND NEW.resource_key LIKE '%:restore:%' BEGIN SELECT RAISE(FAIL,'ledger write failed after restore'); END`); err != nil {
				t.Fatal(err)
			}
			if err = s.recoverResources(); err == nil {
				t.Fatal("recovery restore fault did not execute")
			}
			if f.count("restore") != 1 {
				t.Fatal("missing single physical restore")
			}
			ledgerMoney(t, s, p.TenantID, 1, 320)
			f.control.Lock()
			f.findError = api.Fail(api.Unknown, "receipt query unavailable")
			f.control.Unlock()
			if err = s.recoverResources(); err == nil {
				t.Fatal("receipt uncertainty hidden")
			}
			ledgerMoney(t, s, p.TenantID, 1, 320)
			ledgerRejectClaim(t, s, f, p, "unknown-restore")
			if fault == "sqlite" {
				if _, err = s.store.db.Exec(
					`DROP TRIGGER reject_recovery_restore`,
				); err != nil {
					t.Fatal(err)
				}
			}
			s = ledgerRestart(t, s)
			f.control.Lock()
			f.findError = nil
			f.control.Unlock()
			for i := 0; i < 3; i++ {
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
			}
			if f.count("restore") != 1 {
				t.Fatal("receipt caused a second restore")
			}
			ledgerMoney(t, s, p.TenantID, 1, 320)
			// Reclaim the catalog reference first; both independent D versions
			// and all D+M backing remain billed through actual VM deletion.
			ledgerExecute(
				t,
				s,
				p,
				"delete-checkpoint",
				Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
			)
			if _, err = f.GetSnapshot(
				s.ctx,
				cp.SnapshotID,
			); !isCode(
				err,
				api.NotFound,
			) {
				t.Fatal("snapshot catalog was not physically reclaimed", err)
			}
			var disks int
			if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE kind='vm_disk'`).
				Scan(&disks); err != nil ||
				disks != 2 {
				t.Fatal("independent disk version responsibility lost", err)
			}
			ledgerMoney(t, s, p.TenantID, 1, 320)
			ledgerExecute(
				t,
				s,
				p,
				"release",
				Command{Action: "release", SandboxID: b.ID},
			)
			s = ledgerRestart(t, s)
			for i := 0; i < 3; i++ {
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
			}
			ledgerMoney(t, s, p.TenantID, 0, 0)
			var after []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
				Scan(&after); err != nil ||
				!bytes.Equal(original, after) {
				t.Fatal("terminal rollback result changed", err)
			}
			if f.count("restore") != 1 || f.count("delete") != 1 {
				t.Fatal("restore or VM delete replayed")
			}
			ledgerExecute(
				t,
				s,
				p,
				"after-cleanup",
				Command{Action: "claim", TemplateID: "python"},
			)
		})
	}
}
