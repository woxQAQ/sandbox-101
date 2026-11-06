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

type r3RestoreScenario struct {
	s        *Service
	f        *recoveryTestEngine
	adapter  *ledgerRecoveryDeliveryEngine
	p        Principal
	b        Sandbox
	cp       *Checkpoint
	rollback string
	release  string
	original map[string][]byte
}

func newR3RestoreScenario(
	t *testing.T,
	fault string,
	faithful bool,
) *r3RestoreScenario {
	t.Helper()
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
		faithfulFailure:     faithful,
	}
	s.opts.Engine = adapter
	result, err := s.Execute(
		s.ctx,
		p,
		"rollback",
		Command{Action: "rollback", SandboxID: b.ID, CheckpointID: cp.ID},
	)
	if err == nil {
		t.Fatal("ordinary rollback did not fail")
	}
	x := &r3RestoreScenario{
		s:        s,
		f:        f,
		adapter:  adapter,
		p:        p,
		b:        b,
		cp:       cp,
		rollback: result.OperationID,
		original: map[string][]byte{},
	}
	x.remember(t, result.OperationID)
	if fault == "transport" {
		adapter.action = "restore"
	} else {
		if _, err = s.store.db.Exec(
			`CREATE TRIGGER r3_initial BEFORE INSERT ON quota_resources WHEN NEW.kind='vm_disk' AND NEW.resource_key LIKE '%:restore:%' AND NEW.source_operation_id LIKE 'recovery:%' BEGIN SELECT RAISE(ABORT,'recovery receipt adoption'); END`,
		); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.recoverResources(); err == nil {
		t.Fatal("initial compensation fault missing")
	}
	if f.count("restore") != 1 {
		t.Fatal("restore effect must be one")
	}
	return x
}

func (x *r3RestoreScenario) remember(t *testing.T, id string) {
	t.Helper()
	var body []byte
	if err := x.s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, id).
		Scan(&body); err != nil {
		t.Fatal(err)
	}
	x.original[id] = body
}

func (x *r3RestoreScenario) releaseFirst(t *testing.T) {
	t.Helper()
	result, err := x.s.Execute(
		x.s.ctx,
		Principal{Root: true, TenantID: "bob"},
		"release-before-receipt",
		Command{Action: "release", SandboxID: x.b.ID},
	)
	if !isCode(err, api.Conflict) {
		t.Fatal("expected frozen A pre-admission conflict", err)
	}
	x.release = result.OperationID
	x.remember(t, x.release)
	if _, err = x.s.store.db.Exec(
		`DROP TRIGGER IF EXISTS r3_initial`,
	); err != nil {
		t.Fatal(err)
	}
}

func (x *r3RestoreScenario) assertImmutable(t *testing.T) {
	t.Helper()
	for id, body := range x.original {
		var actual []byte
		if err := x.s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, id).
			Scan(&actual); err != nil ||
			!bytes.Equal(body, actual) {
			t.Fatal("public failure changed", id, err)
		}
	}
	var foreign int
	if err := x.s.store.db.QueryRow(`SELECT count(*) FROM quota_resources WHERE tenant_id!=?`, x.p.TenantID).
		Scan(&foreign); err != nil ||
		foreign != 0 {
		t.Fatal("wrong charge owner", err)
	}
}

func (x *r3RestoreScenario) collector(t *testing.T) resourceRecovery {
	t.Helper()
	all, err := x.s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	var found []resourceRecovery
	for _, r := range all {
		if r.Kind == "instance_cleanup" && r.SandboxID == x.b.ID {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("target collectors=%d", len(found))
	}
	return found[0]
}

type r3CleanupTransport struct {
	api.Client

	mu       sync.Mutex
	boundary string
	blocked  bool
	request  *api.Request
}

func (f *r3CleanupTransport) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.mu.Lock()
	target := r.Action == "delete" &&
		strings.Contains(r.Key, "instance-cleanup:")
	blocked := f.blocked
	if target {
		if f.request != nil && !bytes.Equal(encode(*f.request), encode(r)) {
			f.mu.Unlock()
			return api.Operation{}, api.Fail(
				api.Conflict,
				"unknown cleanup key changed",
			)
		}
		copyR := r
		f.request = &copyR
	}
	boundary := f.boundary
	f.mu.Unlock()
	if target && blocked && boundary == "before-dispatch" {
		return api.Operation{}, api.Fail(api.Unknown, "dispatch unavailable")
	}
	op, err := f.Client.Submit(ctx, r)
	if target && blocked && boundary == "lost-reply" && err == nil {
		return api.Operation{}, api.Fail(
			api.Unknown,
			"successful deletion reply lost",
		)
	}
	return op, err
}

func (f *r3CleanupTransport) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	f.mu.Lock()
	blocked := f.blocked && f.boundary == "lost-reply" &&
		strings.Contains(key, "instance-cleanup:")
	f.mu.Unlock()
	if blocked {
		return api.Operation{}, api.Fail(
			api.Unknown,
			"delete receipt unavailable",
		)
	}
	return f.Client.FindOperation(ctx, key)
}

func TestQuotaLedgerRecoveryRestoreCollectorContinuations(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, boundary := range []string{"before-dispatch", "lost-reply", "post-delete-ledger"} {
			t.Run(fault+"/"+boundary, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				x.releaseFirst(t)
				transport := &r3CleanupTransport{
					Client:   x.adapter,
					boundary: boundary,
					blocked:  true,
				}
				x.s.opts.Engine = transport
				if boundary == "post-delete-ledger" {
					if _, err := x.s.store.db.Exec(
						`CREATE TRIGGER r3_reclaim BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk' BEGIN SELECT RAISE(ABORT,'T-reclaim'); END`,
					); err != nil {
						t.Fatal(err)
					}
				}
				_ = x.s.recoverResources()
				frozen := x.collector(t)
				key := frozen.Cleanup.Request.Key
				source := frozen.Source
				request := encode(frozen.Cleanup.Original.Request)
				t.Logf(
					"collector=%s Source=%s Attempt=%d Bkey=%s originalA=%s immutableB=%s",
					frozen.ID,
					source,
					frozen.Attempt,
					key,
					request,
					frozen.Instance,
				)
				for _, fresh := range []string{"fresh-R2", "fresh-R3"} {
					if fresh == "fresh-R3" {
						x.s = ledgerRestart(t, x.s)
					}
					result, _ := x.s.Execute(
						x.s.ctx,
						x.p,
						fresh,
						Command{Action: "release", SandboxID: x.b.ID},
					)
					if result.OperationID == x.release ||
						result.OperationID == "" {
						t.Fatal("fresh release lacks new public identity")
					}
					_ = x.s.recoverResources()
					current := x.collector(t)
					if current.ID != frozen.ID || current.Source != source ||
						current.Attempt != 0 ||
						current.Cleanup.Request.Key != key ||
						!bytes.Equal(
							request,
							encode(current.Cleanup.Original.Request),
						) ||
						current.Done {
						t.Fatal("fresh release changed frozen cleanup sequence")
					}
					ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
					ledgerRejectClaim(t, x.s, x.f, x.p, "blocked-"+fresh)
				}
				transport.mu.Lock()
				transport.blocked = false
				transport.mu.Unlock()
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER IF EXISTS r3_reclaim`,
				); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4; i++ {
					_ = x.s.recoverResources()
				}
				x.assertImmutable(t)
				ledgerMoney(t, x.s, x.p.TenantID, 0, 192)
				x.f.mu.Lock()
				deletes := x.f.physicalDeletes
				x.f.mu.Unlock()
				if deletes != 1 || x.f.count("restore") != 1 ||
					!x.collector(t).Done {
					t.Fatalf(
						"physical effects: Bdelete=%d restore=%d",
						deletes,
						x.f.count("restore"),
					)
				}
				t.Logf(
					"physical Bdelete=%d restore=%d reclaimed collector=%s; remaining CP192/slot0",
					deletes,
					x.f.count("restore"),
					frozen.ID,
				)
				ledgerExecute(
					t,
					x.s,
					x.p,
					"delete-cp",
					Command{Action: "delete_checkpoint", CheckpointID: x.cp.ID},
				)
				ledgerMoney(t, x.s, x.p.TenantID, 0, 0)
				ledgerExecute(
					t,
					x.s,
					x.p,
					"new-claim",
					Command{Action: "claim", TemplateID: "python"},
				)
			})
		}
	}
}

func TestQuotaLedgerRecoveryRestoreResolutionProgress(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, window := range []string{"before-publication", "resolution-update", "parent-done", "after-publication-new-C"} {
			t.Run(fault+"/"+window, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER IF EXISTS r3_initial`,
				); err != nil {
					t.Fatal(err)
				}
				trigger := `CREATE TRIGGER r3_active BEFORE UPDATE ON sandboxes WHEN NEW.state='running' BEGIN SELECT RAISE(ABORT,'publication'); END`
				if window == "resolution-update" {
					trigger = `CREATE TRIGGER r3_active BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.allocations[0].resolution.state')='active' BEGIN SELECT RAISE(ABORT,'active resolution'); END`
				}
				if window == "parent-done" ||
					window == "after-publication-new-C" {
					trigger = `CREATE TRIGGER r3_active BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.done')=1 AND json_extract(NEW.body,'$.kind')='sandbox' BEGIN SELECT RAISE(ABORT,'parent done'); END`
				}
				if _, err := x.s.store.db.Exec(trigger); err != nil {
					t.Fatal(err)
				}
				_ = x.s.recoverResources()
				state := recoverySandbox(t, x.s, x.b.ID)
				all, err := x.s.resourceRecoveries()
				if err != nil {
					t.Fatal(err)
				}
				var allocation recoveryAllocation
				for _, r := range all {
					if len(r.Allocations) > 0 {
						allocation = r.Allocations[0]
					}
				}
				if window == "before-publication" ||
					window == "resolution-update" {
					if state.State != "resuming" ||
						allocation.Resolution.State != "result_known" {
						t.Fatalf(
							"publication was not atomic: state=%s resolution=%s",
							state.State,
							allocation.Resolution.State,
						)
					}
				} else if state.State != "running" ||
					allocation.Resolution.State != "active" {
					t.Fatalf(
						"active destination not durable: %+v %+v",
						state,
						allocation,
					)
				}
				originalB := allocation.Resolution.Proof.ResultInstance
				if window == "after-publication-new-C" {
					ledgerExecute(
						t,
						x.s,
						x.p,
						"pause-C",
						Command{Action: "pause", SandboxID: x.b.ID},
					)
					ledgerExecute(
						t,
						x.s,
						x.p,
						"resume-C",
						Command{Action: "resume", SandboxID: x.b.ID},
					)
				}
				if _, err = x.s.store.db.Exec(
					`DROP TRIGGER r3_active`,
				); err != nil {
					t.Fatal(err)
				}
				x.s = ledgerRestart(t, x.s)
				for i := 0; i < 4; i++ {
					_ = x.s.recoverResources()
				}
				state = recoverySandbox(t, x.s, x.b.ID)
				if state.State != "running" ||
					(window != "after-publication-new-C" && state.Instance != originalB) ||
					(window == "after-publication-new-C" && state.Instance == originalB) ||
					x.f.count("restore") != 1 {
					t.Fatalf(
						"exact B activation failed or repeated: %+v",
						state,
					)
				}
				x.assertImmutable(t)
				ledgerExecute(
					t,
					x.s,
					x.p,
					"release-final",
					Command{Action: "release", SandboxID: x.b.ID},
				)
			})
		}
	}
}

func TestQuotaLedgerRecoveryRestoreTransactions(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, window := range []string{"collector", "refs", "resolution", "parent-done", "reclaim"} {
			t.Run(fault+"/"+window, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				x.releaseFirst(t)
				triggers := map[string]string{
					"collector":   `CREATE TRIGGER r3_tx BEFORE INSERT ON resource_recovery WHEN json_extract(NEW.body,'$.kind')='instance_cleanup' BEGIN SELECT RAISE(ABORT,'collector'); END`,
					"refs":        `CREATE TRIGGER r3_tx BEFORE INSERT ON quota_refs WHEN NEW.holder_id LIKE 'recovery:instance-cleanup:%' BEGIN SELECT RAISE(ABORT,'collector refs'); END`,
					"resolution":  `CREATE TRIGGER r3_tx BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.allocations[0].resolution.state')='cleanup' BEGIN SELECT RAISE(ABORT,'handoff resolution'); END`,
					"parent-done": `CREATE TRIGGER r3_tx BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.done')=1 AND json_extract(NEW.body,'$.kind')='sandbox' BEGIN SELECT RAISE(ABORT,'done'); END`,
					"reclaim":     `CREATE TRIGGER r3_tx BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk' BEGIN SELECT RAISE(ABORT,'reclaim'); END`,
				}
				if _, err := x.s.store.db.Exec(triggers[window]); err != nil {
					t.Fatal(err)
				}
				if err := x.s.recoverResources(); err == nil {
					t.Fatal("SQLite fault not reached", window)
				}
				if window != "parent-done" {
					ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
				}
				x.s = ledgerRestart(t, x.s)
				_ = x.s.recoverResources()
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER r3_tx`,
				); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 4; i++ {
					_ = x.s.recoverResources()
				}
				ledgerMoney(t, x.s, x.p.TenantID, 0, 192)
				x.assertImmutable(t)
				x.f.mu.Lock()
				deletes := x.f.physicalDeletes
				x.f.mu.Unlock()
				if deletes != 1 || x.f.count("restore") != 1 {
					t.Fatal("physical effects repeated")
				}
			})
		}
	}
}

func TestQuotaLedgerRecoveryRestoreAcceptedFailureCosts(t *testing.T) {
	t.Run("383-before-restore", func(t *testing.T) {
		s, f, _, p := recoveryFixture(t)
		ledgerLimit(t, s, p, 1, 383)
		b := *claimTest(t, s, p, "source", nil).Sandbox
		cp := ledgerExecute(
			t,
			s,
			p,
			"checkpoint",
			Command{Action: "checkpoint", SandboxID: b.ID},
		).Checkpoint
		s.opts.Engine = &ledgerRecoveryDeliveryEngine{
			recoveryTestEngine:  f,
			rejectPublicRestore: true,
			faithfulFailure:     true,
		}
		_, err := s.Execute(
			s.ctx,
			p,
			"rollback",
			Command{Action: "rollback", SandboxID: b.ID, CheckpointID: cp.ID},
		)
		if err == nil {
			t.Fatal("ordinary restore fault missing")
		}
		ledgerMoney(t, s, p.TenantID, 1, 320)
		if err = s.recoverResources(); !isCode(err, api.Exhausted) {
			t.Fatal("383 admitted extra D64", err)
		}
		if f.count("restore") != 0 {
			t.Fatal("383 allocated before admission")
		}
		ledgerMoney(t, s, p.TenantID, 1, 320)
		ledgerLimit(t, s, p, 1, 384)
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		if f.count("restore") != 1 {
			t.Fatal("384 did not permit exact D64")
		}
		ledgerMoney(t, s, p.TenantID, 1, 384)
	})
	x := newR3RestoreScenario(t, "transport", true)
	ledgerMoney(t, x.s, x.p.TenantID, 1, 384)
	if _, err := x.s.store.db.Exec(
		`DROP TRIGGER IF EXISTS r3_initial`,
	); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		_ = x.s.recoverResources()
	}
	ledgerMoney(t, x.s, x.p.TenantID, 1, 384)
	rows, err := x.s.store.db.Query(
		`SELECT kind,resource_key,storage_mib,source_operation_id FROM quota_resources ORDER BY kind,resource_key`,
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var kind, key, source string
		var size int
		if err = rows.Scan(&kind, &key, &size, &source); err != nil {
			t.Fatal(err)
		}
		t.Logf(
			"resource kind=%s key=%s MiB=%d source=%s",
			kind,
			key,
			size,
			source,
		)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	refs, err := x.s.store.db.Query(
		`SELECT kind,resource_key,holder_kind,holder_id FROM quota_refs ORDER BY kind,resource_key,holder_id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	for refs.Next() {
		var kind, key, holderKind, holder string
		if err = refs.Scan(&kind, &key, &holderKind, &holder); err != nil {
			t.Fatal(err)
		}
		t.Logf(
			"ref kind=%s key=%s holder_kind=%s holder=%s",
			kind,
			key,
			holderKind,
			holder,
		)
	}
	if err = refs.Close(); err != nil {
		t.Fatal(err)
	}
	x.f.mu.Lock()
	for _, receipt := range x.f.ops {
		if receipt.Request.Action == "restore" {
			t.Logf(
				"restore receipt=%s status=%s VM=%s witness=%q request=%s",
				receipt.ID,
				receipt.Status,
				receipt.VMID,
				receipt.ResultInstance,
				encode(receipt.Request),
			)
		}
	}
	v := x.f.vms[x.b.VMID]
	x.f.mu.Unlock()
	t.Logf(
		"physical restored V=%s instance=%s; one restore effect, failed public receipt has no successful candidate witness",
		v.ID,
		v.Instance,
	)
	ledgerLimit(t, x.s, x.p, 2, 383)
	ledgerRejectClaim(t, x.s, x.f, x.p, "383-reject")
	ledgerExecute(
		t,
		x.s,
		x.p,
		"release",
		Command{Action: "release", SandboxID: x.b.ID},
	)
	ledgerMoney(t, x.s, x.p.TenantID, 0, 192)
	ledgerExecute(
		t,
		x.s,
		x.p,
		"delete-cp",
		Command{Action: "delete_checkpoint", CheckpointID: x.cp.ID},
	)
	ledgerMoney(t, x.s, x.p.TenantID, 0, 0)
	x.assertImmutable(t)
	ledgerExecute(
		t,
		x.s,
		x.p,
		"new-claim",
		Command{Action: "claim", TemplateID: "python"},
	)
}

type r3ObservingAgent struct {
	Agent

	beforeInfo     func(api.VM)
	beforeActivate func(api.VM)
}

func (a *r3ObservingAgent) Info(ctx context.Context, v api.VM) error {
	if a.beforeInfo != nil {
		a.beforeInfo(v)
	}
	return a.Agent.Info(ctx, v)
}

func (a *r3ObservingAgent) Activate(
	ctx context.Context,
	v api.VM,
	activation Activation,
) error {
	if a.beforeActivate != nil {
		a.beforeActivate(v)
	}
	return a.Agent.Activate(ctx, v, activation)
}

type r3ObservingEngine struct {
	api.Client

	beforeRead   func(string)
	beforeSubmit func(api.Request)
}

func (e *r3ObservingEngine) GetVM(
	ctx context.Context,
	id string,
) (api.VM, error) {
	if e.beforeRead != nil {
		e.beforeRead(id)
	}
	return e.Client.GetVM(ctx, id)
}

func (e *r3ObservingEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	if e.beforeSubmit != nil {
		e.beforeSubmit(r)
	}
	return e.Client.Submit(ctx, r)
}

func TestQuotaLedgerRecoveryRestoreForeignInstance(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, damage := range []string{"foreign-W", "foreign-C", "unverified", "other-owner", "missing-witness", "key-mismatch", "query-error", "activation-read-C", "agent-C", "delete-admission-C"} {
			t.Run(fault+"/"+damage, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER IF EXISTS r3_initial`,
				); err != nil {
					t.Fatal(err)
				}
				replace := func() {
					x.f.mu.Lock()
					v := x.f.vms[x.b.VMID]
					v.Instance = "foreign-C"
					x.f.vms[x.b.VMID] = v
					x.f.mu.Unlock()
				}
				if damage == "delete-admission-C" {
					x.releaseFirst(t)
				}
				switch damage {
				case "foreign-W":
					b := recoverySandbox(t, x.s, x.b.ID)
					b.VMID = "foreign-W"
					if err := saveSandbox(
						x.s.ctx,
						x.s.store.db,
						b,
					); err != nil {
						t.Fatal(err)
					}
				case "foreign-C":
					replace()
				case "unverified", "other-owner":
					x.f.mu.Lock()
					v := x.f.vms[x.b.VMID]
					if damage == "unverified" {
						v.Verified = false
					} else {
						v.OperationID = "foreign-owner"
					}
					x.f.vms[x.b.VMID] = v
					x.f.mu.Unlock()
				case "missing-witness", "key-mismatch":
					x.f.mu.Lock()
					for key, op := range x.f.ops {
						if op.Request.Action == "restore" &&
							strings.Contains(key, "recovery:") {
							if damage == "missing-witness" {
								op.ResultInstance = ""
							} else {
								op.Request.ExpectedInstance = "mismatching-A"
							}
							x.f.ops[key] = op
						}
					}
					x.f.mu.Unlock()
				case "query-error":
					x.f.control.Lock()
					x.f.findError = api.Fail(api.Unknown, "receipt unavailable")
					x.f.control.Unlock()
				case "activation-read-C":
					reads := 0
					x.s.opts.Engine = &r3ObservingEngine{
						Client: x.adapter,
						beforeRead: func(id string) {
							if id == x.b.VMID {
								reads++
								if reads == 3 {
									replace()
								}
							}
						},
					}
				case "agent-C":
					x.s.opts.Agent = &r3ObservingAgent{
						Agent:      x.s.opts.Agent,
						beforeInfo: func(v api.VM) { replace() },
					}
				case "delete-admission-C":
					x.s.opts.Engine = &r3ObservingEngine{
						Client: x.adapter,
						beforeSubmit: func(r api.Request) {
							if strings.Contains(r.Key, "instance-cleanup:") {
								replace()
							}
						},
					}
				}
				if err := x.s.recoverResources(); err == nil {
					t.Fatal("untrusted continuation was accepted", damage)
				}
				x.s = ledgerRestart(t, x.s)
				_ = x.s.recoverResources()
				if x.f.count("restore") != 1 {
					t.Fatal("unknown result repeated restore")
				}
				x.f.mu.Lock()
				deletes := x.f.physicalDeletes
				current := x.f.vms[x.b.VMID]
				x.f.mu.Unlock()
				if deletes != 0 {
					t.Fatal("foreign or unproven VM was deleted")
				}
				if damage == "foreign-C" || damage == "activation-read-C" ||
					damage == "agent-C" ||
					damage == "delete-admission-C" {
					if current.Instance != "foreign-C" {
						t.Fatal("foreign C changed")
					}
				}
				ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
				ledgerRejectClaim(t, x.s, x.f, x.p, "unproven-new-claim")
				x.assertImmutable(t)
			})
		}
	}
}

func TestQuotaLedgerRecoveryRestoreCleanupReferencesAndKeys(t *testing.T) {
	for _, mode := range []string{"cleanup-only", "active-use", "engine-dependency", "B-terminal-failure"} {
		t.Run(mode, func(t *testing.T) {
			x := newR3RestoreScenario(t, "transport", false)
			x.releaseFirst(t)
			transport := &r3CleanupTransport{
				Client:   x.adapter,
				boundary: "before-dispatch",
				blocked:  true,
			}
			x.s.opts.Engine = transport
			_ = x.s.recoverResources()
			collector := x.collector(t)
			originalKey := collector.Cleanup.Request.Key
			if mode != "B-terminal-failure" {
				kind := "cleanup"
				if mode == "active-use" {
					kind = "active_use"
				}
				if mode == "engine-dependency" {
					kind = "engine_dependency"
				}
				if err := putQuotaRef(
					x.s.ctx,
					x.s.store.db,
					x.p.TenantID,
					"vm_disk",
					x.b.VMID,
					quotaRef{Kind: kind, ID: "independent-user"},
				); err != nil {
					t.Fatal(err)
				}
			}
			transport.mu.Lock()
			transport.blocked = false
			transport.mu.Unlock()
			if mode == "B-terminal-failure" {
				x.s.opts.Engine = x.adapter
				x.f.control.Lock()
				x.f.failDeletes = 1
				x.f.control.Unlock()
				if _, err := x.s.store.db.Exec(
					`CREATE TRIGGER r3_attempt BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.kind')='instance_cleanup' AND json_extract(NEW.body,'$.attempt')>0 BEGIN SELECT RAISE(ABORT,'attempt history'); END`,
				); err != nil {
					t.Fatal(err)
				}
			}
			_ = x.s.recoverResources()
			if mode == "active-use" || mode == "engine-dependency" {
				ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
				if x.collector(t).Done {
					t.Fatal("live dependency ignored")
				}
				if _, err := x.s.store.db.Exec(
					`DELETE FROM quota_refs WHERE holder_id='independent-user'`,
				); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "B-terminal-failure" {
				_ = x.s.recoverResources()
				same := x.collector(t)
				if same.Attempt != 0 ||
					same.Cleanup.Request.Key != originalKey {
					t.Fatal("failed attempt TX changed key")
				}
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER r3_attempt`,
				); err != nil {
					t.Fatal(err)
				}
				x.s = ledgerRestart(t, x.s)
			}
			for i := 0; i < 4; i++ {
				_ = x.s.recoverResources()
			}
			final := x.collector(t)
			if !final.Done {
				t.Fatal("cleanup did not converge")
			}
			if mode == "B-terminal-failure" &&
				(final.Attempt != 1 || len(final.Cleanup.History) != 1 || final.Cleanup.History[0].Request.Key != originalKey || final.Cleanup.Request.Key == originalKey) {
				t.Fatal("terminal failure retry history missing")
			}
			ledgerMoney(t, x.s, x.p.TenantID, 0, 192)
			x.assertImmutable(t)
		})
	}
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
	for i := 0; i < 4; i++ {
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

type r3FailedCompensation struct {
	api.Client

	f      *recoveryTestEngine
	mu     sync.Mutex
	failed bool
	omitVM bool
	key    string
}

func (e *r3FailedCompensation) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	e.mu.Lock()
	fail := !e.failed && r.Action == "restore" &&
		strings.Contains(r.Key, "recovery:")
	if fail {
		e.failed = true
		e.key = r.Key
	}
	e.mu.Unlock()
	if !fail {
		return e.Client.Submit(ctx, r)
	}
	e.f.mu.Lock()
	defer e.f.mu.Unlock()
	op := api.Operation{
		ID:      r.Key,
		Key:     r.Key,
		Request: r,
		VMID:    r.VMID,
		Status:  "failed",
		Error: &api.Error{
			Code:    api.Internal,
			Message: "accepted compensation failed before candidate",
		},
	}
	if e.omitVM {
		op.VMID = ""
	}
	e.f.ops[r.Key] = op
	return op, nil
}

func TestQuotaLedgerRecoveryFailedRestoreTransfersSnapshotDependency(
	t *testing.T,
) {
	for _, scenario := range []string{"accepted-VMID", "no-result", "destination-sql", "dependency-sql", "release-before-destination"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			ledgerLimit(t, s, p, 1, 448)
			b := *claimTest(t, s, p, "source", nil).Sandbox
			cp := ledgerExecute(
				t,
				s,
				p,
				"checkpoint",
				Command{Action: "checkpoint", SandboxID: b.ID},
			).Checkpoint
			original := &ledgerRecoveryDeliveryEngine{
				recoveryTestEngine:  f,
				rejectPublicRestore: true,
				faithfulFailure:     true,
			}
			adapter := &r3FailedCompensation{
				Client: original,
				f:      f,
				omitVM: scenario == "no-result",
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
				t.Fatal("public restore did not fail")
			}
			var body []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
				Scan(&body); err != nil {
				t.Fatal(err)
			}
			if err = s.recoverResources(); err == nil {
				t.Fatal("compensation failure missing")
			}
			initial, peak := int64(384), int64(448)
			if scenario == "no-result" {
				initial, peak = 320, 384
			}
			ledgerMoney(t, s, p.TenantID, 1, initial)
			if scenario == "release-before-destination" {
				ledgerExecute(
					t,
					s,
					p,
					"release-first",
					Command{Action: "release", SandboxID: b.ID},
				)
				s = ledgerRestart(t, s)
				for i := 0; i < 3; i++ {
					_ = s.recoverResources()
				}
				ledgerExecute(
					t,
					s,
					p,
					"delete-cp-first",
					Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
				)
				for i := 0; i < 3; i++ {
					_ = s.recoverResources()
				}
				ledgerMoney(t, s, p.TenantID, 0, 0)
				if f.count("restore") != 0 {
					t.Fatal("terminated failed allocation was restored")
				}
				return
			}
			if scenario == "destination-sql" || scenario == "dependency-sql" {
				trigger := `CREATE TRIGGER r3_terminal_transfer BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.allocations[0].resolution.state')='cleanup' BEGIN SELECT RAISE(ABORT,'terminal destination'); END`
				if scenario == "dependency-sql" {
					trigger = `CREATE TRIGGER r3_terminal_transfer BEFORE DELETE ON quota_refs WHEN OLD.holder_kind='engine_dependency' AND OLD.holder_id LIKE 'operation:recovery:%' BEGIN SELECT RAISE(ABORT,'terminal dependency'); END`
				}
				if _, err = s.store.db.Exec(trigger); err != nil {
					t.Fatal(err)
				}
				if err = s.recoverResources(); err == nil {
					t.Fatal("terminal transfer SQL fault missing")
				}
				if f.count("restore") != 0 {
					t.Fatal(
						"next attempt crossed failed responsibility transfer",
					)
				}
				all, err := s.resourceRecoveries()
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range all {
					if len(r.Allocations) > 0 &&
						r.Allocations[0].Resolution.State != "result_known" {
						t.Fatal("failed transfer committed partial destination")
					}
				}
				if _, err = s.store.db.Exec(
					`DROP TRIGGER r3_terminal_transfer`,
				); err != nil {
					t.Fatal(err)
				}
			}
			s = ledgerRestart(t, s)
			for i := 0; i < 3; i++ {
				_ = s.recoverResources()
			}
			if f.count("restore") != 1 ||
				recoverySandbox(t, s, b.ID).State != "running" {
				t.Fatal(
					"known terminal failure did not permit its next attempt",
				)
			}
			ledgerMoney(t, s, p.TenantID, 1, peak)
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
			for i := 0; i < 3; i++ {
				_ = s.recoverResources()
			}
			ledgerMoney(t, s, p.TenantID, 0, 0)
			var after []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
				Scan(&after); err != nil ||
				!bytes.Equal(body, after) {
				t.Fatal("public failure changed", err)
			}
			if _, err = f.GetSnapshot(
				s.ctx,
				cp.SnapshotID,
			); !isCode(
				err,
				api.NotFound,
			) {
				t.Fatal(
					"terminal failed restore retained its source dependency",
					err,
				)
			}
		})
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
				for i := 0; i < 3; i++ {
					_ = s.recoverResources()
				}
			}
			ledgerMoney(t, s, p.TenantID, 0, 0)
		})
	}
}

func TestQuotaLedgerRecoveryRestoreReleaseFirst(t *testing.T) {
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
				t.Fatal("missing ordinary restore fault")
			}
			var original []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, result.OperationID).
				Scan(&original); err != nil {
				t.Fatal(err)
			}
			if fault == "transport" {
				adapter.action = "restore"
			} else {
				if _, err = s.store.db.Exec(
					`CREATE TRIGGER r3_restore_charge BEFORE INSERT ON quota_resources WHEN NEW.kind='vm_disk' AND NEW.resource_key LIKE '%:restore:%' BEGIN SELECT RAISE(FAIL,'r3 restore ledger'); END`,
				); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.recoverResources(); err == nil {
				t.Fatal("missing recovery restore fault")
			}
			if f.count("restore") != 1 {
				t.Fatal("restore not executed once")
			}
			release, releaseErr := s.Execute(
				s.ctx,
				p,
				"release-before-receipt",
				Command{Action: "release", SandboxID: b.ID},
			)
			if !isCode(releaseErr, api.Conflict) {
				t.Fatal("need original stale instance rejection", releaseErr)
			}
			var failedRelease []byte
			if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, release.OperationID).
				Scan(&failedRelease); err != nil {
				t.Fatal(err)
			}
			if fault == "sqlite" {
				if _, err = s.store.db.Exec(
					`DROP TRIGGER r3_restore_charge`,
				); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 4; i++ {
				_ = s.recoverResources()
			}
			s = ledgerRestart(t, s)
			for i := 0; i < 4; i++ {
				_ = s.recoverResources()
			}
			_, _ = s.Execute(
				s.ctx,
				p,
				"release-again",
				Command{Action: "release", SandboxID: b.ID},
			)
			for i := 0; i < 4; i++ {
				_ = s.recoverResources()
			}
			var after []byte
			for id, body := range map[string][]byte{result.OperationID: original, release.OperationID: failedRelease} {
				if err = s.store.db.QueryRow(`SELECT body FROM operations WHERE id=?`, id).
					Scan(&after); err != nil ||
					!bytes.Equal(body, after) {
					t.Fatal("public failure changed", err)
				}
			}
			vms, err := f.ListVMs(s.ctx)
			if err != nil || len(vms) != 0 {
				t.Fatalf(
					"completed compensation restore lost release cleanup: VMs=%+v err=%v",
					vms,
					err,
				)
			}
			ledgerMoney(t, s, p.TenantID, 0, 192)
			if f.count("restore") != 1 {
				t.Fatal("restore repeated")
			}
			ledgerExecute(
				t,
				s,
				p,
				"delete-cp",
				Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
			)
			ledgerMoney(t, s, p.TenantID, 0, 0)
			ledgerExecute(
				t,
				s,
				p,
				"new-claim",
				Command{Action: "claim", TemplateID: "python"},
			)
		})
	}
}

// Delivery faults leave the strict fixture's deterministic engine receipt intact.
// An ordinary restore can fail terminally before allocation so compensation owns
// the single actual restore effect.
type ledgerRecoveryDeliveryEngine struct {
	*recoveryTestEngine

	muDelivery          sync.Mutex
	action              string
	faithfulFailure     bool
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
			if !bytes.Equal(encode(old.Request), encode(r)) {
				return old, api.Fail(api.Conflict, "key parameters differ")
			}
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
		if f.faithfulFailure {
			o.VMID = r.VMID
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
