package sandboxd

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

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
	if !isCode(err, ErrManualRecovery) {
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

// These cases preserve the review's public Engine deletion and release/reopen
// ordering. Absence is observed through GetVM; no fixture inventory is erased.
func (x *r3RestoreScenario) deleteResultBeforeDestination(t *testing.T) {
	t.Helper()
	v, err := x.f.GetVM(x.s.ctx, x.b.VMID)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := api.Wait(x.s.ctx, x.f, api.Request{
		Key: "explicit-result-delete", Action: "delete", VMID: v.ID,
		ExpectedInstance: v.Instance,
	})
	if err != nil || proof.Status != "succeeded" {
		t.Fatal("legitimate exact-B delete failed", err)
	}
	if _, err = x.f.GetVM(x.s.ctx, v.ID); !isEngineCode(err, api.NotFound) {
		t.Fatal("deleted B is still present", err)
	}
	x.assertResultEffects(t)
}

func (x *r3RestoreScenario) assertResultEffects(t *testing.T) {
	t.Helper()
	x.f.mu.Lock()
	deletes := x.f.physicalDeletes
	x.f.mu.Unlock()
	if x.f.count("restore") != 1 || deletes != 1 {
		t.Fatalf("restore=%d physical delete=%d", x.f.count("restore"), deletes)
	}
}

func (x *r3RestoreScenario) resultAllocation(
	t *testing.T,
) (resourceRecovery, recoveryAllocation) {
	t.Helper()
	all, err := x.s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range all {
		if r.Source == x.rollback && len(r.Allocations) == 1 {
			return r, r.Allocations[0]
		}
	}
	t.Fatal("missing frozen restore allocation")
	return resourceRecovery{}, recoveryAllocation{}
}

func (x *r3RestoreScenario) finishAbsentResult(t *testing.T) {
	t.Helper()
	for i := range 4 {
		if err := x.s.recoverResources(); err != nil {
			t.Fatal("recover", i, err)
		}
	}
	x.s = ledgerRestart(t, x.s)
	for i := range 4 {
		if err := x.s.recoverResources(); err != nil {
			t.Fatal("reopen recover", i, err)
		}
	}
	ledgerExecute(
		t,
		x.s,
		x.p,
		"absent-fresh-release",
		Command{Action: "release", SandboxID: x.b.ID},
	)
	r, a := x.resultAllocation(t)
	if !r.Done || a.Resolution.State != "reclaimed" ||
		a.Resolution.Destination != "vm-absent:"+x.b.VMID ||
		a.Resolution.Proof == nil ||
		a.Resolution.Proof.ResultInstance == "" {
		t.Fatalf("allocation has no reclaimed delivery: %s", encode(r))
	}
	b := recoverySandbox(t, x.s, x.b.ID)
	if b.State != "terminated" || b.VMID != "" || b.Instance != "" {
		t.Fatalf("terminated metadata retained V: %+v", b)
	}
	var pending, dependency, backing int
	if err := x.s.store.db.QueryRow(`SELECT
	 (SELECT count(*) FROM quota_resources WHERE kind='pending_storage'),
	 (SELECT count(*) FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?),
	 (SELECT count(*) FROM quota_refs WHERE holder_id=?)`, "operation:"+a.OperationID, "backing:"+x.b.VMID).
		Scan(&pending, &dependency, &backing); err != nil || pending != 0 || dependency != 0 || backing != 0 {
		t.Fatal(
			"incomplete responsibility retirement",
			pending,
			dependency,
			backing,
			err,
		)
	}
	ledgerMoney(t, x.s, x.p.TenantID, 0, 192)
	t.Logf(
		"delivered allocation=%s request=%s proof=%s CP-active storage=192 slot=0 pending=0 dependency=0 backing=0 restore=1 physical-delete=1",
		r.ID,
		encode(a.Request),
		encode(a.Resolution),
	)
	rows, err := x.s.store.db.Query(
		`SELECT kind,resource_key,storage_mib FROM quota_resources ORDER BY kind,resource_key`,
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var kind, key string
		var storage int64
		if err = rows.Scan(&kind, &key, &storage); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		t.Logf("resource kind=%s key=%s storage=%d", kind, key, storage)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	ledgerExecute(
		t,
		x.s,
		x.p,
		"absent-delete-independent-cp",
		Command{Action: "delete_checkpoint", CheckpointID: x.cp.ID},
	)
	for range 4 {
		if err = x.s.recoverResources(); err != nil {
			t.Fatal(err)
		}
	}
	ledgerMoney(t, x.s, x.p.TenantID, 0, 0)
	if _, err = x.f.GetSnapshot(
		x.s.ctx,
		x.cp.SnapshotID,
	); !isEngineCode(
		err,
		api.NotFound,
	) {
		t.Fatal("CP physical snapshot was not reclaimed", err)
	}
	x.assertResultEffects(t)
	x.assertImmutable(t)
	ledgerLimit(t, x.s, x.p, 1, 192)
	ledgerExecute(
		t,
		x.s,
		x.p,
		"absent-new-claim",
		Command{Action: "claim", TemplateID: "python"},
	)
	ledgerMoney(t, x.s, x.p.TenantID, 1, 64)
	// Old receipt replay during further scans cannot recreate/refund the old disk
	// or affect the new sandbox's responsibility.
	for range 2 {
		if err = x.s.recoverResources(); err != nil {
			t.Fatal(err)
		}
	}
	ledgerMoney(t, x.s, x.p.TenantID, 1, 64)
	x.assertResultEffects(t)
	x.assertImmutable(t)
}

func TestQuotaLedgerRecoverySuccessfulRestoreAbsentBeforeDestination(
	t *testing.T,
) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, metadata := range []string{"cleared-by-release", "original-V"} {
			t.Run(fault+"/"+metadata, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				ledgerRejectClaim(t, x.s, x.f, x.p, "absent-before-effects")
				if metadata == "original-V" {
					x.releaseFirst(t)
				}
				x.deleteResultBeforeDestination(t)
				if _, err := x.s.store.db.Exec(
					`DROP TRIGGER IF EXISTS r3_initial`,
				); err != nil {
					t.Fatal(err)
				}
				if metadata == "cleared-by-release" {
					result := ledgerExecute(
						t,
						x.s,
						Principal{Root: true, TenantID: "bob"},
						"absent-public-release",
						Command{Action: "release", SandboxID: x.b.ID},
					)
					op, err := x.s.store.operation(x.s.ctx, result.OperationID)
					if err != nil || op.TenantID != "bob" ||
						op.ChargeTenantID != x.p.TenantID {
						t.Fatal("root request/charge owner changed", err)
					}
					b := recoverySandbox(t, x.s, x.b.ID)
					if b.VMID != "" || b.Instance != "" {
						t.Fatal(
							"public release did not clear absent V metadata",
						)
					}
				}
				x.finishAbsentResult(t)
			})
		}
	}
}

func TestQuotaLedgerRecoveryAbsentResultTransaction(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, effect := range []string{"ABORT", "IGNORE"} {
			for _, boundary := range []string{"destination", "dependency", "disk", "backing", "slot", "metadata"} {
				t.Run(fault+"/"+effect+"/"+boundary, func(t *testing.T) {
					x := newR3RestoreScenario(t, fault, false)
					x.releaseFirst(t)
					x.deleteResultBeforeDestination(t)
					raise := "RAISE(" + effect + ",'absent result transaction')"
					if effect == "IGNORE" {
						raise = "RAISE(IGNORE)"
					}
					statement := map[string]string{
						"destination": `BEFORE UPDATE ON resource_recovery WHEN json_extract(NEW.body,'$.allocations[0].resolution.state')='reclaimed'`,
						"dependency":  `BEFORE DELETE ON quota_refs WHEN OLD.holder_kind='engine_dependency' AND OLD.holder_id LIKE 'operation:recovery:%'`,
						"disk":        `BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk'`,
						"backing":     `BEFORE DELETE ON quota_refs WHEN OLD.holder_id LIKE 'backing:%'`,
						"slot":        `BEFORE DELETE ON quota_resources WHEN OLD.kind='sandbox_slot'`,
						"metadata":    `BEFORE UPDATE ON sandboxes WHEN json_extract(OLD.body,'$.vm_id') IS NOT NULL AND COALESCE(json_extract(NEW.body,'$.vm_id'),'')=''`,
					}[boundary]
					if _, err := x.s.store.db.Exec(
						"CREATE TRIGGER absent_transaction " + statement + " BEGIN SELECT " + raise + "; END",
					); err != nil {
						t.Fatal(err)
					}
					if err := x.s.recoverResources(); err == nil {
						t.Fatal(
							"SQL no-effect/error did not block destination",
							boundary,
						)
					} else {
						t.Logf(
							"actual %s/%s blocked delivery: %v",
							effect,
							boundary,
							err,
						)
					}
					_, before := x.resultAllocation(t)
					frozen := encode(before.Request)
					proof := encode(before.Resolution.Proof)
					x.s = ledgerRestart(t, x.s)
					if err := x.s.recoverResources(); err == nil {
						t.Fatal("reopen crossed persistent SQL fault")
					}
					r, a := x.resultAllocation(t)
					if r.Done || a.Resolution.State != "result_known" ||
						a.Resolution.Destination != "" ||
						!bytes.Equal(frozen, encode(a.Request)) ||
						!bytes.Equal(proof, encode(a.Resolution.Proof)) {
						t.Fatalf(
							"fault committed partial delivery: %s",
							encode(r),
						)
					}
					var dependency, backing int
					if err := x.s.store.db.QueryRow(`SELECT
					 (SELECT count(*) FROM quota_refs WHERE holder_kind='engine_dependency' AND holder_id=?),
					 (SELECT count(*) FROM quota_refs WHERE holder_id=?)`, "operation:"+a.OperationID, "backing:"+x.b.VMID).
						Scan(&dependency, &backing); err != nil || dependency < 1 || backing < 1 {
						t.Fatal(
							"fault lost dependency/backing responsibility",
							dependency,
							backing,
							err,
						)
					}
					ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
					ledgerRejectClaim(t, x.s, x.f, x.p, "absent-fault-refusal")
					x.assertResultEffects(t)
					x.assertImmutable(t)
					t.Logf(
						"fault retained allocation=%s proof=%s dependency=%d backing=%d storage=320 slot=1",
						r.ID,
						proof,
						dependency,
						backing,
					)
					if _, err := x.s.store.db.Exec(
						`DROP TRIGGER absent_transaction`,
					); err != nil {
						t.Fatal(err)
					}
					x.finishAbsentResult(t)
				})
			}
		}
	}
}

type absentResultQueryEngine struct {
	api.Client

	vm string
}

func (e *absentResultQueryEngine) GetVM(
	ctx context.Context,
	id string,
) (api.VM, error) {
	if id == e.vm {
		return api.VM{}, api.Fail(api.Unknown, "VM proof query unavailable")
	}
	return e.Client.GetVM(ctx, id)
}

func TestQuotaLedgerRecoveryAbsentResultRejectsUnprovenHistory(t *testing.T) {
	for _, fault := range []string{"transport", "sqlite"} {
		for _, damage := range []string{"query-error", "missing-witness", "foreign-result-V", "foreign-S", "public-request", "current-C", "unverified", "other-owner", "engine-dependency"} {
			t.Run(fault+"/"+damage, func(t *testing.T) {
				x := newR3RestoreScenario(t, fault, false)
				x.releaseFirst(t)
				if damage != "current-C" && damage != "unverified" &&
					damage != "other-owner" {
					x.deleteResultBeforeDestination(t)
				}
				switch damage {
				case "query-error":
					x.s.opts.Engine = &absentResultQueryEngine{
						Client: x.adapter,
						vm:     x.b.VMID,
					}
				case "missing-witness", "foreign-result-V":
					x.f.mu.Lock()
					for key, op := range x.f.ops {
						if op.Request.Action == "restore" &&
							strings.Contains(key, "recovery:") {
							if damage == "missing-witness" {
								op.ResultInstance = ""
							} else {
								op.VMID = "foreign-V"
							}
							x.f.ops[key] = op
						}
					}
					x.f.mu.Unlock()
				case "foreign-S":
					r, _ := x.resultAllocation(t)
					r.SandboxID = "foreign-S"
					if err := writeRecovery(
						x.s.ctx,
						x.s.store.db,
						r,
					); err != nil {
						t.Fatal(err)
					}
				case "public-request":
					if _, err := x.s.store.db.Exec(
						`UPDATE operations SET body=json_set(body,'$.command.sandbox_id','foreign-S') WHERE id=?`,
						x.rollback,
					); err != nil {
						t.Fatal(err)
					}
					// This case intentionally corrupts the test-owned public request;
					// retain the corrupted bytes to prove recovery never repairs history.
					x.remember(t, x.rollback)
				case "current-C", "unverified", "other-owner":
					x.f.mu.Lock()
					v := x.f.vms[x.b.VMID]
					switch damage {
					case "current-C":
						v.Instance = "foreign-C"
					case "unverified":
						v.Verified = false
					case "other-owner":
						v.OperationID = "other-owner"
					}
					x.f.vms[x.b.VMID] = v
					x.f.mu.Unlock()
				case "engine-dependency":
					if err := putQuotaRef(
						x.s.ctx,
						x.s.store.db,
						x.p.TenantID,
						"vm_disk",
						x.b.VMID,
						quotaRef{
							Kind: "engine_dependency",
							ID:   "independent-reader",
						},
					); err != nil {
						t.Fatal(err)
					}
				}
				if err := x.s.recoverResources(); err == nil {
					t.Fatal("unproven result was accepted", damage)
				}
				x.s = ledgerRestart(t, x.s)
				if err := x.s.recoverResources(); err == nil {
					t.Fatal("reopen accepted unproven history", damage)
				}
				r, a := x.resultAllocation(t)
				if r.Done || a.Resolution.delivered() {
					t.Fatalf("unproven allocation delivered: %s", encode(r))
				}
				ledgerMoney(t, x.s, x.p.TenantID, 1, 320)
				ledgerRejectClaim(t, x.s, x.f, x.p, "absent-unproven-refusal")
				x.assertImmutable(t)
				if x.f.count("restore") != 1 {
					t.Fatal("unproven result caused another restore")
				}
				x.f.mu.Lock()
				deletes := x.f.physicalDeletes
				current := x.f.vms[x.b.VMID]
				x.f.mu.Unlock()
				expectedDeletes := 1
				if damage == "current-C" || damage == "unverified" ||
					damage == "other-owner" {
					expectedDeletes = 0
				}
				if deletes != expectedDeletes ||
					(damage == "current-C" && current.Instance != "foreign-C") {
					t.Fatal(
						"unproven continuation changed physical target",
						deletes,
						current.Instance,
					)
				}
			})
		}
	}
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
				for range 4 {
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
				for range 4 {
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
				for range 4 {
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
		if err = s.recoverResources(); !isCode(err, ErrExhausted) {
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
	for range 4 {
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
			for range 4 {
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
				for range 3 {
					_ = s.recoverResources()
				}
				ledgerExecute(
					t,
					s,
					p,
					"delete-cp-first",
					Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
				)
				for range 3 {
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
			for range 3 {
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
			for range 3 {
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
			); !isEngineCode(
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
			if !isCode(releaseErr, ErrManualRecovery) {
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
			for range 4 {
				_ = s.recoverResources()
			}
			s = ledgerRestart(t, s)
			for range 4 {
				_ = s.recoverResources()
			}
			_, _ = s.Execute(
				s.ctx,
				p,
				"release-again",
				Command{Action: "release", SandboxID: b.ID},
			)
			for range 4 {
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
				for range 3 {
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
				for range 3 {
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
			for range 3 {
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
			); !isEngineCode(
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
			for range 3 {
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
