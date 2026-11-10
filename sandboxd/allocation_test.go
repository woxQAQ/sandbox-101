package sandboxd

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func templateQuotaFaultObserved(
	t *testing.T,
	s *Service,
	fault string,
	err error,
) {
	t.Helper()
	if err == nil {
		t.Fatal("required SQL fault not reached")
	}
	if fault == "commit" &&
		!strings.Contains(privateDiagnostic(err), "FOREIGN KEY") {
		t.Fatal("not a deferred COMMIT error", err)
	}
	t.Logf("SQL fault=%s actual=%v", fault, err)
	templateQuotaReuseConnection(t, s, fault)
}

func templateQuotaCaptureFault(t *testing.T, target, fault string) {
	s, f, _, p := recoveryFixture(t)
	e := &templateQuotaEngine{
		Client:      f,
		attempts:    map[string]int{},
		hideCapture: true,
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"capture-atomic",
		templateQuotaBuild("capture-atomic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, ErrUnknown) {
		t.Fatal(err)
	}
	public, _ := s.store.operation(s.ctx, o.ID)
	original := encode(public)
	table, event, condition := "quota_resources", "INSERT", "NEW.kind='snapshot'"
	switch target {
	case "owner":
		table = "snapshot_owners"
		condition = "1"
	case "reader":
		table = "quota_refs"
		condition = "NEW.kind='snapshot' AND NEW.holder_kind='engine_dependency'"
	case "resolution":
		table = "resource_recovery"
		event = "UPDATE"
		condition = "json_extract(NEW.body,'$.kind')='allocation_group' AND json_extract(NEW.body,'$.allocations[1].resolution.state')='result_known'"
	}
	templateQuotaSQLFault(t, s, table, event, condition, fault)
	before := templateQuotaState(t, s)
	e.mu.Lock()
	e.hideCapture = false
	e.mu.Unlock()
	err = s.recoverResources()
	templateQuotaFaultObserved(t, s, fault, err)
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal("known SID/owner/reader/Resolution partially adopted")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	if _, err = s.store.db.Exec(`DROP TRIGGER handoff_fault`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	if f.count("create") != 1 || f.count("snapshot") != 1 ||
		f.count("delete") != 1 {
		t.Fatal("receipt recovery repeated physical effects")
	}
	now, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(original, encode(now)) {
		t.Fatal("capture failure public bytes changed")
	}
}

// A setup failure parks a real qualified collector before dispatch. Subsequent
// faults exercise the same persisted business responsibility, with no new key.
func templateQuotaQualifiedFixture(
	t *testing.T,
	subject, point string,
) (*Service, *recoveryTestEngine, *templateQuotaEngine, Principal, Operation, resourceRecovery) {
	t.Helper()
	var s *Service
	var f *recoveryTestEngine
	var p Principal
	var o Operation
	var tpl Template
	if subject == "build" {
		s, f, _, p = recoveryFixture(t)
	} else {
		s, f, p, tpl = templateQuotaGolden(t)
	}
	e := &templateQuotaEngine{Client: f, attempts: map[string]int{}}
	s.opts.Engine = e
	event, condition := "UPDATE", "json_extract(NEW.body,'$.kind')='allocation_cleanup' AND json_extract(NEW.body,'$.allocation_deletion.step.dispatched')=1"
	if point == "collector" {
		event = "INSERT"
		condition = "json_extract(NEW.body,'$.kind')='allocation_cleanup'"
	}
	if _, err := s.store.db.Exec(
		"CREATE TRIGGER qualification_setup BEFORE " + event + " ON resource_recovery WHEN " + condition + " BEGIN SELECT RAISE(ABORT,'qualification setup'); END",
	); err != nil {
		t.Fatal(err)
	}
	if subject == "build" {
		var err error
		o, err = s.Submit(
			s.ctx,
			p,
			"qualified-build",
			templateQuotaBuild("qualified-build"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Wait(s.ctx, p, o.ID); err == nil {
			t.Fatal("setup marker fault missing")
		}
	} else {
		templateQuotaTarget(t, s, p, tpl, 1)
		agent := s.opts.Agent.(*fakeAgent)
		agent.mu.Lock()
		agent.failInfo = true
		agent.mu.Unlock()
		s.pools()
		s.wg.Wait()
		ops, err := s.resourceOperations()
		if err != nil {
			t.Fatal(err)
		}
		for _, candidate := range ops {
			if candidate.Command.Action == "refill" {
				o = candidate
			}
		}
		if o.Status != "failed" {
			t.Fatal("refill did not fail before ready publication")
		}
		templateQuotaTarget(t, s, p, tpl, 0)
	}
	err := s.recoverResources()
	if err == nil ||
		!strings.Contains(privateDiagnostic(err), "qualification setup") {
		t.Fatal("qualified setup not reached", err)
	}
	if _, err = s.store.db.Exec(
		`DROP TRIGGER qualification_setup`,
	); err != nil {
		t.Fatal(err)
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	return s, f, e, p, o, r
}

func templateQuotaQualifiedFault(t *testing.T, subject, point, fault string) {
	setup := point
	if point != "collector" {
		setup = "marker"
	}
	s, f, e, p, o, r := templateQuotaQualifiedFixture(t, subject, setup)
	public, _ := s.store.operation(s.ctx, o.ID)
	original := encode(public)
	table, event, condition := "resource_recovery", "INSERT", "json_extract(NEW.body,'$.kind')='allocation_cleanup'"
	if point == "marker" {
		event = "UPDATE"
		condition = "json_extract(NEW.body,'$.kind')='allocation_cleanup' AND json_extract(NEW.body,'$.allocation_deletion.step.dispatched')=1"
	}
	retirement := point != "collector" && point != "marker"
	if retirement {
		switch point {
		case "slot":
			table = "quota_resources"
			event = "DELETE"
			condition = "OLD.kind='build_slot'"
		case "backing":
			table = "quota_refs"
			event = "DELETE"
			condition = "OLD.holder_id='backing:" + r.Group.Identity.VM + "'"
		case "resolution":
			event = "UPDATE"
			condition = "NEW.id='" + r.ID + "' AND json_extract(NEW.body,'$.allocations[0].resolution.state')='reclaimed'"
		}
	}
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(gate) }) }
	defer unpark()
	var before []byte
	var err error
	if retirement {
		e.afterDelete = func(api.Request, api.Operation) error {
			close(entered)
			select {
			case <-gate:
				return nil
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
		}
		done := make(chan error, 1)
		go func() { done <- s.recoverResources() }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("qualified physical deletion gate missing")
		}
		templateQuotaSQLFault(t, s, table, event, condition, fault)
		before = templateQuotaState(t, s)
		unpark()
		select {
		case err = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("qualified cleanup did not return")
		}
	} else {
		templateQuotaSQLFault(t, s, table, event, condition, fault)
		before = templateQuotaState(t, s)
		err = s.recoverResources()
	}
	templateQuotaFaultObserved(t, s, fault, err)
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatalf(
			"qualified %s/%s partially transferred responsibility",
			subject,
			point,
		)
	}
	if !retirement {
		e.mu.Lock()
		attempts := e.attempts["delete"]
		e.mu.Unlock()
		if attempts != 0 {
			t.Fatal(
				"delete dispatched without its collector/marker transaction",
			)
		}
	}
	if subject == "build" {
		ledgerMoney(t, s, p.TenantID, 1, 256)
	} else {
		ledgerMoney(t, s, p.TenantID, 0, 256)
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER handoff_fault`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	storage := int64(0)
	if subject == "warm" {
		storage = 192
	}
	ledgerMoney(t, s, p.TenantID, 0, storage)
	e.mu.Lock()
	attempts := e.attempts["delete"]
	e.mu.Unlock()
	if attempts != 1 {
		t.Fatal("cleanup repeated physical delete or changed key")
	}
	now, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(original, encode(now)) {
		t.Fatal("qualified cleanup changed public failure")
	}
	_ = f
}

func templateQuotaLegacyReaderFault(t *testing.T, fault string) {
	s, f, p, tpl := templateQuotaGolden(t)
	tpl = templateQuotaTarget(t, s, p, tpl, 1)
	s.pools()
	s.wg.Wait()
	claim := ledgerExecute(
		t,
		s,
		p,
		"legacy-active",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	o, err := s.store.byKey(s.ctx, p.TenantID, "legacy-active")
	if err != nil {
		t.Fatal(err)
	}
	public := encode(o)
	// This is the valid active destination left by d3, with its original reader.
	if _, err = s.store.db.Exec(
		`INSERT INTO quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) VALUES(?,'snapshot',?,'engine_dependency',?)`,
		p.TenantID,
		tpl.SnapshotID,
		"operation:"+o.ID,
	); err != nil {
		t.Fatal(err)
	}
	// An actual unknown successor clone independently keeps its source reader.
	e := &templateQuotaEngine{
		Client:    f,
		attempts:  map[string]int{},
		hideClone: true,
	}
	s.opts.Engine = e
	s.pools()
	s.wg.Wait()
	templateQuotaSQLFault(
		t,
		s,
		"quota_refs",
		"DELETE",
		"OLD.holder_kind='engine_dependency' AND OLD.holder_id='operation:"+o.ID+"'",
		fault,
	)
	before := templateQuotaState(t, s)
	err = s.recoverResources()
	templateQuotaFaultObserved(t, s, fault, err)
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal(
			"active reader fault changed historical destination or another reader",
		)
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER handoff_fault`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	var readers int
	if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency'`, tpl.SnapshotID).
		Scan(&readers); err != nil ||
		readers != 1 {
		t.Fatal(
			"retirement removed an independent unknown reader",
			readers,
			err,
		)
	}
	now, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(public, encode(now)) {
		t.Fatal("active public bytes changed")
	}
	templateQuotaTarget(t, s, p, tpl, 0)
	e.mu.Lock()
	e.hideClone = false
	e.mu.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerExecute(
		t,
		s,
		p,
		"legacy-release",
		Command{Action: "release", SandboxID: claim.Sandbox.ID},
	)
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
}

func templateQuotaPartialQualifiedRetirement(t *testing.T) {
	s, f, e, p, _, r := templateQuotaQualifiedFixture(t, "warm", "marker")
	i := r.Group.Identity
	// An independent, real restore creates another physical disk version. Its
	// conservative fee is retained with the same VM until actual absence.
	restored, err := api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "external-version",
			Action:           "restore",
			VMID:             i.VM,
			SnapshotID:       r.Group.Template.SnapshotID,
			ExpectedInstance: i.Instance,
		},
	)
	if err != nil || restored.ResultInstance == "" ||
		restored.ResultInstance == i.Instance {
		t.Fatal("external version not produced", err)
	}
	version := i.VM + ":restore:" + restored.ID
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = putQuotaResource(
		s.ctx,
		tx,
		quotaResource{
			Tenant:  p.TenantID,
			Kind:    "vm_disk",
			Key:     version,
			Storage: 64,
			Source:  r.Source,
		},
		quotaRef{Kind: "cleanup", ID: "vm:" + i.VM},
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "external-version-delete",
			Action:           "delete",
			VMID:             i.VM,
			ExpectedInstance: restored.ResultInstance,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.db.Exec(
		"CREATE TRIGGER partial_version BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk' AND OLD.resource_key='" + version + "' BEGIN SELECT RAISE(IGNORE); END",
	); err != nil {
		t.Fatal(err)
	}
	before := templateQuotaState(t, s)
	err = s.recoverResources()
	if !isCode(err, ErrManualRecovery) ||
		!strings.Contains(err.Error(), "not fully retired") {
		t.Fatal("partial multirow deletion was accepted", err)
	}
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal("partial IGNORE retired the original disk or backing")
	}
	ledgerMoney(t, s, p.TenantID, 0, 320)
	e.mu.Lock()
	deletes := e.attempts["delete"]
	e.mu.Unlock()
	if deletes != 0 {
		t.Fatal("Service attempted deletion of external generation")
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER partial_version`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
}

func templateQuotaWithoutStepProgress(t *testing.T, s *Service) []byte {
	t.Helper()
	var state map[string][]string
	if err := json.Unmarshal(
		templateQuotaWithoutOperations(t, s),
		&state,
	); err != nil {
		t.Fatal(err)
	}
	for n, raw := range state["resource_recovery"] {
		var row []any
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			t.Fatal(err)
		}
		var r resourceRecovery
		var body []byte
		if err := json.Unmarshal(encode(row[1]), &body); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body, &r); err != nil {
			t.Fatal(err)
		}
		if r.Group != nil {
			r.Group.Steps = nil
		}
		row[1] = encode(r)
		state["resource_recovery"][n] = string(encode(row))
	}
	sort.Strings(state["resource_recovery"])
	return encode(state)
}

func templateQuotaSelectionPublicationFault(t *testing.T, fault string) {
	s, f, p, tpl := templateQuotaGolden(t)
	tpl = templateQuotaTarget(t, s, p, tpl, 1)
	s.pools()
	s.wg.Wait()
	// Keep the admitted operation pending while exercising the complete warm
	// activation boundary, so its transaction can be inspected before worker
	// compensation begins. Engine, Agent and SQLite remain the real fixture flow.
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	o, err := s.Submit(
		s.ctx,
		p,
		"active-reader-transaction",
		Command{Action: "claim", TemplateID: tpl.ID},
	)
	s.mu.Lock()
	s.closing = false
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	b := recoverySandbox(t, s, o.IDs[0])
	memory := encode(b)
	templateQuotaSQLFault(
		t,
		s,
		"quota_refs",
		"DELETE",
		"OLD.holder_kind='engine_dependency' AND OLD.holder_id='operation:"+o.ID+"'",
		fault,
	)
	before := templateQuotaWithoutStepProgress(t, s)
	err = s.activateWarm(o, &b)
	templateQuotaFaultObserved(t, s, fault, err)
	if !bytes.Equal(memory, encode(b)) {
		t.Fatal(
			"uncommitted running metadata escaped through the staged Sandbox",
		)
	}
	if !bytes.Equal(before, templateQuotaWithoutStepProgress(t, s)) {
		t.Fatal(
			"active transaction partially changed metadata, holder, destination or fee",
		)
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	if r.Done || r.Group.Selection.Stage != "selected" {
		t.Fatal("active stage escaped rollback")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	if _, err = s.store.db.Exec(`DROP TRIGGER handoff_fault`); err != nil {
		t.Fatal(err)
	}
	// The original nonterminal intent continues with the original Engine keys.
	s.run(o)
	if _, err = s.Wait(s.ctx, p, o.ID); err != nil {
		t.Fatal(err)
	}
	public, _ := s.store.operation(s.ctx, o.ID)
	original := encode(public)
	if f.count("clone") != 1 {
		t.Fatal("publication retry allocated a replacement")
	}
	templateQuotaTarget(t, s, p, tpl, 0)
	ledgerExecute(
		t,
		s,
		p,
		"active-reader-release",
		Command{Action: "release", SandboxID: b.ID},
	)
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	now, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(original, encode(now)) {
		t.Fatal("successful public bytes changed during cleanup")
	}
}

func TestTemplateQuotaSQLHandoffRevision(t *testing.T) {
	for _, fault := range []string{"abort", "ignore", "commit"} {
		for _, target := range []string{"resource", "owner", "reader", "resolution"} {
			t.Run(
				"capture-"+target+"/"+fault,
				func(t *testing.T) { templateQuotaCaptureFault(t, target, fault) },
			)
		}
		for _, subject := range []string{"build", "warm"} {
			for _, point := range []string{"collector", "marker", "resolution"} {
				t.Run(
					subject+"-"+point+"/"+fault,
					func(t *testing.T) { templateQuotaQualifiedFault(t, subject, point, fault) },
				)
			}
		}
		t.Run(
			"build-slot/"+fault,
			func(t *testing.T) { templateQuotaQualifiedFault(t, "build", "slot", fault) },
		)
		t.Run(
			"warm-backing/"+fault,
			func(t *testing.T) { templateQuotaQualifiedFault(t, "warm", "backing", fault) },
		)
		t.Run(
			"legacy-active-reader/"+fault,
			func(t *testing.T) { templateQuotaLegacyReaderFault(t, fault) },
		)
		t.Run(
			"active-reader-publication/"+fault,
			func(t *testing.T) { templateQuotaSelectionPublicationFault(t, fault) },
		)
	}
	t.Run(
		"qualified-partial-multirow-ignore",
		templateQuotaPartialQualifiedRetirement,
	)
}
