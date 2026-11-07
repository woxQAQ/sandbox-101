package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// The first create boundary leaves admission observable while another template
// competes for the same tenant budget. The gate never holds the engine mutex.
type templateQuotaEngine struct {
	api.Client

	mu                  sync.Mutex
	entered             chan struct{}
	gate                chan struct{}
	gateAction          string
	first               bool
	attempts            map[string]int
	afterDelete         func(api.Request, api.Operation) error
	afterCreate         func(api.Request, api.Operation) error
	failCloneEmpty      bool
	failStart           bool
	snapshotEntered     chan struct{}
	snapshotGate        <-chan struct{}
	snapshotOnce        bool
	queryEntered        chan struct{}
	queryGate           <-chan struct{}
	queryOnce           bool
	hideCapture         bool
	missingCloneWitness bool
	hideStart           bool
	hideClone           bool
	capture             *api.Operation
}

func templateQuotaCopy[T any](value T) T {
	var copy T
	if err := json.Unmarshal(encode(value), &copy); err != nil {
		panic(err)
	}
	return copy
}

func (e *templateQuotaEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	e.mu.Lock()
	e.attempts[r.Action]++
	action := e.gateAction
	if action == "" {
		action = "create"
	}
	park := r.Action == action && !e.first && e.gate != nil
	if park {
		e.first = true
		close(e.entered)
	}
	e.mu.Unlock()
	if park {
		select {
		case <-ctx.Done():
			return api.Operation{}, ctx.Err()
		case <-e.gate:
		}
	}
	if r.Action == "delete" && r.ExpectedInstance == "" {
		return api.Operation{}, api.Fail(
			api.Invalid,
			"strict fixture requires nonempty delete witness",
		)
	}
	e.mu.Lock()
	failStart := e.failStart
	e.mu.Unlock()
	e.mu.Lock()
	failEmpty := e.failCloneEmpty
	e.mu.Unlock()
	if r.Action == "clone" && failEmpty {
		f := e.Client.(*recoveryTestEngine)
		f.mu.Lock()
		o, ok := f.ops[r.Key]
		if !ok {
			o = api.Operation{
				ID:      r.Key,
				Key:     r.Key,
				Request: r,
				Status:  "failed",
				Error: &api.Error{
					Code:    api.Internal,
					Message: "controlled clone failure before allocation",
				},
			}
			f.ops[r.Key] = o
			f.calls[r.Action]++
		}
		f.mu.Unlock()
		return o, nil
	}
	if r.Action == "start" && failStart {
		f := e.Client.(*recoveryTestEngine)
		f.mu.Lock()
		o, ok := f.ops[r.Key]
		if !ok {
			o = api.Operation{
				ID:      r.Key,
				Key:     r.Key,
				Request: r,
				VMID:    r.VMID,
				Status:  "failed",
				Error: &api.Error{
					Code:    api.Internal,
					Message: "controlled start failure",
				},
			}
			f.ops[r.Key] = o
			f.calls[r.Action]++
		}
		f.mu.Unlock()
		return o, nil
	}
	proof, err := e.Client.Submit(ctx, templateQuotaCopy(r))
	proof = templateQuotaCopy(proof)
	if err == nil && r.Action == "delete" && e.afterDelete != nil {
		if hookErr := e.afterDelete(r, proof); hookErr != nil {
			return api.Operation{}, hookErr
		}
	}
	if err == nil && r.Action == "create" && e.afterCreate != nil {
		if hookErr := e.afterCreate(r, proof); hookErr != nil {
			return api.Operation{}, hookErr
		}
	}
	e.mu.Lock()
	missing := e.missingCloneWitness && r.Action == "clone"
	e.mu.Unlock()
	if missing && err == nil {
		proof.ResultInstance = ""
		f := e.Client.(*recoveryTestEngine)
		f.mu.Lock()
		f.ops[r.Key] = proof
		f.mu.Unlock()
	}
	e.mu.Lock()
	hidden := (e.hideCapture && r.Action == "snapshot") ||
		(e.hideClone && r.Action == "clone") ||
		(e.hideStart && r.Action == "start")
	if hidden {
		copy := proof
		e.capture = &copy
	}
	e.mu.Unlock()
	if hidden {
		return api.Operation{}, api.Fail(api.Unknown, "capture reply lost")
	}
	return proof, err
}

func (e *templateQuotaEngine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	e.mu.Lock()
	hidden := (e.hideCapture || e.hideClone || e.hideStart) &&
		e.capture != nil &&
		e.capture.Key == key
	e.mu.Unlock()
	if hidden {
		return api.Operation{}, api.Fail(api.NotFound, "receipt not visible")
	}
	proof, err := e.Client.FindOperation(ctx, key)
	return templateQuotaCopy(proof), err
}

func (e *templateQuotaEngine) GetOperation(
	ctx context.Context,
	id string,
) (api.Operation, error) {
	proof, err := e.Client.GetOperation(ctx, id)
	return templateQuotaCopy(proof), err
}

func (e *templateQuotaEngine) GetSnapshot(
	ctx context.Context,
	id string,
) (api.Snapshot, error) {
	snapshot, err := e.Client.GetSnapshot(ctx, id)
	e.mu.Lock()
	park := e.snapshotGate != nil && !e.snapshotOnce
	gate := e.snapshotGate
	if park {
		e.snapshotOnce = true
		close(e.snapshotEntered)
	}
	e.mu.Unlock()
	if park {
		select {
		case <-ctx.Done():
			return api.Snapshot{}, ctx.Err()
		case <-gate:
		}
	}
	return templateQuotaCopy(snapshot), err
}

func (e *templateQuotaEngine) GetVM(
	ctx context.Context,
	id string,
) (api.VM, error) {
	v, err := e.Client.GetVM(ctx, id)
	e.mu.Lock()
	gate := e.queryGate
	park := gate != nil && !e.queryOnce
	if park {
		e.queryOnce = true
		close(e.queryEntered)
	}
	e.mu.Unlock()
	if park {
		select {
		case <-ctx.Done():
			return api.VM{}, ctx.Err()
		case <-gate:
		}
	}
	var copy api.VM
	if err == nil {
		err = json.Unmarshal(encode(v), &copy)
	}
	return copy, err
}

func templateQuotaBuild(name string) Command {
	return Command{Action: "template_build", Template: &Template{
		ID:    name,
		Image: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}}
}

// Ready fixture allocations carry real Engine create/start receipts; no user
// sandbox is introduced for the preexisting cold-pool control case.
func seedTemplateQuotaWarm(
	t *testing.T,
	s *Service,
	template Template,
	created, started api.Operation,
) {
	t.Helper()
	o := Operation{
		ID:             "fixture-warm",
		Key:            "fixture-warm",
		TenantID:       template.TenantID,
		ChargeTenantID: template.TenantID,
		Command: Command{
			Action:     "refill",
			TemplateID: template.ID,
			Template:   &template,
		},
		Status:     "succeeded",
		CreatedAt:  time.Now().UTC(),
		FinishedAt: time.Now().UTC(),
	}
	o.Hash = s.store.sign(o.ID)
	r := newAllocationGroup(
		o,
		allocationSubject{Kind: "warm", ID: o.ID},
		template,
	)
	r.Group.Identity = &allocationIdentity{
		VM:         created.VMID,
		Owner:      template.TenantID,
		Shape:      template.Shape,
		Source:     o.ID,
		Receipt:    created,
		Instance:   started.ResultInstance,
		Generation: &started,
	}
	r.Allocations = []recoveryAllocation{
		{
			OperationID:    o.ID,
			TenantID:       o.TenantID,
			ChargeTenantID: o.ChargeTenantID,
			Subject:        r.Group.Subject,
			Shape:          template.Shape,
			Step:           "warm-clone",
			Kind:           "vm_disk",
			Request:        created.Request,
			RequestBound:   true,
			Resolution: allocationResolution{
				State:       "active",
				Proof:       &created,
				Destination: "warm:" + created.VMID,
			},
		},
	}
	r.Allocations[0].Step = "fixture-create"
	r.Group.Steps["fixture-create"] = allocationStep{
		Request:    created.Request,
		Bound:      true,
		Dispatched: true,
		Proof:      &created,
	}
	r.Group.Steps["fixture-start"] = allocationStep{
		Request:    started.Request,
		Bound:      true,
		Dispatched: true,
		Proof:      &started,
	}
	r.Done = true
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err = putQuotaResource(
		s.ctx,
		tx,
		quotaResource{
			Tenant:  template.TenantID,
			Kind:    "vm_disk",
			Key:     created.VMID,
			Storage: template.Shape.DiskMiB,
			Source:  o.ID,
		},
		quotaRef{Kind: "active_use", ID: "warm:" + created.VMID},
	); err != nil {
		t.Fatal(err)
	}
	if err = putQuotaRef(
		s.ctx,
		tx,
		template.TenantID,
		"vm_disk",
		created.VMID,
		quotaRef{Kind: "cleanup", ID: "vm:" + created.VMID},
	); err != nil {
		t.Fatal(err)
	}
	if template.SnapshotID != "" {
		if err = putQuotaRef(
			s.ctx,
			tx,
			template.TenantID,
			"snapshot",
			template.SnapshotID,
			quotaRef{Kind: "active_use", ID: "backing:" + created.VMID},
		); err != nil {
			t.Fatal(err)
		}
	}
	if err = saveOp(s.ctx, tx, o); err != nil {
		t.Fatal(err)
	}
	if err = insertRecovery(s.ctx, tx, r); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`INSERT INTO warm(vm_id,template_id,snapshot_id,created_at) VALUES(?,?,?,?)`,
		created.VMID,
		template.ID,
		template.SnapshotID,
		time.Now().UnixNano(),
	); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTemplateQuotaBuildAdmission(t *testing.T) {
	for _, fault := range []string{"abort", "ignore", "commit"} {
		for _, target := range []string{"slot", "group", "operation"} {
			t.Run(
				"admission-"+target+"-"+fault,
				func(t *testing.T) { templateQuotaAdmissionFault(t, target, fault) },
			)
		}
	}
	for _, fault := range []string{"abort", "ignore", "commit"} {
		t.Run(
			"known-"+fault,
			func(t *testing.T) { templateQuotaHandoffFault(t, "known", fault) },
		)
	}
	t.Run("late-original-start-success", func(t *testing.T) {
		s, f, _, p := recoveryFixture(t)
		e := &templateQuotaEngine{
			Client:    f,
			attempts:  map[string]int{},
			hideStart: true,
		}
		s.opts.Engine = e
		o, err := s.Submit(
			s.ctx,
			p,
			"late-start",
			templateQuotaBuild("late-start"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, api.Unknown) {
			t.Fatal("start reply was not unknown", err)
		}
		original, _ := s.store.operation(s.ctx, o.ID)
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 1, 64)
		e.mu.Lock()
		e.hideStart = false
		e.mu.Unlock()
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 0)
		current, _ := s.store.operation(s.ctx, o.ID)
		if !bytes.Equal(encode(original), encode(current)) {
			t.Fatal("late receipt changed public failure")
		}
		if f.count("start") != 1 || f.count("snapshot") != 0 {
			t.Fatal("cleanup resumed guest lifecycle")
		}
	})
	t.Run("nonterminal-original-start-reopen", templateQuotaNonterminalStart)
	t.Run("manual-root-full-fee", func(t *testing.T) {
		s, _, _, p := recoveryFixture(t)
		ledgerLimit(t, s, p, 1, 1)
		ledgerExecute(
			t,
			s,
			Principal{Root: true, TenantID: p.TenantID},
			"manual-root",
			templateQuotaBuild("manual-root"),
		)
		ledgerMoney(t, s, p.TenantID, 0, 192)
	})
	t.Run("witnessless-external-generation", templateQuotaWitnessless)
	t.Run(
		"unknown-start-absence-retains-slot",
		templateQuotaUnknownStartAbsence,
	)
	t.Run(
		"retired-build-publication-reader",
		templateQuotaBuildPublicationReader,
	)
	t.Run("unknown-capture-absence", templateQuotaUnknownCapture)
	for _, fault := range []string{"abort", "ignore", "commit"} {
		t.Run(
			"absence-"+fault,
			func(t *testing.T) { templateQuotaAbsenceFault(t, fault) },
		)
	}
	for _, limit := range []struct {
		name    string
		slots   int
		storage int64
	}{
		{"slots", 1, 512}, {"storage", 2, 511},
	} {
		t.Run(limit.name, func(t *testing.T) {
			s, f, _, p := recoveryFixture(t)
			ledgerLimit(t, s, p, limit.slots, limit.storage)
			e := &templateQuotaEngine{
				Client:   f,
				entered:  make(chan struct{}),
				gate:     make(chan struct{}),
				attempts: map[string]int{},
			}
			s.opts.Engine = e
			var once sync.Once
			release := func() { once.Do(func() { close(e.gate) }) }
			defer release()
			o, err := s.Submit(
				s.ctx,
				p,
				"first",
				templateQuotaBuild("build-one"),
			)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-e.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("first build did not reach create")
			}
			_, err = s.Submit(
				s.ctx,
				p,
				"second",
				templateQuotaBuild("build-two"),
			)
			if !isCode(err, api.Exhausted) {
				t.Errorf(
					"second distinct template must be refused before Engine: %v",
					err,
				)
			}
			e.mu.Lock()
			attempts := e.attempts["create"]
			e.mu.Unlock()
			if attempts != 1 {
				t.Errorf("create attempts=%d, want first build only", attempts)
			}
			ledgerMoney(t, s, p.TenantID, 1, 256)
			release()
			ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			defer cancel()
			if _, err = s.Wait(ctx, p, o.ID); err != nil {
				t.Fatal(err)
			}
			ledgerMoney(t, s, p.TenantID, 0, 192)
		})
	}
	t.Run("low-one-no-effects", func(t *testing.T) {
		s, f, a, p := recoveryFixture(t)
		ledgerLimit(t, s, p, 1, 255)
		_, err := s.Submit(s.ctx, p, "low", templateQuotaBuild("low"))
		if !isCode(err, api.Exhausted) {
			t.Fatalf("255 MiB build must be refused: %v", err)
		}
		if f.count("create") != 0 || f.count("start") != 0 {
			t.Fatal("rejected build reached engine")
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		if len(a.activations) != 0 {
			t.Fatal("rejected build reached agent")
		}
	})
}

func templateQuotaNonterminalStart(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	e := &templateQuotaEngine{
		Client:     f,
		attempts:   map[string]int{},
		entered:    make(chan struct{}),
		gate:       make(chan struct{}),
		gateAction: "start",
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"nonterminal",
		templateQuotaBuild("nonterminal"),
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("start dispatch did not park")
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	e.mu.Lock()
	e.gate = nil
	e.mu.Unlock()
	s, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(ctx, p, o.ID); err != nil {
		t.Fatal("lawful original start was overblocked", err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	if f.count("create") != 1 || f.count("start") != 1 {
		t.Fatal("reopen allocated another VM or start identity")
	}
}

func templateQuotaAdmissionFault(t *testing.T, target, fault string) {
	s, f, _, p := recoveryFixture(t)
	table, condition := "quota_resources", "NEW.kind='build_slot'"
	if target == "group" {
		table = "resource_recovery"
		condition = "json_extract(NEW.body,'$.kind')='allocation_group'"
	}
	if target == "operation" {
		table = "operations"
		condition = "json_extract(NEW.body,'$.command.action')='template_build'"
	}
	body := "SELECT RAISE(ABORT,'admission fault');"
	timing := "BEFORE"
	if fault == "ignore" {
		body = "SELECT RAISE(IGNORE);"
	}
	if fault == "commit" {
		if _, err := s.store.db.Exec(
			`CREATE TABLE admit_parent(id INTEGER PRIMARY KEY); CREATE TABLE admit_fault(id INTEGER REFERENCES admit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		); err != nil {
			t.Fatal(err)
		}
		timing = "AFTER"
		body = "INSERT INTO admit_fault VALUES(1);"
	}
	if _, err := s.store.db.Exec(
		"CREATE TRIGGER admit_fault_trigger " + timing + " INSERT ON " + table + " WHEN " + condition + " BEGIN " + body + " END",
	); err != nil {
		t.Fatal(err)
	}
	before := templateQuotaState(t, s)
	_, err := s.Submit(
		s.ctx,
		p,
		"faulted-build",
		templateQuotaBuild("faulted-build"),
	)
	if err == nil {
		t.Fatal("admission fault missing")
	}
	if fault == "commit" &&
		(!strings.Contains(err.Error(), "operation admission commit") || !strings.Contains(err.Error(), "FOREIGN KEY")) {
		t.Fatal("not an actual admission COMMIT error", err)
	}
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal("failed admission left partial operation, intent or ledger")
	}
	if f.count("create") != 0 || f.count("start") != 0 {
		t.Fatal("failed admission dispatched Engine")
	}
	if _, err = s.store.db.Exec(
		`DROP TRIGGER admit_fault_trigger`,
	); err != nil {
		t.Fatal(err)
	}
	ledgerExecute(t, s, p, "faulted-build", templateQuotaBuild("faulted-build"))
	ledgerMoney(t, s, p.TenantID, 0, 192)
}

func templateQuotaFailedBuild(
	t *testing.T,
	beforeStart bool,
) (*Service, *recoveryTestEngine, *templateQuotaEngine, Principal, Operation) {
	t.Helper()
	s, f, _, p := recoveryFixture(t)
	e := &templateQuotaEngine{
		Client:    f,
		attempts:  map[string]int{},
		failStart: !beforeStart,
	}
	if beforeStart {
		e.afterCreate = func(request api.Request, _ api.Operation) error {
			o, err := s.store.operation(
				s.ctx,
				request.Config.Labels["operation_id"],
			)
			if err != nil {
				return err
			}
			o.Status = "failed"
			o.Error = &api.Error{
				Code:    api.Internal,
				Message: "controlled public failure before start dispatch",
			}
			o.FinishedAt = time.Now().UTC()
			return saveOp(s.ctx, s.store.db, o)
		}
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"witnessless",
		templateQuotaBuild("witnessless"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(ctx, p, o.ID); err == nil {
		t.Fatal("controlled build failure missing")
	}
	// Wait also for the worker, since a forced public failure can precede its return.
	s.mu.Lock()
	done := s.active[o.ID]
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	o, err = s.store.operation(s.ctx, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	return s, f, e, p, o
}

func templateQuotaWitnessless(t *testing.T) {
	for _, beforeStart := range []bool{true, false} {
		t.Run(fmt.Sprint(beforeStart), func(t *testing.T) {
			s, f, e, p, o := templateQuotaFailedBuild(t, beforeStart)
			original := encode(o)
			for n := 0; n < 2; n++ {
				if err := s.recoverResources(); err != nil {
					t.Fatal(err)
				}
			}
			ledgerMoney(t, s, p.TenantID, 1, 64)
			r, err := s.loadRecovery(allocationGroupID(o))
			if err != nil {
				t.Fatal(err)
			}
			if r.Group.Gate == nil || r.Done || r.Group.Identity == nil ||
				r.Group.Identity.Instance != "" {
				t.Fatalf("missing observation responsibility: %+v", r)
			}
			vmid := r.Group.Identity.VM
			s = ledgerRestart(t, s)
			entered := make(chan struct{})
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
			e.mu.Lock()
			e.queryEntered = entered
			e.queryGate = gate
			e.queryOnce = false
			e.mu.Unlock()
			result := make(chan error, 1)
			go func() { result <- s.recoverResources() }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("absence observer did not query actual VM")
			}
			c, err := api.Wait(
				context.Background(),
				f,
				api.Request{
					Key:    "independent-start-C",
					Action: "start",
					VMID:   vmid,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if c.ResultInstance == "" {
				t.Fatal("external generation has no immutable witness")
			}
			release()
			if err = <-result; err != nil {
				t.Fatal(err)
			}
			current, err := f.GetVM(s.ctx, vmid)
			if err != nil || current.Instance != c.ResultInstance {
				t.Fatal("external C was changed", err)
			}
			ledgerMoney(t, s, p.TenantID, 1, 64)
			e.mu.Lock()
			attempts := e.attempts["delete"]
			starts := e.attempts["start"]
			e.mu.Unlock()
			wantStarts := 1
			if beforeStart {
				wantStarts = 0
			}
			if attempts != 0 || starts != wantStarts {
				t.Fatalf(
					"Service attempts delete=%d start=%d",
					attempts,
					starts,
				)
			}
			if _, err = api.Wait(
				s.ctx,
				f,
				api.Request{
					Key:              "independent-delete-C",
					Action:           "delete",
					VMID:             vmid,
					ExpectedInstance: c.ResultInstance,
				},
			); err != nil {
				t.Fatal(err)
			}
			for n := 0; n < 3; n++ {
				if err = s.recoverResources(); err != nil {
					t.Fatal(err)
				}
			}
			ledgerMoney(t, s, p.TenantID, 0, 0)
			r, err = s.loadRecovery(r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !r.Done || r.Group.Gate != nil ||
				r.Allocations[0].Resolution.State != "reclaimed" ||
				r.Allocations[0].Resolution.Destination != "vm-absent:"+vmid {
				t.Fatal("absence destination was not committed")
			}
			public, _ := s.store.operation(s.ctx, o.ID)
			if !bytes.Equal(original, encode(public)) {
				t.Fatal("failed public result changed")
			}
			e.mu.Lock()
			attempts = e.attempts["delete"]
			e.mu.Unlock()
			if attempts != 0 {
				t.Fatal("Service attempted a physical delete")
			}
			t.Logf(
				"D1 beforeStart=%v externalB=%s ServiceDeleteAttempts=%d destination=%s",
				beforeStart,
				c.ResultInstance,
				attempts,
				r.Allocations[0].Resolution.Destination,
			)
		})
	}
}

func templateQuotaState(t *testing.T, s *Service) []byte {
	t.Helper()
	state := map[string][]string{}
	for _, table := range []string{"quota_resources", "quota_refs", "resource_recovery", "sandboxes", "templates", "warm", "snapshot_owners", "operations", "template_intents"} {
		rows, err := s.store.db.QueryContext(s.ctx, "SELECT * FROM "+table)
		if err != nil {
			t.Fatal(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(cols))
			pointers := make([]any, len(cols))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err = rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			state[table] = append(state[table], string(encode(values)))
		}
		if err = errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
		sort.Strings(state[table])
	}
	return encode(state)
}

func templateQuotaAbsenceFault(t *testing.T, fault string) {
	s, f, _, p, o := templateQuotaFailedBuild(t, true)
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	v := r.Group.Identity.VM
	started, err := api.Wait(
		s.ctx,
		f,
		api.Request{Key: "external-fault-start", Action: "start", VMID: v},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "external-fault-delete",
			Action:           "delete",
			VMID:             v,
			ExpectedInstance: started.ResultInstance,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`CREATE TEMP TABLE connection_marker(value INTEGER); INSERT INTO connection_marker VALUES(1)`,
	); err != nil {
		t.Fatal(err)
	}
	trigger := `CREATE TRIGGER absence_fault BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk' BEGIN SELECT RAISE(ABORT,'absence abort'); END`
	if fault == "ignore" {
		trigger = `CREATE TRIGGER absence_fault BEFORE DELETE ON quota_resources WHEN OLD.kind='vm_disk' BEGIN SELECT RAISE(IGNORE); END`
	}
	if fault == "commit" {
		if _, err = s.store.db.ExecContext(
			s.ctx,
			`CREATE TABLE commit_parent(id INTEGER PRIMARY KEY); CREATE TABLE commit_fault(id INTEGER REFERENCES commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		); err != nil {
			t.Fatal(err)
		}
		trigger = `CREATE TRIGGER absence_fault AFTER DELETE ON quota_resources WHEN OLD.kind='vm_disk' BEGIN INSERT INTO commit_fault VALUES(1); END`
	}
	if _, err = s.store.db.ExecContext(s.ctx, trigger); err != nil {
		t.Fatal(err)
	}
	before := templateQuotaState(t, s)
	err = s.recoverResources()
	if err == nil {
		t.Fatal("absence fault was not observed")
	}
	if fault == "commit" &&
		(!strings.Contains(err.Error(), "allocation absence commit") || !strings.Contains(err.Error(), "FOREIGN KEY")) {
		t.Fatalf("not an actual deferred COMMIT error: %v", err)
	}
	t.Logf("absence %s actual error: %v", fault, err)
	if !bytes.Equal(before, templateQuotaState(t, s)) {
		t.Fatal(
			"absence fault changed durable responsibility or public metadata",
		)
	}
	ledgerMoney(t, s, p.TenantID, 1, 64)
	var marker int
	if err = s.store.db.QueryRowContext(s.ctx, `SELECT value FROM connection_marker`).
		Scan(&marker); err != nil ||
		marker != 1 {
		t.Fatal("same connection unavailable", err)
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(
		s.ctx,
		`UPDATE connection_marker SET value=2`,
	); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.db.ExecContext(
		s.ctx,
		`DROP TRIGGER absence_fault`,
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
}

func templateQuotaBuildPublicationReader(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	e := &templateQuotaEngine{
		Client:          f,
		attempts:        map[string]int{},
		snapshotEntered: make(chan struct{}),
		snapshotGate:    gate,
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"publication-reader",
		templateQuotaBuild("publication-reader"),
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-e.snapshotEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("build did not reach final snapshot query")
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	sid := r.Allocations[1].Resolution.Proof.SnapshotID
	var dependency int
	if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?`, p.TenantID, sid, "operation:"+o.ID).
		Scan(&dependency); err != nil ||
		dependency != 1 {
		t.Fatal("VM retirement dropped the pending publication reader", err)
	}
	intents, err := s.resourceRecoveries()
	if err != nil {
		t.Fatal(err)
	}
	for _, collector := range intents {
		if collector.Kind == "retired_snapshot" && collector.Snapshot == sid {
			if err = s.recoverRetiredSnapshot(&collector); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err = f.GetSnapshot(
		s.ctx,
		sid,
	); err != nil ||
		f.count("delete_snapshot") != 0 {
		t.Fatal("collector deleted a build product before publication", err)
	}
	release()
	if _, err = s.Wait(s.ctx, p, o.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 192)
}

func templateQuotaUnknownStartAbsence(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	e := &templateQuotaEngine{
		Client:    f,
		attempts:  map[string]int{},
		hideStart: true,
	}
	s.opts.Engine = e
	o, err := s.Submit(
		s.ctx,
		p,
		"unknown-start-absence",
		templateQuotaBuild("unknown-start-absence"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, api.Unknown) {
		t.Fatal("original start was not unknown", err)
	}
	original, _ := s.store.operation(s.ctx, o.ID)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	// A different Engine actor can see the immutable original receipt. Service
	// still sees Find NotFound through its controlled transport boundary.
	proof, err := f.FindOperation(s.ctx, stepKey(o, "build-start"))
	if err != nil {
		t.Fatal(err)
	}
	if proof.Status != "succeeded" || proof.ResultInstance == "" {
		t.Fatal("external actor has no legitimate witness")
	}
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "unknown-start-external-delete",
			Action:           "delete",
			VMID:             proof.VMID,
			ExpectedInstance: proof.ResultInstance,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 0)
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	if r.Done || r.Group.Gate == nil ||
		r.Allocations[0].Resolution.State != "reclaimed" ||
		r.Group.Steps["build-start"].Proof != nil {
		t.Fatal("V retirement masked the unresolved original start")
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 0)
	e.mu.Lock()
	e.hideStart = false
	e.mu.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	current, _ := s.store.operation(s.ctx, o.ID)
	if !bytes.Equal(encode(original), encode(current)) {
		t.Fatal("late original receipt changed public failed result")
	}
	e.mu.Lock()
	deletes := e.attempts["delete"]
	starts := e.attempts["start"]
	e.mu.Unlock()
	if deletes != 0 || starts != 1 {
		t.Fatalf("Service delete/start attempts=%d/%d", deletes, starts)
	}
}

func templateQuotaUnknownCapture(t *testing.T) {
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
		"unknown-capture",
		templateQuotaBuild("unknown-capture"),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(ctx, p, o.ID); !isCode(err, api.Unknown) {
		t.Fatal("capture was not unknown", err)
	}
	r, err := s.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	i := r.Group.Identity
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "external-capture-vm-delete",
			Action:           "delete",
			VMID:             i.VM,
			ExpectedInstance: i.Instance,
		},
	); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 192)
	r, err = s.loadRecovery(r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Done || r.Allocations[1].Resolution.State != "unresolved" {
		t.Fatal("VM absence erased unknown SID responsibility")
	}
	e.mu.Lock()
	e.hideCapture = false
	e.mu.Unlock()
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	ledgerMoney(t, s, p.TenantID, 0, 0)
	if f.count("snapshot") != 1 || f.count("create") != 1 {
		t.Fatal("late receipt replay allocated another resource")
	}
}

func TestTemplateQuotaCheckpointReferenceHandoff(t *testing.T) {
	s, _, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	for i := 0; i < 2; i++ {
		ledgerExecute(
			t,
			s,
			p,
			fmt.Sprintf("put-%d", i),
			Command{Action: "template_put", Template: &Template{
				ID: fmt.Sprintf(
					"retained-%d",
					i,
				),
				SnapshotID: cp.SnapshotID,
				Shape:      b.Shape,
			}},
		)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	ledgerExecute(
		t,
		s,
		p,
		"delete-checkpoint",
		Command{Action: "delete_checkpoint", CheckpointID: cp.ID},
	)
	ledgerMoney(t, s, p.TenantID, 1, 256)
	ledgerLimit(t, s, p, 2, 447)
	before, _ := s.store.sandboxes(s.ctx)
	_, err := s.Submit(
		s.ctx,
		p,
		"new-capture",
		Command{Action: "checkpoint", SandboxID: b.ID},
	)
	if !isCode(err, api.Exhausted) {
		t.Fatalf("retained template SID must still consume 192 MiB: %v", err)
	}
	after, _ := s.store.sandboxes(s.ctx)
	if !bytes.Equal(encode(before), encode(after)) {
		t.Fatal("rejected capture changed sandboxes")
	}
	bad := Template{
		ID:         "underreported-shape",
		SnapshotID: cp.SnapshotID,
		Shape:      b.Shape,
	}
	bad.Shape.VCPUs++
	if _, err = s.Submit(
		s.ctx,
		p,
		"shape-mismatch",
		Command{Action: "template_put", Template: &bad},
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatal("same cost accepted a different full shape", err)
	}
	ledgerExecute(
		t,
		s,
		p,
		"release-template-source",
		Command{Action: "release", SandboxID: b.ID},
	)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	s = ledgerRestart(t, s)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	var holders int
	if err = s.store.db.QueryRow(`SELECT count(*) FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id LIKE 'template:%'`, cp.SnapshotID).
		Scan(&holders); err != nil ||
		holders != 2 {
		t.Fatal(
			"shared template holders did not survive source deletion/restart",
			err,
		)
	}
}

func templateQuotaGolden(
	t *testing.T,
) (*Service, *recoveryTestEngine, Principal, Template) {
	t.Helper()
	s, f, _, p := recoveryFixture(t)
	built := ledgerExecute(
		t,
		s,
		p,
		"golden-build",
		templateQuotaBuild("golden-quota"),
	)
	ledgerMoney(t, s, p.TenantID, 0, 192)
	return s, f, p, *built.Template
}

func templateQuotaTarget(
	t *testing.T,
	s *Service,
	p Principal,
	template Template,
	target int,
) Template {
	t.Helper()
	template.TargetWarm = target
	return *ledgerExecute(t, s, Principal{Root: true, TenantID: p.TenantID}, fmt.Sprintf("target-%d-%s", target, newID("")), Command{Action: "template_put", Template: &template}).Template
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
				if !isCode(err, api.Exhausted) || f.count("clone") != before {
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
					api.Conflict,
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
			for n := 0; n < 3; n++ {
				if err := s.recoverResources(); err != nil &&
					!isCode(err, api.ManualRecovery) {
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

func templateQuotaSQLFault(
	t *testing.T,
	s *Service,
	table, event, condition, fault string,
) {
	t.Helper()
	timing, body := "BEFORE", "SELECT RAISE(ABORT,'handoff fault');"
	if fault == "ignore" {
		body = "SELECT RAISE(IGNORE);"
	}
	if fault == "commit" {
		if _, err := s.store.db.Exec(
			`CREATE TABLE handoff_parent(id INTEGER PRIMARY KEY); CREATE TABLE handoff_fk(id INTEGER REFERENCES handoff_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		); err != nil {
			t.Fatal(err)
		}
		timing = "AFTER"
		body = "INSERT INTO handoff_fk VALUES(1);"
	}
	if _, err := s.store.db.Exec(
		"CREATE TRIGGER handoff_fault " + timing + " " + event + " ON " + table + " WHEN " + condition + " BEGIN " + body + " END",
	); err != nil {
		t.Fatal(err)
	}
}

func templateQuotaHandoffFault(t *testing.T, stage, fault string) {
	var s *Service
	var f *recoveryTestEngine
	var p Principal
	var template Template
	var o Operation
	var err error
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unpark := func() { once.Do(func() { close(release) }) }
	defer unpark()
	gate := func(context.Context) error {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-release:
			return nil
		}
	}
	switch stage {
	case "fallback":
		service, engine, principal, tpl := warmFallbackFixture(t, true)
		s = service
		p = principal
		template = tpl
		f = &recoveryTestEngine{fakeEngine: engine}
		e := &templateQuotaEngine{
			Client:      f,
			attempts:    map[string]int{},
			afterDelete: func(api.Request, api.Operation) error { close(entered); return gate(s.ctx) },
		}
		s.opts.Engine = e
		templateQuotaSQLFault(
			t,
			s,
			"quota_resources",
			"DELETE",
			"OLD.kind='vm_disk'",
			fault,
		)
		o, err = s.Submit(
			s.ctx,
			p,
			"handoff-fallback",
			Command{Action: "claim", TemplateID: template.ID},
		)
	case "known":
		s, f, _, p = recoveryFixture(t)
		e := &templateQuotaEngine{
			Client:      f,
			attempts:    map[string]int{},
			afterCreate: func(api.Request, api.Operation) error { close(entered); return gate(s.ctx) },
		}
		s.opts.Engine = e
		templateQuotaSQLFault(
			t,
			s,
			"quota_resources",
			"INSERT",
			"NEW.kind='vm_disk'",
			fault,
		)
		o, err = s.Submit(
			s.ctx,
			p,
			"handoff-known",
			templateQuotaBuild("handoff-known"),
		)
	default:
		s, f, p, template = templateQuotaGolden(t)
		template = templateQuotaTarget(t, s, p, template, 1)
		if stage == "ready" {
			agent := s.opts.Agent.(*fakeAgent)
			agent.mu.Lock()
			agent.activateEntered = entered
			agent.activateGate = release
			agent.mu.Unlock()
			templateQuotaSQLFault(t, s, "warm", "INSERT", "1", fault)
			s.pools()
		} else {
			s.pools()
			s.wg.Wait()
			templateQuotaSQLFault(t, s, "warm", "DELETE", "1", fault)
			before := templateQuotaState(t, s)
			effects := f.count("clone")
			_, err = s.Submit(
				s.ctx,
				p,
				"handoff-selection",
				Command{Action: "claim", TemplateID: template.ID},
			)
			if err == nil {
				t.Fatal("selection fault missing")
			}
			if !bytes.Equal(before, templateQuotaState(t, s)) {
				t.Fatal(
					"selection fault lost warm row, D holder, slot, op or group",
				)
			}
			if f.count("clone") != effects {
				t.Fatal("failed selection dispatched Engine")
			}
			if fault == "commit" &&
				!strings.Contains(err.Error(), "operation admission commit") {
				t.Fatal("selection did not fail at actual Commit", err)
			}
			if _, err = s.store.db.Exec(
				`DROP TRIGGER handoff_fault`,
			); err != nil {
				t.Fatal(err)
			}
			ledgerExecute(
				t,
				s,
				p,
				"handoff-selection",
				Command{Action: "claim", TemplateID: template.ID},
			)
			ledgerMoney(t, s, p.TenantID, 1, 256)
			return
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handoff did not reach controlled pre-transaction boundary")
	}
	before := templateQuotaWithoutOperations(t, s)
	if stage == "ready" {
		ops, e := s.resourceOperations()
		if e != nil {
			t.Fatal(e)
		}
		for _, candidate := range ops {
			if candidate.Command.Action == "refill" {
				o = candidate
			}
		}
	}
	unpark()
	ctx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
	defer cancel()
	_, err = s.Wait(ctx, p, o.ID)
	if err == nil {
		t.Fatal("handoff fault missing")
	}
	if fault == "commit" && !strings.Contains(err.Error(), "FOREIGN KEY") {
		t.Fatal("handoff did not fail on deferred FK Commit", err)
	}
	t.Logf("handoff stage=%s fault=%s actual=%v", stage, fault, err)
	if !bytes.Equal(before, templateQuotaWithoutOperations(t, s)) {
		t.Fatal(
			"handoff failure partially published ledger, reference, metadata or destination",
		)
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER handoff_fault`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if stage == "known" {
		ledgerMoney(t, s, p.TenantID, 1, 64)
		r, e := s.loadRecovery(allocationGroupID(o))
		if e != nil {
			t.Fatal(e)
		}
		vm := r.Group.Identity.VM
		started, e := api.Wait(
			s.ctx,
			f,
			api.Request{Key: "known-external-start", Action: "start", VMID: vm},
		)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = api.Wait(
			s.ctx,
			f,
			api.Request{
				Key:              "known-external-delete",
				Action:           "delete",
				VMID:             vm,
				ExpectedInstance: started.ResultInstance,
			},
		); e != nil {
			t.Fatal(e)
		}
		if err = s.recoverResources(); err != nil {
			t.Fatal(err)
		}
		ledgerMoney(t, s, p.TenantID, 0, 0)
	} else {
		ledgerMoney(t, s, p.TenantID, 0, 192)
	}
	s = ledgerRestart(t, s)
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	if stage == "known" {
		ledgerMoney(t, s, p.TenantID, 0, 0)
	} else {
		ledgerMoney(t, s, p.TenantID, 0, 192)
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
	for n := 0; n < 2; n++ {
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
		api.ManualRecovery,
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
		for _, stage := range []string{"ready", "selection", "fallback"} {
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
		if _, err = f.GetSnapshot(s.ctx, old); !isCode(err, api.NotFound) {
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
		if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, api.ManualRecovery) {
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
	if !isCode(err, api.Conflict) {
		t.Fatalf("ordinary first nonzero pool target must be refused: %v", err)
	}
}

func templateQuotaWithoutOperations(t *testing.T, s *Service) []byte {
	t.Helper()
	var state map[string][]string
	if err := json.Unmarshal(templateQuotaState(t, s), &state); err != nil {
		t.Fatal(err)
	}
	delete(state, "operations")
	return encode(state)
}

func templateQuotaPublicationFault(t *testing.T, fault string) {
	s, _, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "publication-source", nil).Sandbox
	old := ledgerExecute(
		t,
		s,
		p,
		"publication-old-cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	newCP := ledgerExecute(
		t,
		s,
		p,
		"publication-new-cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	command := Command{
		Action: "template_put",
		Template: &Template{
			ID:         "publication",
			SnapshotID: old.SnapshotID,
			Shape:      b.Shape,
		},
	}
	ledgerExecute(t, s, p, "publication-first", command)
	trigger := `CREATE TRIGGER publication_fault BEFORE UPDATE ON templates WHEN NEW.id='publication' BEGIN SELECT RAISE(ABORT,'publication abort'); END`
	if fault == "ignore" {
		trigger = `CREATE TRIGGER publication_fault BEFORE UPDATE ON templates WHEN NEW.id='publication' BEGIN SELECT RAISE(IGNORE); END`
	}
	if fault == "commit" {
		if _, err := s.store.db.Exec(
			`CREATE TABLE publication_parent(id INTEGER PRIMARY KEY); CREATE TABLE publication_fk(id INTEGER REFERENCES publication_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		); err != nil {
			t.Fatal(err)
		}
		trigger = `CREATE TRIGGER publication_fault AFTER UPDATE ON templates WHEN NEW.id='publication' BEGIN INSERT INTO publication_fk VALUES(1); END`
	}
	if _, err := s.store.db.Exec(trigger); err != nil {
		t.Fatal(err)
	}
	command.Template.SnapshotID = newCP.SnapshotID
	unlock := s.lock("template:publication")
	o, err := s.Submit(s.ctx, p, "publication-fault", command)
	if err != nil {
		unlock()
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		current, e := s.store.operation(s.ctx, o.ID)
		if e != nil {
			unlock()
			t.Fatal(e)
		}
		if current.Status == "running" {
			break
		}
		select {
		case <-deadline.C:
			unlock()
			t.Fatal("worker did not park at template lock")
		case <-ticker.C:
		}
	}
	before := templateQuotaWithoutOperations(t, s)
	unlock()
	_, err = s.Wait(s.ctx, p, o.ID)
	if err == nil {
		t.Fatal("publication fault missing")
	}
	if fault == "commit" &&
		(!strings.Contains(err.Error(), "template publication commit") || !strings.Contains(err.Error(), "FOREIGN KEY")) {
		t.Fatalf("not a real publication COMMIT failure: %v", err)
	}
	t.Logf("publication %s error=%v", fault, err)
	if !bytes.Equal(before, templateQuotaWithoutOperations(t, s)) {
		t.Fatal("publication fault partially moved catalog, holder or group")
	}
	stored, err := s.store.template(s.ctx, "publication")
	if err != nil || stored.SnapshotID != old.SnapshotID {
		t.Fatal("old ready catalog was lost", err)
	}
	if _, err = s.store.db.Exec(`DROP TRIGGER publication_fault`); err != nil {
		t.Fatal(err)
	}
	ledgerExecute(t, s, p, "publication-retry", command)
	s = ledgerRestart(t, s)
	stored, err = s.store.template(s.ctx, "publication")
	if err != nil || stored.SnapshotID != newCP.SnapshotID {
		t.Fatal("replacement did not converge", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 448)
}

func templateQuotaBackupGate(t *testing.T) {
	s, f, e, p, o := templateQuotaFailedBuild(t, true)
	if err := s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	before := templateQuotaState(t, s)
	backup := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(s.ctx, backup); err != nil {
		t.Fatal(err)
	}
	opts := s.opts
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := RestoreStateBackup(destination, backup); err != nil {
		t.Fatal(err)
	}
	opts.StateDir = destination
	opts.Initialize = false
	if _, err := Open(opts); !isCode(err, api.ManualRecovery) {
		t.Fatal("restore recovery fence missing", err)
	}
	if err := AcknowledgeStateRecovery(
		destination,
		"Reviewed owned fake Engine receipts and absence-only gate",
	); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	reopened.cancel()
	reopened.wg.Wait()
	reopened.ctx, reopened.cancel = context.WithCancel(context.Background())
	if !bytes.Equal(before, templateQuotaState(t, reopened)) {
		t.Fatal("backup/Ack changed frozen gate, costs, keys or public result")
	}
	ledgerMoney(t, reopened, p.TenantID, 1, 64)
	for n := 0; n < 2; n++ {
		if err = reopened.recoverResources(); err != nil {
			t.Fatal(err)
		}
	}
	e.mu.Lock()
	attempts := e.attempts["delete"]
	e.mu.Unlock()
	if attempts != 0 {
		t.Fatal("restore acknowledgment authorized witnessless deletion")
	}
	r, err := reopened.loadRecovery(allocationGroupID(o))
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.GetVM(reopened.ctx, r.Group.Identity.VM)
	if err != nil || current.Instance != "" {
		t.Fatal("backup changed created VM", err)
	}
}

func templateQuotaSchema(t *testing.T) {
	for _, version := range []int{4, 5, 7} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			dir := t.TempDir()
			st, err := openStore(dir, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = st.db.Exec(
				`UPDATE version SET version=?; DROP TABLE quota_resources`,
				version,
			); err != nil {
				t.Fatal(err)
			}
			if err = st.close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "sandboxd.db")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = openStore(dir, false); !isCode(err, api.Incompatible) {
				t.Fatal("old/future schema was accepted", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("schema rejection wrote database bytes")
			}
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var actual, tables int
			if err = db.QueryRow(`SELECT version FROM version`).
				Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='quota_resources'`).
				Scan(&tables); err != nil {
				t.Fatal(err)
			}
			if actual != version || tables != 0 {
				t.Fatal("schema rejection executed DDL")
			}
		})
	}
}

func TestTemplateQuotaReplacementAndRestart(t *testing.T) {
	t.Run(
		"old-warm-and-two-snapshot-fees",
		TestQuotaLedgerWarmBackingKeepsSharedCharge,
	)
	for _, fault := range []string{"abort", "ignore", "commit"} {
		t.Run(
			"publication-"+fault,
			func(t *testing.T) { templateQuotaPublicationFault(t, fault) },
		)
	}
	s, _, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		p,
		"cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	c := Command{
		Action: "template_put",
		Template: &Template{
			ID:         "retained",
			SnapshotID: cp.SnapshotID,
			Shape:      b.Shape,
		},
	}
	ledgerExecute(t, s, p, "put", c)
	s = ledgerRestart(t, s)
	ledgerExecute(t, s, p, "same-id", c)
	ledgerMoney(t, s, p.TenantID, 1, 256)
}

func templateQuotaSelectedOwner(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	bob := Principal{Root: true, TenantID: "bob"}
	if _, err := s.PutTenant(
		s.ctx,
		bob,
		Tenant{
			ID:            "bob",
			Token:         "bob-test-token",
			MaxSandboxes:  2,
			MaxStorageMiB: 512,
		},
	); err != nil {
		t.Fatal(err)
	}
	b := *claimTest(t, s, p, "alice-owner", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		bob,
		"bob-requests-alice-capture",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	o, err := s.store.byKey(s.ctx, "bob", "bob-requests-alice-capture")
	if err != nil || o.TenantID != "bob" || o.ChargeTenantID != p.TenantID {
		t.Fatal("request namespace changed charge owner", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
	ledgerMoney(t, s, "bob", 0, 0)
	owned := Template{
		ID:         "bob-import-alice",
		SnapshotID: cp.SnapshotID,
		Shape:      b.Shape,
	}
	if _, err = s.Submit(
		s.ctx,
		bob,
		"deny-owned-sid",
		Command{Action: "template_put", Template: &owned},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("root transferred Alice snapshot ownership", err)
	}
	if _, err = s.Submit(
		s.ctx,
		Principal{TenantID: "bob"},
		"ordinary-owned-sid",
		Command{Action: "template_put", Template: &owned},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("published state authorized foreign snapshot", err)
	}
	created, err := api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:    "external-import-create",
			Action: "create",
			Config: &api.Config{
				Shape: b.Shape,
				Image: templateQuotaBuild("external").Template.Image,
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
			Key:    "external-import-start",
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
			Key:              "external-import-capture",
			Action:           "snapshot",
			VMID:             created.VMID,
			ExpectedInstance: started.ResultInstance,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	external := Template{
		ID:         "bob-external",
		TenantID:   p.TenantID,
		SnapshotID: captured.SnapshotID,
		Shape:      b.Shape,
	}
	result := ledgerExecute(
		t,
		s,
		bob,
		"explicit-bob-import",
		Command{Action: "template_put", Template: &external},
	)
	if result.Template.TenantID != "bob" {
		t.Fatal("payload tenant replaced selected resource owner")
	}
	ledgerMoney(t, s, "bob", 0, 192)
	ledgerMoney(t, s, p.TenantID, 1, 256)
	if _, err = api.Wait(
		s.ctx,
		f,
		api.Request{
			Key:              "external-import-cleanup",
			Action:           "delete",
			VMID:             created.VMID,
			ExpectedInstance: started.ResultInstance,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func templateQuotaBackupPoint(t *testing.T, point string) {
	var s *Service
	var f *recoveryTestEngine
	if point == "selected" || point == "rejected" ||
		point == "fallback-reserved" {
		service, engine, p, template := warmFallbackFixture(t, true)
		s = service
		switch point {
		case "selected":
			s.mu.Lock()
			s.closing = true
			s.mu.Unlock()
			_, err := s.Submit(
				s.ctx,
				p,
				"backup-selected",
				Command{Action: "claim", TemplateID: template.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.closing = false
			s.mu.Unlock()
			engine.mu.Lock()
			engine.unavailable = true
			engine.mu.Unlock()
		case "fallback-reserved":
			e := &templateQuotaEngine{
				Client:     engine,
				attempts:   map[string]int{},
				gateAction: "clone",
				entered:    make(chan struct{}),
				gate:       make(chan struct{}),
			}
			s.opts.Engine = e
			if _, err := s.Submit(
				s.ctx,
				p,
				"backup-fallback",
				Command{Action: "claim", TemplateID: template.ID},
			); err != nil {
				t.Fatal(err)
			}
			select {
			case <-e.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("fallback did not reach reserved clone")
			}
		default:
			adapter := &recoveryTestEngine{
				fakeEngine:             engine,
				dropDeleteBeforeAccept: 1,
			}
			s.opts.Engine = adapter
			if _, err := s.Execute(
				s.ctx,
				p,
				"backup-rejected",
				Command{Action: "claim", TemplateID: template.ID},
			); !isCode(
				err,
				api.Unknown,
			) {
				t.Fatal("rejected selection unknown delete missing", err)
			}
			engine.mu.Lock()
			engine.unavailable = true
			engine.mu.Unlock()
		}
	} else {
		service, engine, _, p := recoveryFixture(t)
		s = service
		f = engine
		switch point {
		case "pending-build":
			e := &templateQuotaEngine{
				Client:   f,
				attempts: map[string]int{},
				entered:  make(chan struct{}),
				gate:     make(chan struct{}),
			}
			s.opts.Engine = e
			if _, err := s.Submit(
				s.ctx,
				p,
				"backup-pending",
				templateQuotaBuild("backup-pending"),
			); err != nil {
				t.Fatal(err)
			}
			select {
			case <-e.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("pending build did not park")
			}
		case "unknown-capture":
			e := &templateQuotaEngine{
				Client:      f,
				attempts:    map[string]int{},
				hideCapture: true,
			}
			s.opts.Engine = e
			if _, err := s.Execute(
				s.ctx,
				p,
				"backup-capture",
				templateQuotaBuild("backup-capture"),
			); !isCode(
				err,
				api.Unknown,
			) {
				t.Fatal("unknown backup capture missing", err)
			}
		default:
			built := ledgerExecute(
				t,
				s,
				p,
				"backup-build",
				templateQuotaBuild("backup-warm"),
			)
			template := templateQuotaTarget(t, s, p, *built.Template, 1)
			s.pools()
			s.wg.Wait()
			if point == "cleanup-warm" {
				templateQuotaTarget(t, s, p, template, 0)
				f.control.Lock()
				f.failDeletes = 100
				f.control.Unlock()
				s.pools()
				f.mu.Lock()
				f.unavailable = true
				f.mu.Unlock()
			}
		}
	}
	before := templateQuotaWithoutOperations(t, s)
	backup := filepath.Join(t.TempDir(), "backup")
	if _, err := s.Backup(s.ctx, backup); err != nil {
		t.Fatal(err)
	}
	opts := s.opts
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if wrapped, ok := opts.Engine.(*templateQuotaEngine); ok &&
		wrapped.gate != nil {
		wrapped.mu.Lock()
		wrapped.first = false
		wrapped.entered = make(chan struct{})
		wrapped.mu.Unlock()
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := RestoreStateBackup(restored, backup); err != nil {
		t.Fatal(err)
	}
	if err := AcknowledgeStateRecovery(
		restored,
		"Reviewed controlled fixture generation, receipts and retained responsibilities",
	); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(restored, false)
	if err != nil {
		t.Fatal(err)
	}
	view := &Service{store: st, ctx: context.Background()}
	if !bytes.Equal(before, templateQuotaWithoutOperations(t, view)) {
		_ = st.close()
		t.Fatal(
			"backup changed private group, cost, key, proof, holder or destination",
		)
	}
	if err = st.close(); err != nil {
		t.Fatal(err)
	}
	opts.StateDir = restored
	opts.Initialize = false
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	reopened.cancel()
	reopened.wg.Wait()
	reopened.ctx, reopened.cancel = context.WithCancel(context.Background())
	var pending, slots, storage int64
	if err = reopened.store.db.QueryRow(`SELECT COALESCE(sum(sandbox_slots),0),COALESCE(sum(storage_mib),0),sum(CASE WHEN kind='pending_storage' THEN 1 ELSE 0 END) FROM quota_resources`).
		Scan(&slots, &storage, &pending); err != nil {
		t.Fatal(err)
	}
	t.Logf(
		"backup point=%s slots=%d storage=%d pending=%d",
		point,
		slots,
		storage,
		pending,
	)
	if point == "pending-build" &&
		(slots != 1 || storage != 256 || pending != 2) {
		t.Fatal("unknown create responsibility was refunded")
	}
	if point == "fallback-reserved" &&
		(slots != 1 || storage != 256 || pending != 1) {
		t.Fatal("unknown replacement reservation did not survive Ack")
	}
	if point == "unknown-capture" &&
		(slots != 1 || storage != 256 || pending != 1) {
		t.Fatal("unknown SID responsibility did not survive Ack")
	}
}

func TestTemplateQuotaBackupAndAuthorization(t *testing.T) {
	t.Run("cleanup-request-corruption", func(t *testing.T) {
		s, f, p, template := templateQuotaGolden(t)
		templateQuotaTarget(t, s, p, template, 1)
		s.pools()
		s.wg.Wait()
		templateQuotaTarget(t, s, p, template, 0)
		f.control.Lock()
		f.dropDeleteBeforeAccept = 1
		f.control.Unlock()
		s.pools()
		intents, err := s.resourceRecoveries()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range intents {
			if r.Kind == "allocation_cleanup" && !r.Done {
				found = true
				r.Deletion.Step.Request.ExpectedInstance = ""
				if _, err = s.store.db.Exec(
					`UPDATE resource_recovery SET body=? WHERE id=?`,
					encode(r),
					r.ID,
				); err != nil {
					t.Fatal(err)
				}
			}
		}
		if !found {
			t.Fatal("qualified cleanup marker was not created")
		}
		opts := s.opts
		opts.Initialize = false
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if next, e := Open(opts); !isCode(e, api.ManualRecovery) {
			if next != nil {
				next.Close()
			}
			t.Fatal("empty cleanup predicate accepted at startup", e)
		}
	})
	t.Run("corrupt-frozen-state", func(t *testing.T) {
		for _, kind := range []string{"generation", "step", "template-holder"} {
			t.Run(kind, func(t *testing.T) {
				s, f, p, template := templateQuotaGolden(t)
				o, err := s.store.byKey(s.ctx, p.TenantID, "golden-build")
				if err != nil {
					t.Fatal(err)
				}
				r, err := s.loadRecovery(allocationGroupID(o))
				if err != nil {
					t.Fatal(err)
				}
				if kind == "template-holder" {
					_, err = s.store.db.Exec(
						`DELETE FROM quota_refs WHERE kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?`,
						template.SnapshotID,
						"template:"+template.ID,
					)
				} else {
					if kind == "generation" {
						r.Group.Identity.Generation.Key += "-foreign"
					} else {
						step := r.Group.Steps["build-start"]
						step.Proof.Request.VMID = "foreign"
						r.Group.Steps["build-start"] = step
					}
					_, err = s.store.db.Exec(
						`UPDATE resource_recovery SET body=? WHERE id=?`,
						encode(r),
						r.ID,
					)
				}
				if err != nil {
					t.Fatal(err)
				}
				opts := s.opts
				opts.Initialize = false
				before := f.count("start")
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				if next, e := Open(opts); !isCode(e, api.ManualRecovery) {
					if next != nil {
						next.Close()
					}
					t.Fatal("corrupt frozen provenance accepted", e)
				}
				if f.count("start") != before {
					t.Fatal("database validation called Engine lifecycle")
				}
			})
		}
	})
	t.Run("selected-owner-authorization", templateQuotaSelectedOwner)
	for _, point := range []string{"pending-build", "unknown-capture", "active-warm", "cleanup-warm", "selected", "rejected", "fallback-reserved"} {
		t.Run(
			"backup-"+point,
			func(t *testing.T) { templateQuotaBackupPoint(t, point) },
		)
	}
	t.Run("gate-backup", templateQuotaBackupGate)
	t.Run("old-schema-before-ddl", templateQuotaSchema)
	s, _, _, p := recoveryFixture(t)
	b := *claimTest(t, s, p, "source", nil).Sandbox
	cp := ledgerExecute(
		t,
		s,
		p,
		"cp",
		Command{Action: "checkpoint", SandboxID: b.ID},
	).Checkpoint
	_, err := s.Submit(
		s.ctx,
		Principal{TenantID: "bob"},
		"foreign",
		Command{Action: "template_put", Template: &Template{
			ID: "foreign", SnapshotID: cp.SnapshotID, Shape: b.Shape,
		}},
	)
	if !isCode(err, api.NotFound) {
		t.Fatalf("foreign published SID must be NotFound: %v", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
}
