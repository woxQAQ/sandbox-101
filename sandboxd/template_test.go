package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestUntrustedExistingTemplateCannotCloneSnapshot(t *testing.T) {
	s, f, _, p := recoveryFixture(t)
	template := Template{
		ID:         "untrusted",
		TenantID:   p.TenantID,
		State:      "ready",
		SnapshotID: "unowned",
		TargetWarm: 1,
		Shape:      api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}
	f.mu.Lock()
	f.snaps[template.SnapshotID] = api.Snapshot{
		ID:        template.SnapshotID,
		Config:    api.Config{Shape: template.Shape},
		Published: true,
	}
	f.mu.Unlock()
	if _, err := s.store.db.Exec(
		"INSERT INTO templates(id,tenant,body) VALUES(?,?,?)",
		template.ID,
		template.TenantID,
		encode(template),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Execute(
		s.ctx,
		p,
		"claim",
		Command{Action: "claim", TemplateID: template.ID},
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatal("untrusted template admitted", err)
	}
	s.pools()
	// A previously frozen internal refill is revalidated by the worker too.
	o := Operation{
		ID:       "untrusted-refill",
		Key:      "untrusted-refill",
		TenantID: p.TenantID,
		Command: Command{
			Action:     "refill",
			TemplateID: template.ID,
			Template:   &template,
		},
		Status: "pending",
	}
	o.hash = s.store.sign(o.ID)
	if err := saveOp(s.ctx, s.store.db, o); err != nil {
		t.Fatal(err)
	}
	s.start(o)
	if _, err := s.Wait(s.ctx, p, o.ID); !isCode(err, ErrNotFound) {
		t.Fatal("untrusted refill accepted", err)
	}
	if f.count("clone") != 0 {
		t.Fatal("unauthorized snapshot cloned")
	}
}

func TestTemplateSnapshotTenantAuthorization(t *testing.T) {
	s, f, _, p := fixture(t)
	b := claimTest(t, s, p, "source", nil)
	cp, err := s.Execute(
		context.Background(),
		p,
		"checkpoint",
		Command{Action: "checkpoint", SandboxID: b.Sandbox.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            "bob",
			Token:         "bob-token",
			MaxSandboxes:  10,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	bob := Principal{TenantID: "bob"}
	template := Template{
		ID:         "imported",
		Shape:      b.Sandbox.Shape,
		SnapshotID: cp.Checkpoint.SnapshotID,
	}
	if _, err = s.Execute(
		context.Background(),
		bob,
		"foreign",
		Command{Action: "template_put", Template: &template},
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatalf("foreign snapshot accepted: %v", err)
	}
	if _, err = s.Execute(
		context.Background(),
		p,
		"own",
		Command{Action: "template_put", Template: &template},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Execute(
		context.Background(),
		Principal{TenantID: p.TenantID, SandboxID: b.Sandbox.ID},
		"sandbox-import",
		Command{Action: "template_put", Template: &template},
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatal(err)
	}
	h := s.Handler()
	get := func(snapshot string) *httptest.ResponseRecorder {
		template.ID = "bob-template"
		template.SnapshotID = snapshot
		req := httptest.NewRequest(
			"POST",
			"/v1/templates",
			strings.NewReader(string(encode(Command{Template: &template}))),
		)
		req.Header.Set("Authorization", "Bearer bob-token")
		req.Header.Set("Idempotency-Key", snapshot)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	foreign, unknown := get(cp.Checkpoint.SnapshotID), get("unknown")
	if foreign.Code != 404 || unknown.Code != 404 ||
		foreign.Body.String() != unknown.Body.String() {
		t.Fatalf(
			"authorization responses differ: %d %s / %d %s",
			foreign.Code,
			foreign.Body.String(),
			unknown.Code,
			unknown.Body.String(),
		)
	}
	f.mu.Lock()
	f.snaps["external"] = api.Snapshot{
		ID:        "external",
		Config:    api.Config{Shape: b.Sandbox.Shape},
		Published: true,
	}
	f.mu.Unlock()
	template.ID = "golden"
	template.SnapshotID = "external"
	if _, err = s.Execute(
		context.Background(),
		Principal{Root: true, TenantID: "bob"},
		"root-import",
		Command{Action: "template_put", Template: &template},
	); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Execute(
		context.Background(),
		Principal{Root: true, TenantID: "bob"},
		"root-foreign",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:         "no-transfer",
				Shape:      b.Sandbox.Shape,
				SnapshotID: cp.Checkpoint.SnapshotID,
			},
		},
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatal("root transferred ownership", err)
	}
	if f.count("clone") != 0 {
		t.Fatal("import cloned before authorization")
	}
}

// notFoundError asserts the uniform P12 response and returns it for
// body comparison against a reference rejection.
func notFoundError(t *testing.T, err error) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != ErrNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
	return e
}

func TestTemplateAdmissionOwnerReservedBeforePublication(t *testing.T) {
	s, _, a, p := fixture(t)
	ctx := context.Background()
	if _, err := s.PutTenant(
		ctx,
		Principal{Root: true},
		Tenant{
			ID:            "bob",
			Token:         "bob-token",
			MaxSandboxes:  10,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	bob := Principal{TenantID: "bob"}
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	template := func() *Template {
		return &Template{
			ID:    "gated",
			Image: "sha256:" + strings.Repeat("2", 64),
			Shape: shape,
		}
	}
	// Hold alice's first build inside guest activation; the reservation must
	// already bind the name before anything is published.
	entered := make(chan struct{})
	gate := make(chan struct{})
	a.mu.Lock()
	a.activateEntered = entered
	a.activateGate = gate
	a.mu.Unlock()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	first, err := s.Submit(
		ctx,
		p,
		"first-build",
		Command{Action: "template_build", Template: template()},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("build did not reach guest activation")
	}
	// Bob's submission for the reserved name must look exactly like a
	// submission for a name another tenant already published.
	_, reservedErr := s.Submit(
		ctx,
		bob,
		"bob-build",
		Command{Action: "template_build", Template: template()},
	)
	_, foreignErr := s.Submit(
		ctx,
		bob,
		"bob-foreign",
		Command{
			Action: "template_build",
			Template: &Template{
				ID:    "python",
				Image: "sha256:" + strings.Repeat("2", 64),
				Shape: shape,
			},
		},
	)
	reserved, foreign := notFoundError(t, reservedErr),
		notFoundError(t, foreignErr)
	if reserved.Message != foreign.Message {
		t.Fatalf(
			"reserved and foreign responses differ: %q / %q",
			reserved.Message,
			foreign.Message,
		)
	}
	// The owning tenant cannot hold two in-flight publications either; a
	// retry with the original key still returns the original operation.
	if _, err = s.Submit(
		ctx,
		p,
		"second-build",
		Command{Action: "template_build", Template: template()},
	); !isCode(err, ErrConflict) {
		t.Fatalf("concurrent same-tenant build admitted: %v", err)
	}
	again, err := s.Submit(
		ctx,
		p,
		"first-build",
		Command{Action: "template_build", Template: template()},
	)
	if err != nil || again.ID != first.ID {
		t.Fatalf("same key did not return the original operation: %v", err)
	}
	release.Do(func() { close(gate) })
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(waitCtx, p, first.ID); err != nil {
		t.Fatal(err)
	}
	stored, err := s.store.template(ctx, "gated")
	if err != nil || stored.TenantID != p.TenantID || stored.State != "ready" {
		t.Fatalf("publication lost owner: %+v %v", stored, err)
	}
	var owner, pointer string
	if err = s.store.db.QueryRow(
		"SELECT tenant_id,operation_id FROM template_intents WHERE template_id='gated'",
	).Scan(&owner, &pointer); err != nil ||
		owner != p.TenantID || pointer != first.ID {
		t.Fatal(owner, pointer, err)
	}
	if _, err = s.Submit(
		ctx,
		bob,
		"bob-claim",
		Command{Action: "claim", TemplateID: "gated"},
	); !isCode(err, ErrNotFound) {
		t.Fatalf("foreign tenant claimed the template: %v", err)
	}
	claim := func(key string) Result {
		t.Helper()
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		r, err := s.Execute(
			waitCtx,
			p,
			key,
			Command{
				Action:     "claim",
				TemplateID: "gated",
				Policy:     egress.Policy{Mode: "none"},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if got := claim("owner-claim"); got.Sandbox == nil {
		t.Fatal("owner cannot claim its published template")
	}
}

func TestTemplateAdmissionRestartAndFailedBuild(t *testing.T) {
	s, f, a, p := fixture(t)
	ctx := context.Background()
	if _, err := s.PutTenant(
		ctx,
		Principal{Root: true},
		Tenant{
			ID:            "bob",
			Token:         "bob-token",
			MaxSandboxes:  10,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	bob := Principal{TenantID: "bob"}
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	template := func(id, digest string) *Template {
		return &Template{
			ID:    id,
			Image: "sha256:" + strings.Repeat(digest, 64),
			Shape: shape,
		}
	}
	build := func(id, digest string) Command {
		return Command{Action: "template_build", Template: template(id, digest)}
	}
	// A failed first build keeps the reservation without listing a ready
	// template; only the owner may retry, under a new key.
	f.mu.Lock()
	f.failSnapshot = true
	f.mu.Unlock()
	if _, err := s.Execute(
		ctx,
		p,
		"flaky-build",
		build("flaky", "3"),
	); !isCode(err, ErrExhausted) {
		t.Fatalf("expected the injected build failure: %v", err)
	}
	if _, err := s.Submit(
		ctx,
		bob,
		"bob-take",
		build("flaky", "3"),
	); !isCode(err, ErrNotFound) {
		t.Fatalf("failure released the name to another tenant: %v", err)
	}
	listed, err := s.Templates(ctx, p)
	if err != nil {
		t.Fatal(err)
	}
	for _, tmpl := range listed {
		if tmpl.ID == "flaky" {
			t.Fatal("failed intent was disguised as a ready template")
		}
	}
	f.mu.Lock()
	f.failSnapshot = false
	f.mu.Unlock()
	retry, err := s.Execute(ctx, p, "flaky-retry", build("flaky", "3"))
	if err != nil {
		t.Fatal(err)
	}
	if retry.Template == nil || retry.Template.State != "ready" ||
		retry.Template.TenantID != p.TenantID {
		t.Fatalf("owner retry lost the template: %+v", retry.Template)
	}
	var owner, pointer string
	if err = s.store.db.QueryRow(
		"SELECT tenant_id,operation_id FROM template_intents WHERE template_id='flaky'",
	).Scan(&owner, &pointer); err != nil ||
		owner != p.TenantID || pointer != retry.OperationID {
		t.Fatal(owner, pointer, err)
	}
	// A crash mid-build preserves both the reservation and the resumable
	// operation across a restart.
	entered := make(chan struct{})
	gate := make(chan struct{})
	a.mu.Lock()
	a.activateEntered = entered
	a.activateGate = gate
	a.mu.Unlock()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	durable, err := s.Submit(ctx, p, "durable-build", build("durable", "4"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("build did not reach guest activation")
	}
	s.cancel()
	s.wg.Wait()
	opts := s.opts
	opts.Initialize = false
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// The replayed build blocks at the same gate, re-armed for the reopen.
	reentered := make(chan struct{})
	regate := make(chan struct{})
	a.mu.Lock()
	a.activateEntered = reentered
	a.activateGate = regate
	a.mu.Unlock()
	var rerelease sync.Once
	t.Cleanup(func() { rerelease.Do(func() { close(regate) }) })
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	select {
	case <-reentered:
	case <-time.After(5 * time.Second):
		t.Fatal("replayed build did not reach guest activation")
	}
	if _, err = reopened.Submit(
		ctx,
		bob,
		"bob-restart-take",
		build("durable", "4"),
	); !isCode(err, ErrNotFound) {
		t.Fatalf("restart released the name to another tenant: %v", err)
	}
	rerelease.Do(func() { close(regate) })
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err = reopened.Wait(waitCtx, p, durable.ID); err != nil {
		t.Fatal("replay after restart failed", err)
	}
	stored, err := reopened.store.template(ctx, "durable")
	if err != nil || stored.TenantID != p.TenantID || stored.State != "ready" {
		t.Fatalf("restarted publication lost owner: %+v %v", stored, err)
	}
	if err = reopened.store.db.QueryRow(
		"SELECT tenant_id,operation_id FROM template_intents WHERE template_id='durable'",
	).Scan(&owner, &pointer); err != nil ||
		owner != p.TenantID || pointer != durable.ID {
		t.Fatal(owner, pointer, err)
	}
}

func TestTemplateAdmissionPublicationFence(t *testing.T) {
	s, f, a, p := fixture(t)
	ctx := context.Background()
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	template := func(digest string) *Template {
		return &Template{
			ID:    "rolling",
			Image: "sha256:" + strings.Repeat(digest, 64),
			Shape: shape,
		}
	}
	build := func(digest string) Command {
		return Command{Action: "template_build", Template: template(digest)}
	}
	v1, err := s.Execute(ctx, p, "build-v1", build("4"))
	if err != nil {
		t.Fatal(err)
	}
	first := v1.Template.SnapshotID
	// The rolling rebuild is admitted because the previous operation is
	// terminal; hold it inside guest activation.
	entered := make(chan struct{})
	gate := make(chan struct{})
	a.mu.Lock()
	a.activateEntered = entered
	a.activateGate = gate
	a.mu.Unlock()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	v2, err := s.Submit(ctx, p, "build-v2", build("5"))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rebuild did not reach guest activation")
	}
	// The published version keeps serving claims while the rebuild runs.
	claimCtx, claimCancel := context.WithTimeout(ctx, 5*time.Second)
	defer claimCancel()
	claimed, err := s.Execute(
		claimCtx,
		p,
		"claim-rolling",
		Command{
			Action:     "claim",
			TemplateID: "rolling",
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	provision, err := f.FindOperation(
		ctx,
		stepKey(Operation{ID: claimed.OperationID}, "provision"),
	)
	if err != nil || provision.Request.SnapshotID != first {
		t.Fatalf(
			"claim during rebuild did not serve the published snapshot: %v",
			err,
		)
	}
	release.Do(func() { close(gate) })
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err = s.Wait(waitCtx, p, v2.ID); err != nil {
		t.Fatal(err)
	}
	published, err := s.store.template(ctx, "rolling")
	if err != nil || published.SnapshotID == first {
		t.Fatalf("rebuild did not publish a new snapshot: %+v", published)
	}
	// A stale worker from the superseded operation can neither republish
	// over the recycled pointer nor move it.
	stale, err := s.store.operation(ctx, v1.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.template(&stale); !isCode(err, ErrConflict) {
		t.Fatalf("stale publication accepted: %v", err)
	}
	stored, err := s.store.template(ctx, "rolling")
	if err != nil || stored.SnapshotID != published.SnapshotID {
		t.Fatal("stale publication overwrote the template")
	}
	var pointer string
	if err = s.store.db.QueryRow(
		"SELECT operation_id FROM template_intents WHERE template_id='rolling'",
	).Scan(&pointer); err != nil || pointer != v2.ID {
		t.Fatal("stale publication moved the pointer", pointer, err)
	}
	// A pointer referencing a vanished operation requires manual recovery;
	// with an empty pointer the owner admits the next publication.
	if _, err = s.store.db.Exec(
		"UPDATE template_intents SET operation_id='op_lost' WHERE template_id='rolling'",
	); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Submit(ctx, p, "build-v3", build("6")); !isCode(
		err,
		ErrManualRecovery,
	) {
		t.Fatalf("lost pointer admitted silently: %v", err)
	}
	if _, err = s.store.db.Exec(
		"UPDATE template_intents SET operation_id='' WHERE template_id='rolling'",
	); err != nil {
		t.Fatal(err)
	}
	v3, err := s.Execute(ctx, p, "build-v3", build("6"))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.store.db.QueryRow(
		"SELECT operation_id FROM template_intents WHERE template_id='rolling'",
	).Scan(&pointer); err != nil || pointer != v3.OperationID {
		t.Fatal("empty pointer was not retaken", pointer, err)
	}
}

// templateAdmissionState captures durable admission/publication evidence without
// including its contents in failure reports.
func templateAdmissionState(t *testing.T, s *Service, f *fakeEngine) []byte {
	t.Helper()
	var state []any
	for _, query := range []string{
		"SELECT id,tenant,body FROM templates ORDER BY id",
		"SELECT template_id,tenant_id,operation_id FROM template_intents ORDER BY template_id",
		"SELECT id,tenant,idem_key,spec_hash,status,body FROM operations ORDER BY id",
		"SELECT * FROM snapshot_owners ORDER BY snapshot_id",
	} {
		rows, err := s.store.db.Query(query)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var table [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			ptrs := make([]any, len(columns))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err = rows.Scan(ptrs...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			table = append(table, values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		state = append(state, table)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return encode(append(state, f.calls, f.ops, f.vms, f.snaps))
}

func TestTemplateAdmissionPublishedOwnerConsistency(t *testing.T) {
	for _, corruption := range []string{"missing-placeholder", "conflicting-owner"} {
		t.Run(corruption, func(t *testing.T) {
			s, f, _, p := fixture(t)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()
			published, err := s.store.template(ctx, "python")
			if err != nil {
				t.Fatal(err)
			}
			if corruption == "missing-placeholder" {
				_, err = s.store.db.Exec(
					"DELETE FROM template_intents WHERE template_id='python'",
				)
			} else {
				published.TenantID = "bob"
				_, err = s.store.db.Exec(
					"UPDATE templates SET tenant=?,body=? WHERE id='python'",
					published.TenantID,
					encode(published),
				)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := templateAdmissionState(t, s, f)
			for _, caller := range []struct {
				name string
				p    Principal
				code ErrorCode
			}{
				{"foreign", Principal{TenantID: "eve"}, ErrNotFound},
				{"root-foreign-target", Principal{Root: true, TenantID: "eve"}, ErrNotFound},
				{"owner", p, ErrManualRecovery},
				{"root-owner", Principal{Root: true, TenantID: p.TenantID}, ErrManualRecovery},
			} {
				for _, action := range []string{"template_build", "template_put"} {
					_, err = s.Submit(
						ctx,
						caller.p,
						caller.name+action,
						Command{Action: action, Template: &Template{
							ID:    "python",
							Image: published.Image,
							Shape: published.Shape,
						}},
					)
					if !isCode(err, caller.code) {
						t.Fatalf(
							"%s/%s: expected %s, got %v",
							caller.name,
							action,
							caller.code,
							err,
						)
					}
					if caller.code == ErrNotFound &&
						notFoundError(t, err).Message != "not found" {
						t.Fatal("foreign response disclosed owner details")
					}
					if !bytes.Equal(before, templateAdmissionState(t, s, f)) {
						t.Fatal(
							"rejected admission changed durable or engine state",
						)
					}
				}
			}
		})
	}
}

func TestTemplateAdmissionPointerProvenance(t *testing.T) {
	for _, corruption := range []string{
		"missing", "unknown-status", "wrong-action", "missing-template", "wrong-template", "wrong-tenant", "wrong-template-owner", "wrong-id", "column-tenant", "column-status", "invalid-body",
	} {
		t.Run(corruption, func(t *testing.T) {
			s, f, _, p := fixture(t)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				5*time.Second,
			)
			defer cancel()
			old, err := s.store.byKey(ctx, p.TenantID, "template")
			if err != nil {
				t.Fatal(err)
			}
			// Corrupt only the durable pointer's evidence; the published
			// version and permanent reservation remain present.
			switch corruption {
			case "missing":
				_, err = s.store.db.Exec(
					"UPDATE template_intents SET operation_id='op_missing' WHERE template_id='python'",
				)
			case "column-tenant":
				_, err = s.store.db.Exec(
					"UPDATE operations SET tenant='bob' WHERE id=?",
					old.ID,
				)
			case "column-status":
				_, err = s.store.db.Exec(
					"UPDATE operations SET status='failed' WHERE id=?",
					old.ID,
				)
			case "invalid-body":
				_, err = s.store.db.Exec(
					"UPDATE operations SET body='{' WHERE id=?",
					old.ID,
				)
			default:
				switch corruption {
				case "unknown-status":
					old.Status = "unrecognized"
				case "wrong-action":
					old.Command.Action = "claim"
				case "missing-template":
					old.Command.Template = nil
				case "wrong-template":
					old.Command.Template.ID = "other"
				case "wrong-tenant":
					old.TenantID = "bob"
				case "wrong-template-owner":
					old.Command.Template.TenantID = "bob"
				case "wrong-id":
					old.ID = "op_other"
				}
				_, err = s.store.db.Exec(
					"UPDATE operations SET status=?,body=? WHERE idem_key='template'",
					old.Status,
					encode(old),
				)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := templateAdmissionState(t, s, f)
			for _, caller := range []struct {
				name string
				p    Principal
				code ErrorCode
			}{
				{"foreign", Principal{TenantID: "eve"}, ErrNotFound},
				{"root-foreign", Principal{Root: true, TenantID: "eve"}, ErrNotFound},
				{"owner", p, ErrManualRecovery},
				{"root-owner", Principal{Root: true, TenantID: p.TenantID}, ErrManualRecovery},
			} {
				_, err = s.Submit(
					ctx,
					caller.p,
					caller.name,
					Command{Action: "template_build", Template: &Template{
						ID:    "python",
						Image: "sha256:" + strings.Repeat("2", 64),
						Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
					}},
				)
				if !isCode(err, caller.code) {
					t.Fatalf(
						"%s: expected %s, got %v",
						caller.name,
						caller.code,
						err,
					)
				}
				if caller.code == ErrNotFound &&
					notFoundError(t, err).Message != "not found" {
					t.Fatal("foreign response disclosed pointer details")
				}
				if !bytes.Equal(before, templateAdmissionState(t, s, f)) {
					t.Fatal(
						"rejected admission changed durable or engine state",
					)
				}
			}
		})
	}
}

// snapshotQueryGate exposes the engine boundary in a real template publication.
// Embedding the fixture client leaves its keyed engine effects unchanged.
type snapshotQueryGate struct {
	api.Client

	entered chan struct{}
	release chan struct{}
}

func (g *snapshotQueryGate) GetSnapshot(
	ctx context.Context,
	id string,
) (api.Snapshot, error) {
	snap, err := g.Client.GetSnapshot(ctx, id)
	close(g.entered)
	select {
	case <-ctx.Done():
		return api.Snapshot{}, ctx.Err()
	case <-g.release:
		return snap, err
	}
}

func TestTemplateAdmissionPublicationOwnershipGate(t *testing.T) {
	s, f, _, p := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	built, err := s.Execute(
		ctx,
		p,
		"build-golden",
		Command{Action: "template_build", Template: &Template{
			ID: "golden", Image: "sha256:" + strings.Repeat("2", 64),
			Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	gate := &snapshotQueryGate{
		Client:  f,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	s.opts.Engine = gate
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(gate.release) }) })
	update := *built.Template
	update.TargetWarm = 1
	op, err := s.Submit(
		ctx,
		Principal{Root: true, TenantID: p.TenantID},
		"update-golden",
		Command{Action: "template_put", Template: &update},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("publication did not enter snapshot query")
	}
	acquired := make(chan func(), 1)
	go func() { acquired <- s.lockResourceOwnership() }()
	var ownership func()
	select {
	case ownership = <-acquired:
	case <-time.After(200 * time.Millisecond):
		release.Do(func() { close(gate.release) })
		// Drain the waiter so even the RED case cannot strand a guard.
		select {
		case ownership = <-acquired:
			ownership()
		case <-ctx.Done():
		}
		t.Fatal("publication held ownership across an engine query")
	}
	var unlock sync.Once
	t.Cleanup(func() { unlock.Do(ownership) })
	release.Do(func() { close(gate.release) })
	done := make(chan error, 1)
	go func() { _, err := s.Wait(ctx, p, op.ID); done <- err }()
	select {
	case err = <-done:
		t.Fatalf("publication crossed the held ownership guard: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	stored, err := s.store.template(ctx, "golden")
	if err != nil || stored.TargetWarm != 0 {
		t.Fatal("publication changed the catalog while ownership was held")
	}
	unlock.Do(ownership)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	stored, err = s.store.template(ctx, "golden")
	if err != nil || stored.TargetWarm != 1 ||
		stored.SnapshotID != built.Template.SnapshotID {
		t.Fatal("publication did not finish after ownership was released")
	}
}

func TestTemplateAdmissionRootExplicitOwnerUpdate(t *testing.T) {
	s, f, _, p := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	original, err := s.store.byKey(ctx, p.TenantID, "template")
	if err != nil {
		t.Fatal(err)
	}
	before := templateAdmissionState(t, s, f)
	command := Command{Action: "template_put", Template: &Template{
		ID: "python", Image: "sha256:" + strings.Repeat("2", 64),
		Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}}
	for _, tenant := range []string{"", "bob"} {
		_, err = s.Submit(
			ctx,
			Principal{Root: true, TenantID: tenant},
			"root-wrong-target",
			command,
		)
		if !isCode(err, ErrNotFound) {
			t.Fatalf("root with foreign target admitted: %v", err)
		}
	}
	if !bytes.Equal(before, templateAdmissionState(t, s, f)) {
		t.Fatal("root target rejection changed state")
	}
	root := Principal{Root: true, TenantID: p.TenantID}
	updated, err := s.Execute(ctx, root, "root-update", command)
	if err != nil || updated.Template == nil ||
		updated.Template.TenantID != p.TenantID {
		t.Fatal("root could not update the explicitly selected owner", err)
	}
	again, err := s.Submit(ctx, root, "root-update", command)
	if err != nil || again.ID != updated.OperationID {
		t.Fatal("same-key root retry did not deduplicate", err)
	}
	retained, err := s.store.operation(ctx, original.ID)
	if err != nil || !bytes.Equal(encode(original), encode(retained)) {
		t.Fatal("terminal pointer recycling changed the historical result")
	}
	if again.Command.Template == nil ||
		again.Command.Template.TenantID != p.TenantID {
		t.Fatal("root update did not record the explicit owner")
	}
}

func TestTemplateAdmissionBackupPreservesOwner(t *testing.T) {
	s, f, _, p := fixture(t)
	ctx := context.Background()
	if _, err := s.PutTenant(
		ctx,
		Principal{Root: true},
		Tenant{
			ID:            "bob",
			Token:         "bob-token",
			MaxSandboxes:  10,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	bob := Principal{TenantID: "bob"}
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	build := func(id, digest string) Command {
		return Command{
			Action: "template_build",
			Template: &Template{
				ID:    id,
				Image: "sha256:" + strings.Repeat(digest, 64),
				Shape: shape,
			},
		}
	}
	// Both a ready publication and a failed first build enter the backup.
	ready, err := s.Execute(ctx, p, "build-ready", build("backed", "7"))
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failSnapshot = true
	f.mu.Unlock()
	if _, err = s.Execute(
		ctx,
		p,
		"build-failed",
		build("failed-name", "8"),
	); !isCode(err, ErrExhausted) {
		t.Fatalf("expected the injected build failure: %v", err)
	}
	f.mu.Lock()
	f.failSnapshot = false
	f.mu.Unlock()
	destination := filepath.Join(t.TempDir(), "backup")
	if _, err = s.Backup(ctx, destination); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err = RestoreStateBackup(restored, destination); err != nil {
		t.Fatal(err)
	}
	opts := s.opts
	opts.StateDir = restored
	opts.Initialize = false
	// The recovery fence still holds: restored ownership state demands
	// acknowledgement before it can serve again.
	if _, err = Open(opts); !isCode(err, ErrManualRecovery) {
		t.Fatalf("restored state opened without review: %v", err)
	}
	if err = AcknowledgeStateRecovery(
		restored,
		"reviewed engine inventory and template ownership",
	); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	for _, id := range []string{"backed", "failed-name"} {
		if _, err = reopened.Submit(
			ctx,
			bob,
			"bob-"+id,
			build(id, "8"),
		); !isCode(err, ErrNotFound) {
			t.Fatalf("restore released %s to another tenant: %v", id, err)
		}
	}
	// The owner retries the failed publication and the ready version keeps
	// serving the snapshot captured before the backup.
	if _, err = reopened.Execute(
		ctx,
		p,
		"retry-failed",
		build("failed-name", "8"),
	); err != nil {
		t.Fatal(err)
	}
	claimCtx, claimCancel := context.WithTimeout(ctx, 5*time.Second)
	defer claimCancel()
	claimed, err := reopened.Execute(
		claimCtx,
		p,
		"claim-backed",
		Command{
			Action:     "claim",
			TemplateID: "backed",
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	provision, err := f.FindOperation(
		ctx,
		stepKey(Operation{ID: claimed.OperationID}, "provision"),
	)
	if err != nil ||
		provision.Request.SnapshotID != ready.Template.SnapshotID {
		t.Fatalf("restored template lost its snapshot: %v", err)
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
		if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, ErrUnknown) {
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
			if !isCode(err, ErrExhausted) {
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
		if !isCode(err, ErrExhausted) {
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
		templateQuotaMarkConnection(t, s)
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
	templateQuotaReuseConnection(t, s, fault)
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
			o.Error = &Error{
				Code:    ErrInternal,
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
			for range 2 {
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
			for range 3 {
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
		(!strings.Contains(privateDiagnostic(err), "allocation absence commit") || !strings.Contains(privateDiagnostic(err), "FOREIGN KEY")) {
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
	if _, err = s.Wait(s.ctx, p, o.ID); !isCode(err, ErrUnknown) {
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
	if _, err = s.Wait(ctx, p, o.ID); !isCode(err, ErrUnknown) {
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
	for i := range 2 {
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
	if !isCode(err, ErrExhausted) {
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
		ErrConflict,
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
		templateQuotaMarkConnection(t, s)
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
		(!strings.Contains(templateQuotaOperationDiagnostic(t, s, o.ID), "template publication commit") ||
			!strings.Contains(templateQuotaOperationDiagnostic(t, s, o.ID), "FOREIGN KEY")) {
		t.Fatalf("not a real publication COMMIT failure: %v", err)
	}
	t.Logf("publication %s error=%v", fault, err)
	templateQuotaReuseConnection(t, s, fault)
	if !bytes.Equal(before, templateQuotaWithoutOperations(t, s)) {
		t.Fatal("publication fault partially moved catalog, holder or group")
	}
	stored, err := s.store.template(s.ctx, "publication")
	if err != nil || stored.SnapshotID != old.SnapshotID {
		t.Fatal("old ready catalog was lost", err)
	}
	failed, err := s.store.operation(s.ctx, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	original := encode(failed)
	if _, err = s.store.db.Exec(`DROP TRIGGER publication_fault`); err != nil {
		t.Fatal(err)
	}
	if err = s.recoverResources(); err != nil {
		t.Fatal(err)
	}
	current, err := s.store.operation(s.ctx, o.ID)
	if err != nil || !bytes.Equal(original, encode(current)) {
		t.Fatal("original publication failure changed", err)
	}
	// A new public intent publishes the replacement; the failed intent only
	// settles its private source responsibility and remains publicly failed.
	ledgerExecute(t, s, p, "publication-retry", command)
	s = ledgerRestart(t, s)
	current, err = s.store.operation(s.ctx, o.ID)
	if err != nil || !bytes.Equal(original, encode(current)) {
		t.Fatal("new publication rewrote old public failure", err)
	}
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
	if _, err := Open(opts); !isCode(err, ErrManualRecovery) {
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
	for range 2 {
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
			if _, err = openStore(dir, false); !isCode(err, ErrIncompatible) {
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
		ErrNotFound,
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
		ErrNotFound,
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
				ErrUnknown,
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
				ErrUnknown,
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
		if next, e := Open(opts); !isCode(e, ErrManualRecovery) {
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
				if next, e := Open(opts); !isCode(e, ErrManualRecovery) {
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
	if !isCode(err, ErrNotFound) {
		t.Fatalf("foreign published SID must be NotFound: %v", err)
	}
	ledgerMoney(t, s, p.TenantID, 1, 256)
}
