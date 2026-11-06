package sandboxd

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

// notFoundError asserts the uniform P12 response and returns it for
// body comparison against a reference rejection.
func notFoundError(t *testing.T, err error) *api.Error {
	t.Helper()
	var e *api.Error
	if !errors.As(err, &e) || e.Code != api.NotFound {
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
	); !isCode(err, api.Conflict) {
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
	); !isCode(err, api.NotFound) {
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
	); !isCode(err, api.Exhausted) {
		t.Fatalf("expected the injected build failure: %v", err)
	}
	if _, err := s.Submit(
		ctx,
		bob,
		"bob-take",
		build("flaky", "3"),
	); !isCode(err, api.NotFound) {
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
	); !isCode(err, api.NotFound) {
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
	if err = s.template(&stale); !isCode(err, api.Conflict) {
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
		api.ManualRecovery,
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
				code api.Code
			}{
				{"foreign", Principal{TenantID: "eve"}, api.NotFound},
				{"root-foreign-target", Principal{Root: true, TenantID: "eve"}, api.NotFound},
				{"owner", p, api.ManualRecovery},
				{"root-owner", Principal{Root: true, TenantID: p.TenantID}, api.ManualRecovery},
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
					if caller.code == api.NotFound &&
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
				code api.Code
			}{
				{"foreign", Principal{TenantID: "eve"}, api.NotFound},
				{"root-foreign", Principal{Root: true, TenantID: "eve"}, api.NotFound},
				{"owner", p, api.ManualRecovery},
				{"root-owner", Principal{Root: true, TenantID: p.TenantID}, api.ManualRecovery},
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
				if caller.code == api.NotFound &&
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
		p,
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
		if !isCode(err, api.NotFound) {
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
	); !isCode(err, api.Exhausted) {
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
	if _, err = Open(opts); !isCode(err, api.ManualRecovery) {
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
		); !isCode(err, api.NotFound) {
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
