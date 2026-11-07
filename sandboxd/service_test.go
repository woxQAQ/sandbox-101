package sandboxd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
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

type fakeEngine struct {
	mu              sync.Mutex
	ops             map[string]api.Operation
	vms             map[string]api.VM
	snaps           map[string]api.Snapshot
	calls           map[string]int
	generation      map[string]int
	warmCandidate   string
	physicalDeletes int
	failSnapshot    bool
	failNetwork     bool
	unavailable     bool
	cloneGate       <-chan struct{}
	sockDir         string
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		ops:        map[string]api.Operation{},
		vms:        map[string]api.VM{},
		snaps:      map[string]api.Snapshot{},
		calls:      map[string]int{},
		generation: map[string]int{},
	}
}

// Callers hold f.mu. Real egress fixtures need listener paths inside a
// private directory; the default keeps the historical non-listening path.
func (f *fakeEngine) vsockPath(id string) string {
	if f.sockDir != "" {
		return filepath.Join(f.sockDir, "vsock-"+id)
	}
	return "/tmp/" + id + "/vsock"
}

func (f *fakeEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.mu.Lock()
	if o, ok := f.ops[r.Key]; ok {
		f.mu.Unlock()
		if !bytes.Equal(encode(o.Request), encode(r)) {
			return o, api.Fail(
				api.Conflict,
				"key already belongs to different parameters",
			)
		}
		return o, nil
	}
	gate := f.cloneGate
	f.mu.Unlock()
	if r.Action == "clone" && gate != nil {
		select {
		case <-ctx.Done():
			return api.Operation{}, ctx.Err()
		case <-gate:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if o, ok := f.ops[r.Key]; ok {
		if !bytes.Equal(encode(o.Request), encode(r)) {
			return o, api.Fail(
				api.Conflict,
				"key already belongs to different parameters",
			)
		}
		return o, nil
	}
	if r.Action == "delete" {
		if _, ok := f.vms[r.VMID]; !ok {
			return api.Operation{}, api.Fail(api.NotFound, "VM not found")
		}
	}
	f.calls[r.Action]++
	o := api.Operation{
		ID:         r.Key,
		Key:        r.Key,
		Request:    r,
		Status:     "succeeded",
		VMID:       r.VMID,
		SnapshotID: r.SnapshotID,
	}
	v := f.vms[r.VMID]
	if r.ExpectedInstance != "" && v.Instance != r.ExpectedInstance {
		return api.Operation{}, api.Fail(api.Conflict, "instance mismatch")
	}
	switch r.Action {
	case "create":
		o.VMID = newID("vm_")
		f.vms[o.VMID] = api.VM{
			ID:       o.VMID,
			Config:   *r.Config,
			Phase:    "created",
			Verified: true,
		}
	case "start":
		v.Phase = "running"
		v.AgentReady = true
		v.Verified = true
		v.Instance = newID("run_")
		v.VSockPath = f.vsockPath(v.Instance)
		f.vms[v.ID] = v
	case "clone":
		snap, ok := f.snaps[r.SnapshotID]
		if !ok {
			return o, api.Fail(api.NotFound, "snapshot missing")
		}
		o.VMID = newID("vm_")
		f.generation[o.VMID] = f.generation[r.SnapshotID]
		f.vms[o.VMID] = api.VM{
			ID:         o.VMID,
			Config:     snap.Config,
			Phase:      "running",
			Verified:   true,
			AgentReady: true,
			Instance:   newID("run_"),
			VSockPath:  f.vsockPath(o.VMID),
		}
	case "stop":
		v.Phase = "stopped"
		v.AgentReady = false
		f.vms[v.ID] = v
	case "restore":
		if _, ok := f.snaps[r.SnapshotID]; !ok {
			return o, api.Fail(api.NotFound, "snapshot missing")
		}
		v.Phase = "running"
		v.AgentReady = true
		v.Verified = true
		v.Instance = newID("run_")
		v.VSockPath = f.vsockPath(v.Instance)
		f.vms[v.ID] = v
	case "snapshot":
		if f.failSnapshot {
			o.Status = "failed"
			o.Error = &api.Error{Code: api.Exhausted, Message: "disk full"}
			break
		}
		o.SnapshotID = newID("snap_")
		f.generation[o.SnapshotID] = f.generation[r.VMID]
		f.snaps[o.SnapshotID] = api.Snapshot{
			ID:        o.SnapshotID,
			Config:    v.Config,
			Published: true,
		}
	case "delete":
		f.physicalDeletes++
		delete(f.vms, r.VMID)
	case "delete_snapshot":
		delete(f.snaps, r.SnapshotID)
	case "network":
		if f.failNetwork {
			o.Status = "failed"
			o.Error = &api.Error{
				Code:    api.Internal,
				Message: "network change failed",
			}
			break
		}
		v.NetworkActual = r.NetworkEnabled
		v.NetworkEnabled = r.NetworkEnabled
		f.vms[v.ID] = v
	default:
		return o, fmt.Errorf("unexpected action %s", r.Action)
	}
	if o.Status == "succeeded" &&
		(r.Action == "start" || r.Action == "restore" || r.Action == "clone") {
		o.ResultInstance = f.vms[o.VMID].Instance
	}
	f.ops[r.Key] = o
	return o, nil
}

func (f *fakeEngine) GetOperation(
	ctx context.Context,
	id string,
) (api.Operation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.ops[id]
	if !ok {
		return o, api.Fail(api.NotFound, "not found")
	}
	return o, nil
}

func (f *fakeEngine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	return f.GetOperation(ctx, key)
}

func (f *fakeEngine) GetVM(ctx context.Context, id string) (api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return api.VM{}, errors.New("query unavailable")
	}
	v, ok := f.vms[id]
	if !ok {
		return v, api.Fail(api.NotFound, "not found")
	}
	return v, nil
}

func (f *fakeEngine) ListVMs(ctx context.Context) ([]api.VM, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unavailable {
		return nil, errors.New("query unavailable")
	}
	var all []api.VM
	for _, v := range f.vms {
		all = append(all, v)
	}
	return all, nil
}

func (f *fakeEngine) GetSnapshot(
	ctx context.Context,
	id string,
) (api.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.snaps[id]
	if !ok {
		return v, api.Fail(api.NotFound, "not found")
	}
	return v, nil
}

func (f *fakeEngine) ListSnapshots(
	ctx context.Context,
) ([]api.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var all []api.Snapshot
	for _, v := range f.snaps {
		all = append(all, v)
	}
	return all, nil
}

func (f *fakeEngine) count(action string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[action]
}

type fakeAgent struct {
	mu              sync.Mutex
	activations     []Activation
	fail            bool
	failInfo        bool
	infoEntered     chan<- struct{}
	infoGate        <-chan struct{}
	activateEntered chan<- struct{}
	activateGate    <-chan struct{}
}

func (f *fakeAgent) Info(ctx context.Context, _ api.VM) error {
	f.mu.Lock()
	entered, gate := f.infoEntered, f.infoGate
	f.infoEntered, f.infoGate = nil, nil
	fail := f.failInfo
	f.mu.Unlock()
	if entered != nil {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate:
		}
	}
	if fail {
		return errors.New("agent info probe failed")
	}
	return nil
}

func (f *fakeAgent) Activate(
	ctx context.Context,
	_ api.VM,
	a Activation,
) error {
	f.mu.Lock()
	entered, gate := f.activateEntered, f.activateGate
	f.activateEntered, f.activateGate = nil, nil
	f.mu.Unlock()
	if entered != nil {
		close(entered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gate:
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("activation failed")
	}
	f.activations = append(f.activations, a)
	return nil
}

func (f *fakeAgent) PrepareTemplate(
	context.Context,
	api.VM,
) error {
	return nil
}

func (f *fakeAgent) Exec(
	context.Context,
	api.VM,
	string,
	string,
) error {
	return nil
}

func (f *fakeAgent) Connect(context.Context, api.VM) (net.Conn, error) {
	return nil, errors.New("not used")
}

func (f *fakeAgent) Forward(context.Context, api.VM, int) (net.Conn, error) {
	return nil, errors.New("not used")
}

type fakeEgress struct {
	mu       sync.Mutex
	bindings map[string]string
}

func (f *fakeEgress) Bind(
	_ context.Context,
	id, tenant, path string,
	p egress.Policy,
	secrets map[string]egress.Secret,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.bindings == nil {
		f.bindings = map[string]string{}
	}
	if _, ok := f.bindings[id]; ok {
		return errors.New("already bound")
	}
	f.bindings[id] = path
	return nil
}

func (f *fakeEgress) Revoke(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.bindings, id)
	return nil
}
func (f *fakeEgress) Close() error { return nil }
func fixture(t *testing.T) (*Service, *fakeEngine, *fakeAgent, Principal) {
	t.Helper()
	f := newFakeEngine()
	a := &fakeAgent{}
	s, err := Open(
		Options{
			StateDir:   t.TempDir(),
			Initialize: true,
			RootToken:  "root-secret",
			Engine:     f,
			Agent:      a,
			Egress:     &fakeEgress{},
			Interval:   time.Hour,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	tenant, err := s.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            "alice",
			MaxSandboxes:  50,
			MaxStorageMiB: 1 << 20,
			Token:         "alice-token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	p := Principal{TenantID: tenant.ID}
	_, err = s.Execute(
		context.Background(),
		p,
		"template",
		Command{
			Action: "template_put",
			Template: &Template{
				ID:    "python",
				Image: "sha256:" + strings.Repeat("1", 64),
				Shape: api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return s, f, a, p
}

func claimTest(
	t *testing.T,
	s *Service,
	p Principal,
	key string,
	env map[string]EnvVar,
) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := s.Execute(
		ctx,
		p,
		key,
		Command{
			Action:     "claim",
			TemplateID: "python",
			Env:        env,
			Policy:     egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSandboxTokenCannotReadForkOperation(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	sp := Principal{TenantID: p.TenantID, SandboxID: source.Sandbox.ID}
	if _, err := s.Operation(
		context.Background(),
		sp,
		r.OperationID,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatalf("fork operation visible to source: %v", err)
	}
	h := s.Handler()
	get := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/v1/operations/"+id, nil)
		req.Header.Set(
			"Authorization",
			"Bearer "+source.Tokens[source.Sandbox.ID],
		)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	foreign, missing := get(r.OperationID), get("missing")
	if foreign.Code != 404 || foreign.Body.String() != missing.Body.String() {
		t.Fatalf(
			"foreign response differs: %d %s",
			foreign.Code,
			foreign.Body.String(),
		)
	}
}

func TestSandboxTokenCanPollOwnLeaseOperations(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	sp := Principal{TenantID: p.TenantID, SandboxID: source.Sandbox.ID}
	for _, action := range []string{"renew", "release"} {
		o, err := s.Submit(
			context.Background(),
			sp,
			action,
			Command{Action: action, SandboxID: sp.SandboxID, TTLSeconds: 60},
		)
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Wait(context.Background(), sp, o.ID)
		if err != nil {
			t.Fatal(err)
		}
		if action == "release" && len(r.Tokens) != 0 {
			t.Fatal("terminated token issued")
		}
		for _, other := range []Principal{{TenantID: p.TenantID, SandboxID: "other"}, {TenantID: "other"}} {
			if _, err := s.Operation(
				context.Background(),
				other,
				o.ID,
			); !isCode(
				err,
				api.NotFound,
			) {
				t.Fatalf("foreign read: %v", err)
			}
		}
	}
}

func TestOperationResultTokensRespectPrincipal(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.store.operation(context.Background(), r.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	o.Result.Tokens = map[string]string{"forged": "credential"}
	if err := saveOp(context.Background(), s.store.db, o); err != nil {
		t.Fatal(err)
	}
	for _, principal := range []Principal{p, {Root: true}} {
		got, err := s.Wait(context.Background(), principal, o.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Tokens) != 2 || got.Tokens["forged"] != "" {
			t.Fatal("tokens were not rebuilt")
		}
	}
	for _, credential := range []string{"alice-token", "root-secret"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/operations/"+o.ID, nil)
		req.Header.Set("Authorization", "Bearer "+credential)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var got Operation
		if err := json.Unmarshal(
			w.Body.Bytes(),
			&got,
		); err != nil || w.Code != 200 || len(got.Result.Tokens) != 2 ||
			got.Result.Tokens["forged"] != "" {
			t.Fatalf(
				"HTTP result tokens were not rebuilt: status=%d decode=%v",
				w.Code,
				err,
			)
		}
	}
	o.Result.Sandboxes[0].TenantID = "foreign"
	if err := saveOp(context.Background(), s.store.db, o); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Operation(
		context.Background(),
		p,
		o.ID,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatalf("foreign result exposed: %v", err)
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
		api.NotFound,
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
		api.NotFound,
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
		api.NotFound,
	) {
		t.Fatal("root transferred ownership", err)
	}
	if f.count("clone") != 0 {
		t.Fatal("import cloned before authorization")
	}
}

func TestClaimIdempotencyAndTenantIsolation(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim-1", nil)
	again := claimTest(t, s, p, "claim-1", nil)
	if r.Sandbox.ID != again.Sandbox.ID ||
		r.Tokens[r.Sandbox.ID] != again.Tokens[r.Sandbox.ID] ||
		f.count("create") != 1 {
		t.Fatal("retry created a second sandbox or changed token")
	}
	_, err := s.Execute(
		context.Background(),
		p,
		"claim-1",
		Command{Action: "claim", TemplateID: "python", TTLSeconds: 20},
	)
	if !isCode(err, api.Conflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	h := s.Handler()
	foreign := httptest.NewRequest(
		http.MethodGet,
		"/v1/sandboxes/"+r.Sandbox.ID,
		nil,
	)
	foreign.Header.Set("Authorization", "Bearer unknown")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, foreign)
	missing := httptest.NewRequest(http.MethodGet, "/v1/sandboxes/missing", nil)
	missing.Header.Set("Authorization", "Bearer alice-token")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, missing)
	if w1.Code != 404 || w2.Code != 404 ||
		w1.Body.String() != w2.Body.String() {
		t.Fatal("foreign and missing responses differ")
	}
	principal, err := s.Authenticate(
		context.Background(),
		r.Tokens[r.Sandbox.ID],
	)
	if err != nil || principal.SandboxID != r.Sandbox.ID {
		t.Fatal("sandbox token rejected")
	}
	if _, err = s.Execute(
		context.Background(),
		principal,
		"forbidden-claim",
		Command{Action: "claim", TemplateID: "python"},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("sandbox token created resources")
	}
}

func TestPauseFailurePreservesVMAndZeroWakeRelease(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	f.mu.Lock()
	f.failSnapshot = true
	f.mu.Unlock()
	_, err := s.Execute(
		context.Background(),
		p,
		"pause-fails",
		Command{Action: "pause", SandboxID: id},
	)
	if !isCode(err, api.Exhausted) {
		t.Fatalf("expected exhaustion: %v", err)
	}
	b, _ := s.Get(context.Background(), p, id)
	if b.State != "running" || f.count("delete") != 0 {
		t.Fatal("failed hibernate destroyed state")
	}
	f.mu.Lock()
	f.failSnapshot = false
	f.mu.Unlock()
	if _, err = s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	b, _ = s.Get(context.Background(), p, id)
	if b.State != "paused" || b.VMID != "" || b.SnapshotID == "" {
		t.Fatal("pause did not free VM")
	}
	before := f.count("clone")
	if _, err = s.Execute(
		context.Background(),
		p,
		"release",
		Command{Action: "release", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	if f.count("clone") != before || f.count("delete_snapshot") != 1 {
		t.Fatal("paused release woke source or leaked snapshot")
	}
}

func TestConcurrentAutoResumeHasOneCloneAndPreservesIdentity(t *testing.T) {
	s, f, a, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	id := r.Sandbox.ID
	if _, err := s.Execute(
		context.Background(),
		p,
		"pause",
		Command{Action: "pause", SandboxID: id},
	); err != nil {
		t.Fatal(err)
	}
	before := f.count("clone")
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for range 20 {
		wg.Go(
			func() { ; _, err := s.DataReady(context.Background(), p, id); errs <- err },
		)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.count("clone")-before != 1 {
		t.Fatal("concurrent wake cloned more than once")
	}
	a.mu.Lock()
	last := a.activations[len(a.activations)-1]
	a.mu.Unlock()
	if last.ResetIdentity {
		t.Fatal("resume reset guest identity")
	}
}

func TestHostSecretNeverPersistedAndRestartFailsClosed(t *testing.T) {
	s, f, a, p := fixture(t)
	no := false
	secret := "a-secret-that-must-not-exist-on-disk-123"
	r, err := s.Execute(
		context.Background(),
		p,
		"claim",
		Command{
			Action:     "claim",
			TemplateID: "python",
			Policy:     egress.Policy{Mode: "relay"},
			Env: map[string]EnvVar{
				"TOKEN": {
					Value: secret,
					Guest: &no,
					Inject: &Injection{
						Hosts:       []string{"api.example.com"},
						Header:      "Authorization",
						Placeholder: "phantom",
					},
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	act := a.activations[len(a.activations)-1]
	a.mu.Unlock()
	if act.Env["TOKEN"] != "phantom" {
		t.Fatal("secret exposed in activation")
	}
	if err := s.store.db.QueryRow("SELECT body FROM operations WHERE id=?", r.OperationID).
		Scan(new([]byte)); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(s.opts.StateDir, "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		b, _ := os.ReadFile(file)
		if strings.Contains(string(b), secret) {
			t.Fatalf("host secret persisted in %s", file)
		}
	}
	opts := s.opts
	opts.Initialize = false
	s.Close()
	s2, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err = s2.DataReady(
		context.Background(),
		p,
		r.Sandbox.ID,
	); !isCode(
		err,
		api.Conflict,
	) {
		t.Fatalf("restart served sandbox without credentials: %v", err)
	}
	_ = f
}

func TestForkIdentityAndEnvironmentIsolation(t *testing.T) {
	s, _, a, p := fixture(t)
	source := claimTest(
		t,
		s,
		p,
		"claim",
		map[string]EnvVar{"OLD": {Value: "source-only"}},
	)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{
			Action:    "fork",
			SandboxID: source.Sandbox.ID,
			Count:     2,
			Env:       map[string]EnvVar{"NEW": {Value: "branch"}},
			Policy:    egress.Policy{Mode: "none"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Sandboxes) != 2 || r.Sandboxes[0].ID == r.Sandboxes[1].ID ||
		r.Tokens[r.Sandboxes[0].ID] == r.Tokens[r.Sandboxes[1].ID] {
		t.Fatal("fork did not allocate independent branches")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, act := range a.activations[len(a.activations)-2:] {
		if !act.ResetIdentity || act.Env["OLD"] != "" ||
			act.Env["NEW"] != "branch" {
			t.Fatal("fork leaked source environment or identity")
		}
	}
}

func TestConcurrentQuotaAdmission(t *testing.T) {
	s, _, _, p := fixture(t)
	_, err := s.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			MaxSandboxes:  2,
			MaxStorageMiB: 10000,
			Token:         "alice-token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.Execute(
				context.Background(),
				p,
				fmt.Sprint("claim", i),
				Command{
					Action:     "claim",
					TemplateID: "python",
					Policy:     egress.Policy{Mode: "none"},
				},
			)
			if e == nil {
				mu.Lock()
				success++
				mu.Unlock()
			} else if !isCode(e, api.Exhausted) {
				t.Errorf("unexpected admission error %v", e)
			}
		}(i)
	}
	wg.Wait()
	if success != 2 {
		t.Fatalf("quota admitted %d", success)
	}
}

func TestPreviewBoundToEpochAndLease(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	u, err := s.PreviewURL(
		context.Background(),
		p,
		r.Sandbox.ID,
		8080,
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(strings.TrimPrefix(u, "/preview/"), ".")
	if len(parts) != 2 {
		t.Fatal("unsigned preview")
	}
	_, err = s.Execute(
		context.Background(),
		p,
		"release",
		Command{Action: "release", SandboxID: r.Sandbox.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", u, nil))
	if w.Code != 404 {
		t.Fatalf("released preview accepted: %d", w.Code)
	}
}

func TestReconcileQueryFailureDoesNotEraseState(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	f.mu.Lock()
	f.unavailable = true
	f.mu.Unlock()
	s.reconcile()
	b, err := s.Get(context.Background(), p, r.Sandbox.ID)
	if err != nil || b.State != "running" {
		t.Fatal("query failure treated as resource absence")
	}
}

func TestStoreWriterGuardAndMissingDBRecovery(t *testing.T) {
	dir := t.TempDir()
	s, err := openStore(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = openStore(dir, false); !isCode(err, api.Conflict) {
		t.Fatal("second writer allowed")
	}
	s.close()
	if err = os.Remove(filepath.Join(dir, "sandboxd.db")); err != nil {
		t.Fatal(err)
	}
	if _, err = openStore(dir, true); !isCode(err, api.ManualRecovery) {
		t.Fatal("lost DB silently reinitialized")
	}
}

func TestOperationJSONDoesNotExposeEnv(t *testing.T) {
	b, _ := json.Marshal(Sandbox{Env: map[string]string{"PASSWORD": "secret"}})
	if strings.Contains(string(b), "PASSWORD") ||
		strings.Contains(string(b), `"secret"`) {
		t.Fatal("sandbox API exposes env")
	}
}
