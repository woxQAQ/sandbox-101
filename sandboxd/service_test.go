package sandboxd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sort"
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

func requireFence(t *testing.T, s *Service, id, operation string) {
	t.Helper()
	fence, err := configFence(context.Background(), s.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	if fence != operation {
		t.Fatalf("configuration fence = %q, want %q", fence, operation)
	}
}

type directTestEngine struct {
	*fakeEngine

	requestMu  sync.Mutex
	requests   []api.Request
	failRevoke bool
}

func (f *directTestEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.requestMu.Lock()
	f.requests = append(f.requests, r)
	fail := r.Action == "network_policy" && r.Policy == nil && f.failRevoke
	if fail {
		f.failRevoke = false
	}
	f.requestMu.Unlock()
	if r.Action != "network_policy" {
		return f.fakeEngine.Submit(ctx, r)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if old, ok := f.ops[r.Key]; ok {
		return old, nil
	}
	v, ok := f.vms[r.VMID]
	if !ok {
		return api.Operation{}, api.Fail(api.NotFound, "VM missing")
	}
	if v.Instance != r.ExpectedInstance {
		return api.Operation{}, api.Fail(api.Conflict, "instance changed")
	}
	o := api.Operation{
		ID:      r.Key,
		Key:     r.Key,
		VMID:    r.VMID,
		Request: r,
		Status:  "succeeded",
	}
	if fail || v.NetworkActual {
		o.Status = "failed"
		o.Error = &api.Error{
			Code:    api.Conflict,
			Message: "policy replacement failed",
		}
	} else {
		v.NetworkPolicy = r.Policy
		v.NetworkPolicyInstance = v.Instance
		f.vms[v.ID] = v
	}
	f.ops[r.Key] = o
	return o, nil
}

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

type recoveryTestEngine struct {
	*fakeEngine

	control                sync.Mutex
	failDeletes            int
	dropDeleteResponses    int
	dropDeleteBeforeAccept int
	failNetwork            bool
	failSnapshotDeletes    int
	snapshotDeleteUnknown  bool
	snapshotDeleteEntered  chan struct{}
	snapshotDeleteGate     <-chan struct{}
	snapshotEntered        chan struct{}
	snapshotGate           <-chan struct{}
	snapshotError          error
	findError              error
	requests               []api.Request
	deleteEntered          chan struct{}
	deleteGate             <-chan struct{}
}

func (f *recoveryTestEngine) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	f.mu.Lock()
	previous, cached := f.ops[r.Key]
	f.mu.Unlock()
	if cached {
		if !bytes.Equal(encode(previous.Request), encode(r)) {
			return previous, api.Fail(api.Conflict, "key parameters differ")
		}
		return previous, nil
	}
	f.control.Lock()
	f.requests = append(f.requests, r)
	dropBeforeAccept := r.Action == "delete" && f.dropDeleteBeforeAccept > 0
	if dropBeforeAccept {
		f.dropDeleteBeforeAccept--
	}
	dropDelete := r.Action == "delete" && f.dropDeleteResponses > 0
	if dropDelete {
		f.dropDeleteResponses--
	}
	fail := r.Action == "delete" && f.failDeletes > 0
	if fail {
		f.failDeletes--
	}
	if r.Action == "delete_snapshot" && f.failSnapshotDeletes > 0 {
		fail = true
		f.failSnapshotDeletes--
	}
	entered, gate := f.deleteEntered, f.deleteGate
	unknown := f.snapshotDeleteUnknown
	failNetwork := f.failNetwork
	snapshotEntered := f.snapshotEntered
	snapshotGate := f.snapshotGate
	if r.Action == "delete_snapshot" {
		entered, gate = f.snapshotDeleteEntered, f.snapshotDeleteGate
	}
	f.control.Unlock()
	if r.Action == "network" && failNetwork {
		return api.Operation{}, api.Fail(
			api.Internal,
			"network request failed before receipt",
		)
	}
	if r.Action == "delete_snapshot" && unknown {
		return api.Operation{}, api.Fail(api.Unknown, "delete response unknown")
	}
	if (r.Action == "delete" || r.Action == "delete_snapshot") &&
		entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return api.Operation{}, ctx.Err()
		case <-gate:
		}
	}
	if dropBeforeAccept {
		return api.Operation{}, api.Fail(
			api.Unknown,
			"delete transport failed before acceptance",
		)
	}
	if dropDelete {
		f.mu.Lock()
		f.ops[r.Key] = api.Operation{
			ID:      r.Key,
			Key:     r.Key,
			Request: r,
			VMID:    r.VMID,
			Status:  "running",
		}
		f.mu.Unlock()
		return api.Operation{}, api.Fail(
			api.Unknown,
			"accepted delete response lost",
		)
	}
	if fail {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			if !bytes.Equal(encode(old.Request), encode(r)) {
				return old, api.Fail(api.Conflict, "key parameters differ")
			}
			return old, nil
		}
		o := api.Operation{
			ID:         r.Key,
			Key:        r.Key,
			Request:    r,
			VMID:       r.VMID,
			SnapshotID: r.SnapshotID,
			Status:     "failed",
			Error: &api.Error{
				Code:    api.Internal,
				Message: "temporary delete failure",
			},
		}
		f.ops[r.Key] = o
		f.calls[r.Action]++
		return o, nil
	}
	if r.Action == "stop" || r.Action == "restore" {
		f.mu.Lock()
		defer f.mu.Unlock()
		if old, ok := f.ops[r.Key]; ok {
			if !bytes.Equal(encode(old.Request), encode(r)) {
				return old, api.Fail(api.Conflict, "key parameters differ")
			}
			return old, nil
		}
		v, ok := f.vms[r.VMID]
		if !ok {
			return api.Operation{}, api.Fail(api.NotFound, "not found")
		}
		if r.ExpectedInstance != "" && v.Instance != r.ExpectedInstance {
			return api.Operation{}, api.Fail(api.Conflict, "instance mismatch")
		}
		f.calls[r.Action]++
		if r.Action == "stop" {
			v.Phase, v.AgentReady = "stopped", false
		} else {
			v.Phase, v.AgentReady, v.Instance = "running", true, newID("run_")
		}
		v.NetworkActual, v.NetworkEnabled = false, false
		f.vms[v.ID] = v
		o := api.Operation{
			ID:         r.Key,
			Key:        r.Key,
			Request:    r,
			VMID:       v.ID,
			Status:     "succeeded",
			SnapshotID: r.SnapshotID,
		}
		if r.Action == "restore" {
			o.ResultInstance = v.Instance
		}
		f.ops[r.Key] = o
		return o, nil
	}
	o, err := f.fakeEngine.Submit(ctx, r)
	if r.Action == "snapshot" && snapshotEntered != nil {
		select {
		case snapshotEntered <- struct{}{}:
		default:
		}
		if snapshotGate != nil {
			select {
			case <-snapshotGate:
			case <-ctx.Done():
				return o, ctx.Err()
			}
		}
	}
	return o, err
}

func (f *recoveryTestEngine) ListSnapshots(
	ctx context.Context,
) ([]api.Snapshot, error) {
	f.control.Lock()
	err := f.snapshotError
	f.control.Unlock()
	if err != nil {
		return nil, err
	}
	return f.fakeEngine.ListSnapshots(ctx)
}

func (f *recoveryTestEngine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	f.control.Lock()
	err := f.findError
	f.control.Unlock()
	if err != nil {
		return api.Operation{}, err
	}
	return f.fakeEngine.FindOperation(ctx, key)
}

func recoveryFixture(
	t *testing.T,
) (*Service, *recoveryTestEngine, *fakeAgent, Principal) {
	t.Helper()
	s, engine, agent, p := fixture(t)
	// Keep the existing fixture, database and lifecycle implementation, but drive
	// reconciliation explicitly so fault injection has no timing dependency.
	s.cancel()
	s.wg.Wait()
	s.ctx, s.cancel = context.WithCancel(context.Background())
	f := &recoveryTestEngine{fakeEngine: engine}
	s.opts.Engine = f
	return s, f, agent, p
}

func recoverySandbox(t *testing.T, s *Service, id string) Sandbox {
	t.Helper()
	b, err := getSandbox(s.ctx, s.store.db, id)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

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
	hideNetwork         bool
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
		(e.hideStart && r.Action == "start") ||
		(e.hideNetwork && r.Action == "network" && (e.capture == nil || e.capture.Key == r.Key))
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
	hidden := (e.hideCapture || e.hideClone || e.hideStart || e.hideNetwork) &&
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
	o.hash = s.store.sign(o.ID)
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

func templateQuotaMarkConnection(t *testing.T, s *Service) {
	t.Helper()
	if _, err := s.store.db.Exec(
		`CREATE TEMP TABLE handoff_connection_marker(value INTEGER); INSERT INTO handoff_connection_marker VALUES(1)`,
	); err != nil {
		t.Fatal(err)
	}
}

func templateQuotaReuseConnection(t *testing.T, s *Service, fault string) {
	t.Helper()
	if fault != "commit" {
		return
	}
	var marker, enabled int
	if err := s.store.db.QueryRow(`SELECT value FROM handoff_connection_marker`).
		Scan(&marker); err != nil ||
		marker != 1 {
		t.Fatal("physical SQLite connection replaced after COMMIT error", err)
	}
	if err := s.store.db.QueryRow(`PRAGMA foreign_keys`).
		Scan(&enabled); err != nil ||
		enabled != 1 {
		t.Fatal("foreign keys disabled", err)
	}
	tx, err := s.store.db.BeginTx(s.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(
		s.ctx,
		`UPDATE handoff_connection_marker SET value=2`,
	); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal("same-connection ordinary transaction failed", err)
	}
	if err = s.store.db.QueryRow(`SELECT value FROM handoff_connection_marker`).
		Scan(&marker); err != nil ||
		marker != 2 {
		t.Fatal(err)
	}
	if _, err = s.store.db.Exec(
		`UPDATE handoff_connection_marker SET value=1`,
	); err != nil {
		t.Fatal(err)
	}
	t.Log(
		"deferred COMMIT failed; same physical SQLite connection queried and committed an ordinary transaction",
	)
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
		templateQuotaMarkConnection(t, s)
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
	case "fallback", "fallback-destination":
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
		table, event, condition := "quota_resources", "DELETE", "OLD.kind='vm_disk'"
		if stage == "fallback-destination" {
			table, event, condition = "sandboxes", "UPDATE", "COALESCE(json_extract(OLD.body,'$.vm_id'),'')!='' AND COALESCE(json_extract(NEW.body,'$.vm_id'),'')=''"
		}
		templateQuotaSQLFault(t, s, table, event, condition, fault)
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
			templateQuotaReuseConnection(t, s, fault)
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
	templateQuotaReuseConnection(t, s, fault)
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

func templateQuotaWithoutOperations(t *testing.T, s *Service) []byte {
	t.Helper()
	var state map[string][]string
	if err := json.Unmarshal(templateQuotaState(t, s), &state); err != nil {
		t.Fatal(err)
	}
	delete(state, "operations")
	return encode(state)
}

// Attempts are counted before strict rejection, independently of physical effects.
type templateQuotaSelectionClient struct {
	api.Client

	mu       sync.Mutex
	requests []api.Request
	finds    []string
	unknown  bool
	forged   *api.Operation
	findHook func(string)
}

func (e *templateQuotaSelectionClient) Submit(
	ctx context.Context,
	r api.Request,
) (api.Operation, error) {
	e.mu.Lock()
	e.requests = append(e.requests, templateQuotaCopy(r))
	unknown := e.unknown
	e.mu.Unlock()
	if r.Action == "delete" {
		if r.ExpectedInstance == "" {
			return api.Operation{}, api.Fail(
				api.Invalid,
				"strict empty predicate",
			)
		}
		if unknown {
			return api.Operation{}, api.Fail(
				api.Unknown,
				"delete transport unknown before acceptance",
			)
		}
	}
	return e.Client.Submit(ctx, r)
}

func (e *templateQuotaSelectionClient) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	e.mu.Lock()
	e.finds = append(e.finds, key)
	forged := templateQuotaCopy(e.forged)
	hook := e.findHook
	e.mu.Unlock()
	if hook != nil {
		hook(key)
	}
	if forged != nil && strings.Contains(key, ":step:warm-reject:") {
		return *forged, nil
	}
	return e.Client.FindOperation(ctx, key)
}

func (e *templateQuotaSelectionClient) counts() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.requests), len(e.finds)
}

func templateQuotaReadOnlyState(t *testing.T, opts Options) []byte {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(opts.StateDir, "sandboxd.db")+"?mode=ro",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	return templateQuotaState(
		t,
		&Service{ctx: context.Background(), store: &store{db: db}},
	)
}

type rejectWarmAgent struct {
	fakeAgent

	vm string
}

func (a *rejectWarmAgent) Activate(
	ctx context.Context,
	vm api.VM,
	activation Activation,
) error {
	if vm.ID == a.vm {
		return errors.New("warm guest no longer responds")
	}
	return a.fakeAgent.Activate(ctx, vm, activation)
}

func warmFallbackFixture(
	t *testing.T,
	golden bool,
) (*Service, *fakeEngine, Principal, Template) {
	t.Helper()
	engine := newFakeEngine()
	template := Template{
		ID:         "python",
		TenantID:   "alice",
		Image:      "sha256:" + strings.Repeat("1", 64),
		Shape:      api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
		TargetWarm: 1,
	}
	config := api.Config{Image: template.Image, Shape: template.Shape}
	if golden {
		template.SnapshotID = "golden"
		engine.snaps[template.SnapshotID] = api.Snapshot{
			ID:        template.SnapshotID,
			Config:    config,
			Published: true,
		}
	}
	var created, started api.Operation
	var err error
	if !golden {
		created, err = api.Wait(
			context.Background(),
			engine,
			api.Request{
				Key: stepKey(
					Operation{ID: "fixture-warm"},
					"fixture-create",
				),
				Action: "create",
				Config: &config,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		started, err = api.Wait(
			context.Background(),
			engine,
			api.Request{
				Key:    stepKey(Operation{ID: "fixture-warm"}, "fixture-start"),
				Action: "start",
				VMID:   created.VMID,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	state := t.TempDir()
	st, err := openStore(state, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	// Exercise durable lifecycle replay without background pool refills changing
	// the engine allocation counts being asserted by these tests.
	service := &Service{
		opts: Options{
			StateDir:  state,
			RootToken: "root-secret",
			Engine:    engine,
			Agent:     &rejectWarmAgent{vm: created.VMID},
			Egress:    &fakeEgress{},
			MaxTTL:    time.Hour,
			MaxWarm:   32,
		},
		store:       st,
		ctx:         ctx,
		cancel:      cancel,
		epoch:       "test-daemon",
		locks:       map[string]*sync.Mutex{},
		active:      map[string]chan struct{}{},
		secrets:     map[string]map[string]egress.Secret{},
		opSecrets:   map[string]map[string]egress.Secret{},
		bindings:    map[string]string{},
		connections: map[string]map[net.Conn]struct{}{},
	}
	t.Cleanup(func() { service.Close() })
	principal := Principal{TenantID: "alice"}
	if _, err = service.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            "alice",
			Token:         "alice-token",
			MaxSandboxes:  5,
			MaxStorageMiB: 1 << 20,
		},
	); err != nil {
		t.Fatal(err)
	}
	importer := Principal{Root: true, TenantID: principal.TenantID}
	if _, err = service.Execute(
		context.Background(),
		importer,
		"template",
		Command{Action: "template_put", Template: &template},
	); err != nil {
		t.Fatal(err)
	}
	if golden {
		service.pools()
		service.wg.Wait()
		if err = service.store.db.QueryRow(`SELECT vm_id FROM warm WHERE template_id=?`, template.ID).
			Scan(&engine.warmCandidate); err != nil {
			t.Fatal(err)
		}
	} else {
		seedTemplateQuotaWarm(t, service, template, created, started)
		engine.warmCandidate = created.VMID
	}
	service.opts.Agent.(*rejectWarmAgent).vm = engine.warmCandidate
	engine.mu.Lock()
	engine.calls = map[string]int{}
	engine.mu.Unlock() // Count replacement work after the real ready fixture.
	return service, engine, principal, template
}
