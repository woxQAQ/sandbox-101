package sandboxd_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/woxQAQ/sandbox-engine/api"
	pb "github.com/woxQAQ/sandbox-protocol/gen"
	"github.com/woxQAQ/sandboxd"
	"github.com/woxQAQ/sandboxd/agent"
	"github.com/woxQAQ/sandboxd/egress"
	"github.com/woxQAQ/sandboxd/sdk"
)

// These components exercise the real HTTP/auth/service/SQLite boundary. The
// engine models immutable receipts and physical identities, not guest COW or TCP.
func ownerCopy[T any](value T) T {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	var out T
	if err = json.Unmarshal(raw, &out); err != nil {
		panic(err)
	}
	return out
}

type ownerGate struct {
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	enteredOnce sync.Once
}

func newOwnerGate() *ownerGate {
	return &ownerGate{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}
func (g *ownerGate) open() { g.once.Do(func() { close(g.release) }) }
func (g *ownerGate) wait(ctx context.Context) error {
	g.enteredOnce.Do(func() { close(g.entered) })
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type ownerPhysical struct {
	VM       api.VM
	Disks    map[string]bool
	Backings map[string]bool
}
type ownerEngine struct {
	mu                 sync.Mutex
	next               int
	ops                map[string]api.Operation
	vms                map[string]ownerPhysical
	snaps              map[string]api.Snapshot
	calls              map[string]int
	snapshotQueryError bool
	gateMu             sync.Mutex
	inventory          int
	quiet              *ownerGate
	deletion           *ownerGate
}

func newOwnerEngine() *ownerEngine {
	return &ownerEngine{
		ops:   map[string]api.Operation{},
		vms:   map[string]ownerPhysical{},
		snaps: map[string]api.Snapshot{},
		calls: map[string]int{},
	}
}

func (e *ownerEngine) id(
	prefix string,
) string {
	e.next++
	return fmt.Sprintf("%s%d", prefix, e.next)
}

func (e *ownerEngine) Submit(
	ctx context.Context,
	input api.Request,
) (api.Operation, error) {
	// Gates never hold the engine mutex while Service transactions or RO queries run.
	e.gateMu.Lock()
	gate := e.deletion
	e.gateMu.Unlock()
	if gate != nil && input.Action == "delete" &&
		strings.HasSuffix(input.Key, ":step:hibernate-delete") {
		if err := gate.wait(ctx); err != nil {
			return api.Operation{}, err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	r := ownerCopy(input)
	if old, ok := e.ops[r.Key]; ok {
		if !reflect.DeepEqual(old.Request, r) {
			return api.Operation{}, api.Fail(
				api.Conflict,
				"immutable request mismatch",
			)
		}
		return ownerCopy(old), nil
	}
	if r.Key == "" || len(r.Key) > 256 {
		return api.Operation{}, api.Fail(api.Invalid, "invalid key")
	}
	p, exists := e.vms[r.VMID]
	if r.Action != "create" && r.Action != "clone" &&
		r.Action != "delete_snapshot" {
		if !exists {
			return api.Operation{}, api.Fail(api.NotFound, "VM absent")
		}
		if r.ExpectedInstance != "" && r.ExpectedInstance != p.VM.Instance {
			return api.Operation{}, api.Fail(api.Conflict, "instance mismatch")
		}
	}
	var snap api.Snapshot
	if r.Action == "clone" || r.Action == "restore" {
		var ok bool
		snap, ok = e.snaps[r.SnapshotID]
		if !ok {
			return api.Operation{}, api.Fail(api.NotFound, "snapshot absent")
		}
		if !snap.Published || snap.Deleting {
			return api.Operation{}, api.Fail(
				api.Conflict,
				"snapshot unavailable",
			)
		}
		if r.Action == "clone" && r.VMID != "" {
			return api.Operation{}, api.Fail(api.Invalid, "clone allocates VM")
		}
	}
	o := api.Operation{
		ID:         e.id("receipt-"),
		Key:        r.Key,
		Request:    r,
		Status:     "succeeded",
		Step:       "complete",
		VMID:       r.VMID,
		SnapshotID: r.SnapshotID,
	}
	switch r.Action {
	case "create":
		if r.Config == nil || r.VMID != "" {
			return api.Operation{}, api.Fail(api.Invalid, "create config")
		}
		o.VMID = e.id("vm-")
		p = ownerPhysical{
			VM: api.VM{
				ID:       o.VMID,
				Config:   ownerCopy(*r.Config),
				Phase:    "created",
				Verified: true,
			},
			Disks:    map[string]bool{o.VMID: true},
			Backings: map[string]bool{},
		}
	case "start", "clone", "restore":
		if r.Action == "start" && p.VM.Phase != "created" {
			return api.Operation{}, api.Fail(api.Conflict, "start phase")
		}
		if r.Action == "restore" && p.VM.Phase != "stopped" {
			return api.Operation{}, api.Fail(
				api.Conflict,
				"restore requires stop",
			)
		}
		if r.Action == "clone" {
			o.VMID = e.id("vm-")
			p = ownerPhysical{
				VM:       api.VM{ID: o.VMID, Config: ownerCopy(snap.Config)},
				Disks:    map[string]bool{o.VMID: true},
				Backings: map[string]bool{},
			}
		}
		if r.Action == "restore" {
			p.Disks[p.VM.ID+":restore:"+o.ID] = true
		}
		if r.Action != "start" {
			p.Backings[snap.ID] = true
		}
		p.VM.Phase = "running"
		p.VM.Verified = true
		p.VM.AgentReady = true
		p.VM.Instance = e.id("instance-")
		p.VM.VSockPath = "synthetic:" + p.VM.ID + ":" + p.VM.Instance
		p.VM.NetworkActual = false
		p.VM.NetworkEnabled = false
		p.VM.NetworkPolicy = nil
		p.VM.NetworkPolicyInstance = ""
		o.ResultInstance = p.VM.Instance
	case "snapshot":
		if r.ExpectedInstance == "" || p.VM.Phase != "running" ||
			!p.VM.Verified ||
			!p.VM.AgentReady {
			return api.Operation{}, api.Fail(api.Conflict, "capture identity")
		}
		o.SnapshotID = e.id("sid-")
		e.snaps[o.SnapshotID] = api.Snapshot{
			ID:         o.SnapshotID,
			SourceVMID: p.VM.ID,
			Config:     ownerCopy(p.VM.Config),
			Published:  true,
		}
	case "stop":
		if r.ExpectedInstance == "" {
			return api.Operation{}, api.Fail(api.Invalid, "stop identity")
		}
		p.VM.Phase = "stopped"
		p.VM.AgentReady = false
		p.VM.NetworkActual = false
		p.VM.NetworkEnabled = false
	case "network", "network_policy":
		if r.ExpectedInstance == "" || p.VM.Phase != "running" ||
			!p.VM.Verified {
			return api.Operation{}, api.Fail(api.Conflict, "network identity")
		}
		if r.Action == "network_policy" {
			if p.VM.NetworkActual || p.VM.NetworkEnabled {
				return api.Operation{}, api.Fail(
					api.Conflict,
					"policy requires NIC off",
				)
			}
			p.VM.NetworkPolicy = ownerCopy(r.Policy)
			p.VM.NetworkPolicyInstance = ""
			if r.Policy != nil {
				p.VM.NetworkPolicyInstance = p.VM.Instance
			}
		} else {
			if r.NetworkEnabled &&
				(p.VM.NetworkPolicy == nil || p.VM.NetworkPolicyInstance != p.VM.Instance) {
				return api.Operation{}, api.Fail(
					api.Conflict,
					"network requires current policy",
				)
			}
			p.VM.NetworkActual = r.NetworkEnabled
			p.VM.NetworkEnabled = r.NetworkEnabled
		}
	case "delete":
		if (strings.Contains(r.Key, "hibernate-delete") || strings.Contains(r.Key, "release-vm")) &&
			r.ExpectedInstance == "" {
			return api.Operation{}, api.Fail(api.Invalid, "delete identity")
		}
		// Removing V retires every disk version and backing, independently of catalog.
		delete(e.vms, r.VMID)
	case "delete_snapshot":
		if _, ok := e.snaps[r.SnapshotID]; !ok {
			return api.Operation{}, api.Fail(api.NotFound, "snapshot absent")
		}
		delete(e.snaps, r.SnapshotID)
	default:
		return api.Operation{}, api.Fail(
			api.Invalid,
			"unsupported component action",
		)
	}
	if r.Action != "delete" && r.Action != "delete_snapshot" {
		e.vms[o.VMID] = p
	}
	e.calls[r.Action]++
	e.ops[r.Key] = ownerCopy(o)
	return ownerCopy(o), nil
}

func (e *ownerEngine) GetOperation(
	_ context.Context,
	id string,
) (api.Operation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, o := range e.ops {
		if o.ID == id {
			return ownerCopy(o), nil
		}
	}
	return api.Operation{}, api.Fail(api.NotFound, "receipt absent")
}

func (e *ownerEngine) FindOperation(
	_ context.Context,
	key string,
) (api.Operation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	o, ok := e.ops[key]
	if !ok {
		return o, api.Fail(api.NotFound, "receipt absent")
	}
	return ownerCopy(o), nil
}

func (e *ownerEngine) GetVM(_ context.Context, id string) (api.VM, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.vms[id]
	if !ok {
		return api.VM{}, api.Fail(api.NotFound, "VM absent")
	}
	return ownerCopy(p.VM), nil
}

func (e *ownerEngine) ListVMs(ctx context.Context) ([]api.VM, error) {
	e.gateMu.Lock()
	e.inventory++
	n, g := e.inventory, e.quiet
	e.gateMu.Unlock()
	// reconcile and recoverResources each take one inventory. Reaching the
	// third proves the complete first reconcile/reap/pools iteration returned.
	if n >= 3 && g != nil {
		if err := g.wait(ctx); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []api.VM{}
	for _, p := range e.vms {
		out = append(out, ownerCopy(p.VM))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (e *ownerEngine) inventorySnapshot() []api.VM {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []api.VM{}
	for _, p := range e.vms {
		out = append(out, ownerCopy(p.VM))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *ownerEngine) GetSnapshot(
	_ context.Context,
	id string,
) (api.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.snapshotQueryError {
		return api.Snapshot{}, api.Fail(
			api.Internal,
			"injected catalog query failure",
		)
	}
	s, ok := e.snaps[id]
	if !ok {
		return s, api.Fail(api.NotFound, "snapshot absent")
	}
	return ownerCopy(s), nil
}

func (e *ownerEngine) ListSnapshots(context.Context) ([]api.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []api.Snapshot{}
	for _, s := range e.snaps {
		out = append(out, ownerCopy(s))
	}
	return out, nil
}

func (e *ownerEngine) effects() map[string]int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return ownerCopy(e.calls)
}

func (e *ownerEngine) gateDelete(g *ownerGate) {
	e.gateMu.Lock()
	defer e.gateMu.Unlock()
	e.deletion = g
}

func (e *ownerEngine) physical(id string) ownerPhysical {
	e.mu.Lock()
	defer e.mu.Unlock()
	return ownerCopy(e.vms[id])
}

type ownerAgent struct {
	engine     *ownerEngine
	mu         sync.Mutex
	calls      map[string]int
	identities map[string]string
	wg         sync.WaitGroup
}

func (a *ownerAgent) observe(
	ctx context.Context,
	v api.VM,
	action string,
) error {
	current, err := a.engine.GetVM(ctx, v.ID)
	if err != nil {
		return err
	}
	if current.Instance != v.Instance || current.Phase != "running" ||
		!current.AgentReady {
		return api.Fail(api.Conflict, "agent instance")
	}
	a.mu.Lock()
	a.calls[action]++
	a.mu.Unlock()
	return nil
}

func (a *ownerAgent) Info(
	ctx context.Context,
	v api.VM,
) error {
	return a.observe(ctx, v, "info")
}

func (a *ownerAgent) Activate(
	ctx context.Context,
	v api.VM,
	x sandboxd.Activation,
) error {
	if err := a.observe(ctx, v, "activate"); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.identities[v.ID+":"+v.Instance] = x.SandboxID
	return nil
}

func (a *ownerAgent) PrepareTemplate(ctx context.Context, v api.VM) error {
	return a.observe(ctx, v, "prepare")
}

func (a *ownerAgent) Exec(ctx context.Context, v api.VM, _, _ string) error {
	return a.observe(ctx, v, "exec")
}

func (a *ownerAgent) Forward(context.Context, api.VM, int) (net.Conn, error) {
	return nil, api.Fail(api.Invalid, "unused forwarding")
}

func (a *ownerAgent) Connect(ctx context.Context, v api.VM) (net.Conn, error) {
	if err := a.observe(ctx, v, "connect"); err != nil {
		return nil, err
	}
	a.mu.Lock()
	identity := a.identities[v.ID+":"+v.Instance]
	a.mu.Unlock()
	if identity == "" {
		return nil, api.Fail(api.Conflict, "activation absent")
	}
	down, up := net.Pipe()
	a.wg.Go(func() {
		defer up.Close()
		for {
			f, err := agent.Read(up)
			if err != nil {
				return
			}
			reply := &pb.Frame{Id: f.Id, Eof: true}
			if f.Method != "info" {
				reply.Error = "unsupported component RPC"
			} else {
				reply.Params, _ = structpb.NewStruct(
					map[string]any{
						"protocol":   1,
						"portal":     "workload",
						"sandbox_id": identity,
						"vm_id":      v.ID,
						"instance":   v.Instance,
					},
				)
			}
			if agent.Write(up, reply) != nil {
				return
			}
		}
	})
	return down, nil
}

func (a *ownerAgent) effects() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return ownerCopy(a.calls)
}

type ownerEgress struct {
	service  *sandboxd.Service
	mu       sync.Mutex
	cleanups map[string]func() error
}

func (e *ownerEgress) Bind(
	ctx context.Context,
	id, tenant, path string,
	p egress.Policy,
	_ map[string]egress.Secret,
) error {
	if err := e.Revoke(id); err != nil {
		return err
	}
	if p.Mode != "direct" {
		return api.Fail(api.Invalid, "component direct binding required")
	}
	cleanup, err := e.service.InstallDirect(
		ctx,
		egress.Binding{SandboxID: id, TenantID: tenant, VSockPath: path},
		p,
	)
	if cleanup != nil {
		e.mu.Lock()
		e.cleanups[id] = cleanup
		e.mu.Unlock()
	}
	return err
}

func (e *ownerEgress) Revoke(id string) error {
	e.mu.Lock()
	cleanup := e.cleanups[id]
	delete(e.cleanups, id)
	e.mu.Unlock()
	if cleanup != nil {
		return cleanup()
	}
	return nil
}

func (e *ownerEgress) Close() error {
	e.mu.Lock()
	ids := []string{}
	for id := range e.cleanups {
		ids = append(ids, id)
	}
	e.mu.Unlock()
	var err error
	for _, id := range ids {
		err = errors.Join(err, e.Revoke(id))
	}
	return err
}

type ownerFixture struct {
	t       *testing.T
	ctx     context.Context
	cancel  context.CancelFunc
	state   string
	engine  *ownerEngine
	agent   *ownerAgent
	service *sandboxd.Service
	server  *httptest.Server
	root    *sdk.Client
	db      *sql.DB
	quiet   *ownerGate
	tokens  map[string]string
}

func newOwnerFixture(t *testing.T) *ownerFixture {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	f := &ownerFixture{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		state:  t.TempDir(),
		engine: newOwnerEngine(),
		tokens: map[string]string{},
	}
	f.agent = &ownerAgent{
		engine:     f.engine,
		calls:      map[string]int{},
		identities: map[string]string{},
	}
	f.open(true)
	t.Cleanup(func() {
		f.close()
		cancel()
		done := make(chan struct{})
		go func() { f.agent.wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("agent streams did not stop")
		}
	})
	return f
}

func ownerAwait(
	t *testing.T,
	ctx context.Context,
	ch <-chan struct{},
	stage string,
) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("%s timed out", stage)
	}
}

func (f *ownerFixture) open(initialize bool) {
	f.t.Helper()
	f.quiet = newOwnerGate()
	f.engine.gateMu.Lock()
	f.engine.inventory = 0
	f.engine.quiet = f.quiet
	f.engine.gateMu.Unlock()
	out := &ownerEgress{cleanups: map[string]func() error{}}
	s, err := sandboxd.Open(
		sandboxd.Options{
			StateDir:   f.state,
			Initialize: initialize,
			RootToken:  "component-manager-only",
			Engine:     f.engine,
			Agent:      f.agent,
			Egress:     out,
			Interval:   10 * time.Millisecond,
		},
	)
	if err != nil {
		f.t.Fatalf("open: %v", err)
	}
	f.service = s
	out.service = s
	f.server = httptest.NewServer(s.Handler())
	f.root = sdk.New(f.server.URL, "component-manager-only")
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(
			f.state,
			"sandboxd.db",
		)+"?mode=ro&_pragma=busy_timeout(1000)",
	)
	if err != nil {
		f.t.Fatal(err)
	}
	f.db = db
	ownerAwait(f.t, f.ctx, f.quiet.entered, "first maintenance quiet boundary")
}

func (f *ownerFixture) close() {
	if f.service == nil {
		return
	}
	f.engine.gateMu.Lock()
	g := f.engine.deletion
	f.engine.deletion = nil
	f.engine.gateMu.Unlock()
	if g != nil {
		g.open()
	}
	// Close cancels the parked inventory and every tracked upgraded stream.
	if err := f.service.Close(); err != nil {
		f.t.Error("service close", err)
	}
	f.server.CloseClientConnections()
	f.server.Close()
	f.root.HTTP.CloseIdleConnections()
	if err := f.db.Close(); err != nil {
		f.t.Error(err)
	}
	f.service = nil
}

func (f *ownerFixture) reopen() { f.close(); f.open(false) }

func (f *ownerFixture) client(
	id string,
) *sdk.Client {
	return sdk.New(f.server.URL, f.tokens[id])
}

func (f *ownerFixture) budget(id string, slots int, storage int64) {
	f.t.Helper()
	token := f.tokens[id]
	if token == "" {
		token = "ordinary-component-" + id
		f.tokens[id] = token
	}
	payload := sandboxd.Tenant{
		ID:            id,
		Token:         token,
		MaxSandboxes:  slots,
		MaxStorageMiB: storage,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		f.t.Fatal("budget encode")
	}
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		"POST",
		f.root.Endpoint+"/v1/tenants",
		bytes.NewReader(raw),
	)
	if err != nil {
		f.t.Fatal("budget request")
	}
	req.Header.Set("Authorization", "Bearer "+f.root.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.root.HTTP.Do(req)
	if err != nil {
		f.t.Fatal("budget transport")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		f.t.Fatalf("budget status=%d", resp.StatusCode)
	}
	var got struct {
		ID      string `json:"id"`
		Slots   int    `json:"max_sandboxes"`
		Storage int64  `json:"max_storage_mib"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&got) != nil ||
		got.ID != id ||
		got.Slots != slots ||
		got.Storage != storage {
		f.t.Fatal("budget response fields")
	}
}

func ownerMust(t *testing.T, r sandboxd.Result, err error) sandboxd.Result {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (f *ownerFixture) template(
	c *sdk.Client,
	id string,
	shape api.Shape,
	build bool,
) sandboxd.Template {
	f.t.Helper()
	r, err := c.PutTemplate(
		f.ctx,
		"put-"+id,
		sandboxd.Template{
			ID:    id,
			Image: "sha256:" + strings.Repeat("1", 64),
			Shape: shape,
		},
		build,
	)
	return *ownerMust(f.t, r, err).Template
}

func (f *ownerFixture) claim(
	c *sdk.Client,
	template, key string,
	p egress.Policy,
) sandboxd.Result {
	f.t.Helper()
	r, err := c.Claim(
		f.ctx,
		key,
		sandboxd.Command{
			Action:     "claim",
			TemplateID: template,
			TTLSeconds: 600,
			Policy:     p,
		},
	)
	return ownerMust(f.t, r, err)
}

func (f *ownerFixture) change(
	c *sdk.Client,
	id, key, action string,
) sandboxd.Result {
	f.t.Helper()
	r, err := c.Change(f.ctx, id, key, sandboxd.Command{Action: action})
	return ownerMust(f.t, r, err)
}

func (f *ownerFixture) get(c *sdk.Client, id string) sandboxd.Sandbox {
	f.t.Helper()
	b, err := c.Get(f.ctx, id)
	if err != nil {
		f.t.Fatal(err)
	}
	return b
}

func ownerCode(err error, code api.Code) bool {
	var e *api.Error
	return errors.As(err, &e) && e.Code == code
}

type ownerResource struct {
	Tenant, Kind, Key string
	Slots, Storage    int64
	Source            string
}
type (
	ownerRef    struct{ Tenant, Kind, Key, Holder, ID string }
	ownerLedger struct {
		Resources []ownerResource
		Refs      []ownerRef
	}
)

func (f *ownerFixture) ledger() ownerLedger {
	f.t.Helper()
	out := ownerLedger{}
	rows, err := f.db.QueryContext(
		f.ctx,
		"SELECT tenant_id,kind,resource_key,sandbox_slots,storage_mib,source_operation_id FROM quota_resources ORDER BY tenant_id,kind,resource_key",
	)
	if err != nil {
		f.t.Fatal(err)
	}
	for rows.Next() {
		var r ownerResource
		if err = rows.Scan(
			&r.Tenant,
			&r.Kind,
			&r.Key,
			&r.Slots,
			&r.Storage,
			&r.Source,
		); err != nil {
			f.t.Fatal(err)
		}
		out.Resources = append(out.Resources, r)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		f.t.Fatal(err)
	}
	rows, err = f.db.QueryContext(
		f.ctx,
		"SELECT tenant_id,kind,resource_key,holder_kind,holder_id FROM quota_refs ORDER BY tenant_id,kind,resource_key,holder_kind,holder_id",
	)
	if err != nil {
		f.t.Fatal(err)
	}
	for rows.Next() {
		var r ownerRef
		if err = rows.Scan(
			&r.Tenant,
			&r.Kind,
			&r.Key,
			&r.Holder,
			&r.ID,
		); err != nil {
			f.t.Fatal(err)
		}
		out.Refs = append(out.Refs, r)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f *ownerFixture) scalar(query string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.db.QueryRowContext(f.ctx, query, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

func (f *ownerFixture) ops() []sandboxd.Operation {
	f.t.Helper()
	rows, err := f.db.QueryContext(
		f.ctx,
		"SELECT body FROM operations ORDER BY id",
	)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []sandboxd.Operation
	for rows.Next() {
		var raw []byte
		var o sandboxd.Operation
		if err = rows.Scan(&raw); err != nil {
			f.t.Fatal(err)
		}
		if json.Unmarshal(raw, &o) != nil {
			f.t.Fatal("operation decode")
		}
		out = append(out, o)
	}
	if rows.Err() != nil {
		f.t.Fatal(rows.Err())
	}
	return out
}

func (f *ownerFixture) assertLedger(
	stage, owner string,
	slots int,
	disks, sids []string,
	shape api.Shape,
) {
	f.t.Helper()
	l := f.ledger()
	want := map[string]int64{}
	for _, id := range disks {
		want["vm_disk/"+id] = shape.DiskMiB
	}
	for _, id := range sids {
		want["snapshot/"+id] = shape.DiskMiB + shape.MemoryMiB
	}
	got := map[string]int64{}
	gotSlots := int64(0)
	for _, r := range l.Resources {
		if r.Tenant != owner {
			f.t.Fatalf("stage=%s charged unexpected tenant=%s", stage, r.Tenant)
		}
		gotSlots += r.Slots
		if r.Kind == "sandbox_slot" {
			if r.Slots != 1 || r.Storage != 0 {
				f.t.Fatal("slot cost")
			}
			continue
		}
		got[r.Kind+"/"+r.Key] = r.Storage
		if r.Source == "" {
			f.t.Fatal("resource lacks source")
		}
	}
	if gotSlots != int64(slots) || !reflect.DeepEqual(got, want) {
		f.t.Fatalf(
			"stage=%s slots=%d want=%d identities=%v want=%v",
			stage,
			gotSlots,
			slots,
			got,
			want,
		)
	}
	for _, r := range l.Resources {
		found := false
		for _, ref := range l.Refs {
			if ref.Tenant == r.Tenant && ref.Kind == r.Kind &&
				ref.Key == r.Key {
				found = true
			}
		}
		if !found {
			f.t.Fatalf(
				"stage=%s unreferenced charged identity=%s/%s",
				stage,
				r.Kind,
				r.Key,
			)
		}
	}
	raw, _ := json.Marshal(l)
	f.t.Logf("stage=%s ledger=%s", stage, raw)
}

type ownerPrefix struct {
	Source            sandboxd.Sandbox
	Branches          []sandboxd.Sandbox
	CP                sandboxd.Checkpoint
	ForkSID, SleepSID string
	Token             string
	Shape             api.Shape
}

func (f *ownerFixture) prefix(c *sdk.Client, shape api.Shape) ownerPrefix {
	f.t.Helper()
	f.template(c, "lifecycle", shape, false)
	claim := f.claim(c, "lifecycle", "life-claim", egress.Policy{Mode: "none"})
	b := *claim.Sandbox
	f.info(c, b.ID, "")
	cp := *f.change(c, b.ID, "life-cp", "checkpoint").Checkpoint
	fork, err := c.Change(
		f.ctx,
		b.ID,
		"life-fork",
		sandboxd.Command{Action: "fork", Count: 2},
	)
	fork = ownerMust(f.t, fork, err)
	if len(fork.Sandboxes) != 2 {
		f.t.Fatal("two fork branches required")
	}
	sid := ""
	for _, o := range f.ops() {
		if o.Command.Action == "fork" {
			sid = o.SnapshotID
		}
	}
	if sid == "" || sid == cp.SnapshotID {
		f.t.Fatal("distinct fork capture required")
	}
	for _, branch := range fork.Sandboxes {
		f.info(c, branch.ID, "")
	}
	paused := *f.change(c, b.ID, "life-pause", "pause").Sandbox
	if paused.State != "paused" || paused.VMID != "" ||
		paused.SnapshotID == "" {
		f.t.Fatal("pause result")
	}
	if _, err = f.engine.GetVM(f.ctx, b.VMID); !ownerCode(err, api.NotFound) {
		f.t.Fatal("old VM not deleted")
	}
	f.assertLedger(
		"paused",
		b.TenantID,
		3,
		[]string{fork.Sandboxes[0].VMID, fork.Sandboxes[1].VMID},
		[]string{cp.SnapshotID, sid, paused.SnapshotID},
		shape,
	)
	return ownerPrefix{
		Source:   paused,
		Branches: fork.Sandboxes,
		CP:       cp,
		ForkSID:  sid,
		SleepSID: paused.SnapshotID,
		Token:    claim.Tokens[b.ID],
		Shape:    shape,
	}
}

// Only selected-Bob uses raw upgrade: SDK.Connect intentionally bypasses HTTP.
func ownerUpgrade(
	ctx context.Context,
	c *sdk.Client,
	id, selected string,
) (*sdk.Connection, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", u.Host)
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	if deadline, ok := ctx.Deadline(); ok {
		if err = conn.SetDeadline(deadline); err != nil {
			conn.Close()
			return nil, err
		}
	}
	u.Path = "/v1/sandboxes/" + url.PathEscape(id) + "/connect"
	req := &http.Request{
		Method: "GET",
		URL:    u,
		Host:   u.Host,
		Header: make(http.Header),
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "sandbox-v1")
	req.Header.Set("X-Tenant-ID", selected)
	if err = req.Write(conn); err != nil {
		conn.Close()
		return nil, err
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != 101 {
		defer resp.Body.Close()
		defer conn.Close()
		var typed api.Error
		if json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).
			Decode(&typed) !=
			nil {
			return nil, errors.New("upgrade status")
		}
		return nil, &typed
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "sandbox-v1") {
		conn.Close()
		return nil, errors.New("upgrade protocol")
	}
	return &sdk.Connection{
		Conn: &agent.BufferedConn{Conn: conn, Reader: br},
	}, nil
}

func ownerInfo(
	ctx context.Context,
	c *sdk.Client,
	id, selected string,
) (string, error) {
	var conn *sdk.Connection
	var err error
	if selected == "" {
		conn, err = c.Connect(ctx, id)
	} else {
		conn, err = ownerUpgrade(ctx, c, id, selected)
	}
	if err != nil {
		return "connect", err
	}
	defer conn.Close()
	result, err := conn.Call(ctx, "info", map[string]any{}, nil)
	if err == nil &&
		(result["protocol"] != float64(1) || result["portal"] != "workload" || result["sandbox_id"] != id) {
		err = errors.New("info activation identity mismatch")
	}
	return "info", err
}

func (f *ownerFixture) info(c *sdk.Client, id, selected string) {
	f.t.Helper()
	stage, err := ownerInfo(f.ctx, c, id, selected)
	if err != nil {
		f.t.Fatalf("stage=%s code=%s", stage, ownerErrorCode(err))
	}
}

func ownerErrorCode(err error) api.Code {
	var typed *api.Error
	if errors.As(err, &typed) {
		return typed.Code
	}
	return api.Internal
}

func (f *ownerFixture) eight(
	c *sdk.Client,
	id, selected string,
	want api.Code,
) {
	f.t.Helper()
	type outcome struct {
		Index int
		Stage string
		Err   error
	}
	out := make(chan outcome, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(
			func() { stage, err := ownerInfo(f.ctx, c, id, selected); out <- outcome{i, stage, err} },
		)
	}
	wg.Wait()
	close(out)
	failed := 0
	for result := range out {
		code := api.Code("")
		if result.Err != nil {
			code = ownerErrorCode(result.Err)
		}
		f.t.Logf("caller=%d stage=%s code=%s", result.Index, result.Stage, code)
		if want == "" {
			if result.Err != nil {
				failed++
			}
		} else if result.Stage != "connect" || !ownerCode(result.Err, want) {
			failed++
		}
	}
	if failed != 0 {
		f.t.Fatalf(
			"eight-connect success oracle: mismatches=%d expected_code=%s",
			failed,
			want,
		)
	}
}

func (f *ownerFixture) resumed(
	p ownerPrefix,
	c *sdk.Client,
	selected string,
) sandboxd.Sandbox {
	f.t.Helper()
	before := f.engine.effects()["clone"]
	beforeDeletes := f.engine.effects()["delete_snapshot"]
	agentBefore := f.agent.effects()
	ops := len(f.ops())
	f.eight(c, p.Source.ID, selected, "")
	b := f.get(f.root, p.Source.ID)
	if b.State != "running" || b.VMID == "" || b.Instance == "" ||
		b.VMID == p.Source.VMID ||
		f.engine.effects()["clone"] != before+1 ||
		len(f.ops()) != ops+1 {
		f.t.Fatal("eight connections did not converge to one resume")
	}
	if f.engine.effects()["delete_snapshot"] != beforeDeletes+1 ||
		f.agent.effects()["connect"] != agentBefore["connect"]+8 ||
		f.agent.effects()["activate"] != agentBefore["activate"]+1 {
		f.t.Fatal(
			"auto-resume duplicated retirement/activation/connection effects",
		)
	}
	found := 0
	for _, o := range f.ops() {
		if o.Command.Action == "resume" {
			found++
			if o.TenantID != p.Source.TenantID ||
				o.ChargeTenantID != p.Source.TenantID ||
				o.Key != "auto-resume:"+p.Source.ID+":"+p.SleepSID+":0" {
				f.t.Fatal("auto operation owner/namespace")
			}
			f.t.Logf(
				"auto_operation=%s key=%s tenant=%s charge=%s",
				o.ID,
				o.Key,
				o.TenantID,
				o.ChargeTenantID,
			)
		}
	}
	if found != 1 {
		f.t.Fatal("resume count")
	}
	f.assertLedger(
		"auto-resumed",
		p.Source.TenantID,
		3,
		[]string{p.Branches[0].VMID, p.Branches[1].VMID, b.VMID},
		[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID},
		p.Shape,
	)
	for _, branch := range p.Branches {
		f.info(f.root, branch.ID, "")
	}
	return b
}

func (f *ownerFixture) deleteCP(c *sdk.Client, cp sandboxd.Checkpoint) {
	f.t.Helper()
	r, err := c.DeleteCheckpoint(f.ctx, cp.ID, "delete-"+cp.ID)
	ownerMust(f.t, r, err)
}

func (f *ownerFixture) cleanupPrefix(c *sdk.Client, p ownerPrefix) {
	f.t.Helper()
	f.change(c, p.Source.ID, "release-source", "release")
	f.deleteCP(c, p.CP)
	for _, b := range p.Branches {
		f.change(c, b.ID, "release-"+b.ID, "release")
	}
}

func TestProductionOwnerBudgetMissingRowHTTP(t *testing.T) {
	f := newOwnerFixture(t)
	shape := api.Shape{VCPUs: 1, MemoryMiB: 512, DiskMiB: 512}
	p := f.prefix(f.root, shape)
	before, eff, agents, ops := f.ledger(), f.engine.effects(), f.agent.effects(), len(
		f.ops(),
	)
	f.eight(f.root, p.Source.ID, "", api.NotFound)
	if !reflect.DeepEqual(before, f.ledger()) ||
		!reflect.DeepEqual(eff, f.engine.effects()) ||
		!reflect.DeepEqual(agents, f.agent.effects()) ||
		len(f.ops()) != ops ||
		f.scalar("SELECT count(*) FROM tenants") != 0 {
		t.Fatal("missing row changed resources/effects")
	}
	b := f.get(f.root, p.Source.ID)
	if b.SnapshotID != p.SleepSID || b.ResumeAttempt != p.Source.ResumeAttempt {
		t.Fatal("missing row changed sleep identity")
	}
	if _, err := f.engine.GetSnapshot(f.ctx, p.SleepSID); err != nil {
		t.Fatal(err)
	}
	f.change(f.root, b.ID, "explicit-root-control", "resume")
	f.info(f.root, b.ID, "")
	if f.engine.effects()["clone"] != eff["clone"]+1 ||
		f.scalar("SELECT count(*) FROM tenants") != 0 {
		t.Fatal("explicit control")
	}
	f.cleanupPrefix(f.root, p)
}

func TestProductionOwnerBudgetFiniteHTTP(t *testing.T) {
	f := newOwnerFixture(t)
	shape := api.Shape{VCPUs: 1, MemoryMiB: 512, DiskMiB: 512}
	// The success oracle is also run with this single HTTP prerequisite omitted.
	f.budget("root", 9, 13312)
	disks, sids, slots := map[string]bool{}, map[string]bool{}, map[string]bool{}
	record := func(b sandboxd.Sandbox) { disks[b.VMID] = true; slots[b.ID] = true }
	f.template(f.root, "adoption", shape, false)
	adopted := *f.claim(f.root, "adoption", "adoption-claim", egress.Policy{Mode: "none"}).Sandbox
	record(adopted)
	f.assertLedger("adoption", "root", 1, []string{adopted.VMID}, nil, shape)
	f.info(f.root, adopted.ID, "")
	f.reopen()
	current := f.get(f.root, adopted.ID)
	if current.VMID != adopted.VMID || current.Instance != adopted.Instance {
		t.Fatal("adoption replaced current VM")
	}
	f.info(f.root, adopted.ID, "")
	f.change(f.root, adopted.ID, "adoption-release", "release")
	f.assertLedger("adoption-release", "root", 0, nil, nil, shape)
	p := f.prefix(f.root, shape)
	// The original V0 is fixed by the successful claim receipt even after pause.
	for _, o := range f.ops() {
		if o.Key == "life-claim" {
			record(*o.Result.Sandbox)
		}
	}
	for _, b := range p.Branches {
		record(b)
	}
	sids[p.CP.SnapshotID] = true
	sids[p.ForkSID] = true
	sids[p.SleepSID] = true
	running := f.resumed(p, f.root, "")
	record(running)
	r, err := f.root.Change(
		f.ctx,
		running.ID,
		"life-rollback",
		sandboxd.Command{Action: "rollback", CheckpointID: p.CP.ID},
	)
	r = ownerMust(t, r, err)
	physical := f.engine.physical(r.Sandbox.VMID)
	lifeDisks := []string{p.Branches[0].VMID, p.Branches[1].VMID}
	for id := range physical.Disks {
		disks[id] = true
		lifeDisks = append(lifeDisks, id)
	}
	if len(physical.Disks) != 2 || !physical.Backings[p.SleepSID] ||
		!physical.Backings[p.CP.SnapshotID] {
		t.Fatal("lifecycle restore identities")
	}
	f.assertLedger(
		"finite-rollback",
		"root",
		3,
		lifeDisks,
		[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID},
		shape,
	)
	paused := *f.change(f.root, running.ID, "life-second-pause", "pause").Sandbox
	sids[paused.SnapshotID] = true
	before, agents := f.engine.effects(), f.agent.effects()
	f.change(f.root, running.ID, "life-zero-wake", "release")
	if f.engine.effects()["clone"] != before["clone"] ||
		!reflect.DeepEqual(agents, f.agent.effects()) {
		t.Fatal("finite zero wake")
	}
	f.deleteCP(f.root, p.CP)
	for _, b := range p.Branches {
		f.info(f.root, b.ID, "")
		f.change(f.root, b.ID, "life-release-"+b.ID, "release")
	}
	f.reopen()
	f.assertLedger("life-cleaned", "root", 0, nil, nil, shape)
	if f.scalar(
		"SELECT count(*) FROM tenants WHERE id='root' AND json_extract(body,'$.max_sandboxes')=9 AND json_extract(body,'$.max_storage_mib')=13312",
	) != 1 {
		t.Fatal("finite owner row did not persist")
	}

	// Build is intentionally unbilled by source90; golden/warm claims each own D
	// and share exactly one T. Actual maintenance publishes the ready warm row.
	built := f.template(f.root, "tiers", shape, true)
	sids[built.SnapshotID] = true
	if built.SnapshotID == "" || built.State != "ready" {
		t.Fatal("build did not publish template")
	}
	f.assertLedger("build-unclaimed-013", "root", 0, nil, nil, shape)
	golden := *f.claim(f.root, "tiers", "tier-golden", egress.Policy{Mode: "none"}).Sandbox
	record(golden)
	f.info(f.root, golden.ID, "")
	if golden.ProvisionSource != "golden" {
		t.Fatal("golden source")
	}
	f.assertLedger(
		"golden",
		"root",
		1,
		[]string{golden.VMID},
		[]string{built.SnapshotID},
		shape,
	)
	f.change(f.root, golden.ID, "tier-golden-release", "release")
	built.TargetWarm = 1
	result, err := f.root.PutTemplate(f.ctx, "tier-enable", built, false)
	ownerMust(t, result, err)
	f.quiet.open()
	f.until(
		"ready warm row",
		func() bool { return f.scalar("SELECT count(*) FROM warm WHERE template_id=?", built.ID) == 1 },
	)
	var warmID string
	if err = f.db.QueryRowContext(f.ctx, "SELECT vm_id FROM warm WHERE template_id=?", built.ID).
		Scan(&warmID); err != nil {
		t.Fatal(err)
	}
	warmBefore, err := f.engine.GetVM(f.ctx, warmID)
	if err != nil {
		t.Fatal(err)
	}
	f.assertLedger(
		"unclaimed-warm-013",
		"root",
		0,
		nil,
		[]string{built.SnapshotID},
		shape,
	)
	warm := *f.claim(f.root, "tiers", "tier-warm", egress.Policy{Mode: "none"}).Sandbox
	record(warm)
	if warm.ProvisionSource != "warm" || warm.VMID != warmID ||
		warm.Instance != warmBefore.Instance {
		t.Fatal("warm did not reuse current VM")
	}
	f.info(f.root, warm.ID, "")
	f.assertLedger(
		"claimed-warm",
		"root",
		1,
		[]string{warm.VMID},
		[]string{built.SnapshotID},
		shape,
	)
	built.TargetWarm = 0
	result, err = f.root.PutTemplate(f.ctx, "tier-disable", built, false)
	ownerMust(t, result, err)
	f.change(f.root, warm.ID, "tier-warm-release", "release")
	f.until("pool disabled and settled", func() bool {
		return f.scalar("SELECT count(*) FROM warm") == 0 &&
			f.scalar(
				"SELECT count(*) FROM operations WHERE status IN ('pending','running')",
			) == 0
	})
	f.reopen()
	f.assertLedger(
		"tiers-cleaned",
		"root",
		0,
		nil,
		[]string{built.SnapshotID},
		shape,
	)

	// Replay the resource sequence of guest TCP with strict current-instance
	// direct policy installation. No net.Pipe result claims guest TCP enforcement.
	f.template(f.root, "tcp", shape, false)
	direct := *f.claim(f.root, "tcp", "tcp-direct", egress.Policy{Mode: "direct", AllowedCIDRs: []string{"1.1.1.0/24"}, MaxConnections: 4}).Sandbox
	none := *f.claim(f.root, "tcp", "tcp-none", egress.Policy{Mode: "none"}).Sandbox
	record(direct)
	record(none)
	vm, err := f.engine.GetVM(f.ctx, direct.VMID)
	if err != nil {
		t.Fatal(err)
	}
	if !vm.NetworkActual || vm.NetworkPolicy == nil ||
		vm.NetworkPolicyInstance != direct.Instance {
		t.Fatal("direct policy boundary not installed")
	}
	f.info(f.root, direct.ID, "")
	f.info(f.root, none.ID, "")
	f.assertLedger(
		"tcp-direct-none",
		"root",
		2,
		[]string{direct.VMID, none.VMID},
		[]string{built.SnapshotID},
		shape,
	)
	f.change(f.root, none.ID, "tcp-none-release", "release")
	fork, err := f.root.Change(
		f.ctx,
		direct.ID,
		"tcp-fork",
		sandboxd.Command{Action: "fork", Count: 1},
	)
	fork = ownerMust(t, fork, err)
	if len(fork.Sandboxes) != 1 {
		t.Fatal("TCP branch")
	}
	branch := fork.Sandboxes[0]
	record(branch)
	var forkSID string
	for _, o := range f.ops() {
		if o.Key == "tcp-fork" {
			forkSID = o.SnapshotID
		}
	}
	if forkSID == "" {
		t.Fatal("TCP fork capture")
	}
	sids[forkSID] = true
	f.info(f.root, branch.ID, "")
	f.assertLedger(
		"tcp-fork",
		"root",
		2,
		[]string{direct.VMID, branch.VMID},
		[]string{built.SnapshotID, forkSID},
		shape,
	)
	tcpPaused := *f.change(f.root, direct.ID, "tcp-pause", "pause").Sandbox
	sids[tcpPaused.SnapshotID] = true
	f.assertLedger(
		"tcp-paused",
		"root",
		2,
		[]string{branch.VMID},
		[]string{built.SnapshotID, forkSID, tcpPaused.SnapshotID},
		shape,
	)
	f.info(f.root, direct.ID, "")
	tcpRunning := f.get(f.root, direct.ID)
	record(tcpRunning)
	f.assertLedger(
		"tcp-resumed",
		"root",
		2,
		[]string{branch.VMID, tcpRunning.VMID},
		[]string{built.SnapshotID, forkSID, tcpPaused.SnapshotID},
		shape,
	)
	f.change(f.root, direct.ID, "tcp-direct-release", "release")
	f.change(f.root, branch.ID, "tcp-branch-release", "release")
	f.reopen()
	f.assertLedger(
		"full-sequence-cleaned",
		"root",
		0,
		nil,
		[]string{built.SnapshotID},
		shape,
	)
	if len(disks) != 12 || len(sids) != 7 || len(slots) != 9 {
		t.Fatalf(
			"fixed allocation identity counts disks=%d SIDs=%d slots=%d",
			len(disks),
			len(sids),
			len(slots),
		)
	}
	allocationIDs := func(set map[string]bool) []string {
		out := []string{}
		for id := range set {
			out = append(out, id)
		}
		sort.Strings(out)
		return out
	}
	t.Logf(
		"full allocation identities disks=%v snapshots=%v sandboxes=%v",
		allocationIDs(disks),
		allocationIDs(sids),
		allocationIDs(slots),
	)
}

func (f *ownerFixture) until(stage string, condition func() bool) {
	f.t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-f.ctx.Done():
			f.t.Fatalf("%s timeout", stage)
		case <-ticker.C:
		}
	}
}

func TestProductionOwnerBudgetAutoResumeBoundaryHTTP(t *testing.T) {
	callers := []string{
		"manager-root",
		"ordinary-root",
		"alice",
		"root-default-alice",
		"root-selected-bob",
		"sandbox-alice",
	}
	shapes := []api.Shape{
		{VCPUs: 1, MemoryMiB: 512, DiskMiB: 512},
		{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64},
	}
	for _, shape := range shapes {
		for _, caller := range callers {
			for _, low := range []bool{false, true} {
				t.Run(
					fmt.Sprintf(
						"D%d-M%d/%s/low=%t",
						shape.DiskMiB,
						shape.MemoryMiB,
						caller,
						low,
					),
					func(t *testing.T) {
						f := newOwnerFixture(t)
						owner := "alice"
						if caller == "manager-root" ||
							caller == "ordinary-root" {
							owner = "root"
						}
						s := shape.DiskMiB + shape.MemoryMiB
						exact := 3*shape.DiskMiB + 3*s
						f.budget(owner, 3, 5*shape.DiskMiB+4*s)
						setup := f.client(owner)
						p := f.prefix(setup, shape)
						c := f.client(owner)
						principal, err := f.service.Authenticate(f.ctx, c.Token)
						if err != nil || principal.Root ||
							principal.TenantID != owner {
							t.Fatal("ordinary tenant authentication changed")
						}
						selected := ""
						switch caller {
						case "manager-root", "root-default-alice":
							c = f.root
						case "root-selected-bob":
							f.budget("bob", 9, 12*shape.DiskMiB+7*s)
							c = f.root
							selected = "bob"
						case "sandbox-alice":
							c = sdk.New(f.server.URL, p.Token)
						}
						if caller == "sandbox-alice" {
							_, err := c.Get(f.ctx, p.Branches[0].ID)
							if !ownerCode(err, api.NotFound) {
								t.Fatal("sandbox visibility")
							}
						}
						limit := exact
						if low {
							limit--
						}
						f.budget(owner, 3, limit)
						if low {
							ledger, eff, agents, ops := f.ledger(), f.engine.effects(), f.agent.effects(), len(
								f.ops(),
							)
							f.eight(c, p.Source.ID, selected, api.Exhausted)
							after := f.get(f.root, p.Source.ID)
							if !reflect.DeepEqual(ledger, f.ledger()) ||
								!reflect.DeepEqual(eff, f.engine.effects()) ||
								!reflect.DeepEqual(agents, f.agent.effects()) ||
								len(f.ops()) != ops ||
								after.State != "paused" ||
								after.SnapshotID != p.SleepSID ||
								after.ResumeAttempt != p.Source.ResumeAttempt {
								t.Fatal("low admission caused effects")
							}
							if _, err := f.engine.GetSnapshot(
								f.ctx,
								p.SleepSID,
							); err != nil {
								t.Fatal(err)
							}
							f.budget(owner, 3, exact)
						}
						f.resumed(p, c, selected)
						f.cleanupPrefix(setup, p)
					},
				)
			}
		}
	}
	t.Run("public-root-low-bypass", func(t *testing.T) {
		f := newOwnerFixture(t)
		shape := shapes[0]
		f.budget("root", 3, 5*shape.DiskMiB+4*(shape.DiskMiB+shape.MemoryMiB))
		p := f.prefix(f.root, shape)
		f.budget("root", 3, 4607)
		before := f.engine.effects()["clone"]
		f.change(f.root, p.Source.ID, "public-low-resume", "resume")
		f.info(f.root, p.Source.ID, "")
		if f.engine.effects()["clone"] != before+1 {
			t.Fatal("public root bypass")
		}
		b := f.get(f.root, p.Source.ID)
		f.assertLedger(
			"public-root-bypass",
			"root",
			3,
			[]string{p.Branches[0].VMID, p.Branches[1].VMID, b.VMID},
			[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID},
			shape,
		)
	})
}

func TestProductionOwnerBudgetLifecyclePeakHTTP(t *testing.T) {
	for _, limit := range []int64{1024, 1023} {
		t.Run(fmt.Sprintf("limit-%d", limit), func(t *testing.T) {
			f := newOwnerFixture(t)
			shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
			f.budget("alice", 3, limit)
			c := f.client("alice")
			p := f.prefix(c, shape)
			running := f.resumed(p, c, "")
			r, err := c.Change(
				f.ctx,
				running.ID,
				"life-rollback",
				sandboxd.Command{Action: "rollback", CheckpointID: p.CP.ID},
			)
			r = ownerMust(t, r, err)
			disk := f.engine.physical(r.Sandbox.VMID)
			disks := []string{p.Branches[0].VMID, p.Branches[1].VMID}
			for id := range disk.Disks {
				disks = append(disks, id)
			}
			if len(disk.Disks) != 2 || len(disk.Backings) != 2 {
				t.Fatal("restore discarded disk/backing identity")
			}
			f.assertLedger(
				"rollback-832",
				"alice",
				3,
				disks,
				[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID},
				shape,
			)
			if limit == 1023 {
				before, eff, agents, ops := f.ledger(), f.engine.effects(), f.agent.effects(), len(
					f.ops(),
				)
				_, err = c.Change(
					f.ctx,
					running.ID,
					"life-second-pause",
					sandboxd.Command{Action: "pause"},
				)
				if !ownerCode(err, api.Exhausted) ||
					!reflect.DeepEqual(before, f.ledger()) ||
					!reflect.DeepEqual(eff, f.engine.effects()) ||
					!reflect.DeepEqual(agents, f.agent.effects()) ||
					len(f.ops()) != ops {
					t.Fatal("second pause low changed admission state")
				}
				f.budget("alice", 3, 1024)
			}
			gate := newOwnerGate()
			f.engine.gateDelete(gate)
			defer gate.open()
			type result struct {
				r   sandboxd.Result
				err error
			}
			done := make(chan result, 1)
			go func() {
				r, err := c.Change(
					f.ctx,
					running.ID,
					"life-second-pause",
					sandboxd.Command{Action: "pause"},
				)
				done <- result{r, err}
			}()
			ownerAwait(t, f.ctx, gate.entered, "second pause physical deletion")
			var h2 string
			if err = f.db.QueryRowContext(f.ctx, "SELECT json_extract(body,'$.snapshot_id') FROM sandboxes WHERE id=?", running.ID).
				Scan(&h2); err != nil {
				t.Fatal(err)
			}
			if h2 == "" || h2 == p.SleepSID || h2 == p.CP.SnapshotID ||
				h2 == p.ForkSID {
				t.Fatal("second sleep identity")
			}
			f.assertLedger(
				"second-pause-1024",
				"alice",
				3,
				disks,
				[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID, h2},
				shape,
			)
			gate.open()
			var final result
			select {
			case final = <-done:
			case <-f.ctx.Done():
				t.Fatal("second pause completion")
			}
			ownerMust(t, final.r, final.err)
			before, agents := f.engine.effects(), f.agent.effects()
			vms := f.engine.inventorySnapshot()
			f.change(c, running.ID, "life-zero-wake", "release")
			after := f.engine.inventorySnapshot()
			if f.engine.effects()["clone"] != before["clone"] ||
				!reflect.DeepEqual(agents, f.agent.effects()) ||
				!reflect.DeepEqual(vms, after) {
				t.Fatal("zero wake release woke guest")
			}
			f.deleteCP(c, p.CP)
			for _, b := range p.Branches {
				f.change(c, b.ID, "release-"+b.ID, "release")
			}
		})
	}
}

func TestProductionOwnerBudgetRetainedIdentityHTTP(t *testing.T) {
	f := newOwnerFixture(t)
	shape := api.Shape{VCPUs: 1, MemoryMiB: 128, DiskMiB: 64}
	f.budget("alice", 3, 1024)
	c := f.client("alice")
	p := f.prefix(c, shape)
	running := f.resumed(p, c, "")
	before := f.engine.effects()["clone"]
	if _, err := f.engine.GetSnapshot(
		f.ctx,
		p.SleepSID,
	); !ownerCode(
		err,
		api.NotFound,
	) {
		t.Fatal("retired catalog should be absent")
	}
	if !f.engine.physical(running.VMID).Backings[p.SleepSID] {
		t.Fatal("retired SID lost live backing")
	}
	f.engine.mu.Lock()
	f.engine.snapshotQueryError = true
	f.engine.mu.Unlock()
	f.reopen()
	c = f.client("alice")
	f.assertLedger(
		"query-error-reopen",
		"alice",
		3,
		[]string{p.Branches[0].VMID, p.Branches[1].VMID, running.VMID},
		[]string{p.CP.SnapshotID, p.ForkSID, p.SleepSID},
		shape,
	)
	var receipt api.Operation
	f.engine.mu.Lock()
	for _, o := range f.engine.ops {
		if o.Request.Action == "clone" && o.VMID == running.VMID {
			receipt = ownerCopy(o)
		}
	}
	f.engine.mu.Unlock()
	fixed, _ := json.Marshal(receipt)
	f.engine.mu.Lock()
	f.engine.snapshotQueryError = false
	f.engine.mu.Unlock()
	again, err := f.engine.Submit(f.ctx, receipt.Request)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(again)
	if !bytes.Equal(raw, fixed) {
		t.Fatal("same key receipt changed")
	}
	autoKey := "auto-resume:" + p.Source.ID + ":" + p.SleepSID + ":0"
	replay, err := c.Change(
		f.ctx,
		p.Source.ID,
		autoKey,
		sandboxd.Command{Action: "resume"},
	)
	replay = ownerMust(t, replay, err)
	if replay.Sandbox == nil || replay.Sandbox.VMID != running.VMID ||
		replay.Sandbox.Instance != running.Instance {
		t.Fatal("public same-key resume result changed")
	}
	f.change(c, running.ID, "retained-release", "release")
	// Reopen runs actual reconciliation before the explicitly observed quiet gate.
	f.reopen()
	c = f.client("alice")
	f.assertLedger(
		"real-VM-delete-retired-D-and-backing",
		"alice",
		2,
		[]string{p.Branches[0].VMID, p.Branches[1].VMID},
		[]string{p.CP.SnapshotID, p.ForkSID},
		shape,
	)
	replayAfter, err := c.Change(
		f.ctx,
		p.Source.ID,
		autoKey,
		sandboxd.Command{Action: "resume"},
	)
	replayAfter = ownerMust(t, replayAfter, err)
	if replayAfter.OperationID != replay.OperationID ||
		!reflect.DeepEqual(replayAfter.Sandbox, replay.Sandbox) {
		t.Fatal(
			"public historical result was rewritten after physical deletion",
		)
	}
	got, err := f.engine.FindOperation(f.ctx, receipt.Key)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(got)
	if !bytes.Equal(raw, fixed) || f.engine.effects()["clone"] != before {
		t.Fatal("receipt witness was enriched or clone repeated")
	}
	f.deleteCP(c, p.CP)
	for _, b := range p.Branches {
		f.change(c, b.ID, "release-"+b.ID, "release")
	}
}
