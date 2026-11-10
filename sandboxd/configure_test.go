package sandboxd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

// publicAddrResolver answers every lookup with one public address so the real
// egress guard accepts the destination without host DNS.
type publicAddrResolver struct{}

func (publicAddrResolver) LookupNetIP(
	context.Context,
	string,
	string,
) ([]netip.Addr, error) {
	return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
}

// upstreamDialer routes every guarded numeric dial to the local test upstream.
func upstreamDialer(
	target string,
) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		h, _, err := net.SplitHostPort(addr)
		if err != nil || net.ParseIP(h) == nil {
			return nil, errors.New("dialer received a DNS name")
		}
		return (&net.Dialer{}).DialContext(ctx, network, target)
	}
}

// egressFixture runs a real Service over real SQLite with an actual
// egress.Manager. Guest proxy listeners live under the fake engine's private
// socket directory; upstream dials reach only the local test server. The
// returned factory builds a fresh manager for a simulated daemon restart.
func egressFixture(
	t *testing.T,
) (*Service, *fakeEngine, *fakeAgent, Principal, func() *egress.Manager) {
	t.Helper()
	upstream := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "upstream "+r.Host)
		}),
	)
	t.Cleanup(upstream.Close)
	f := newFakeEngine()
	// Keep the socket base short: proxy listeners append _2049/_2050 and unix
	// socket paths are length limited.
	sockDir, err := os.MkdirTemp("", "eg")
	if err != nil {
		t.Fatal(err)
	}
	f.sockDir = sockDir
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })
	a := &fakeAgent{}
	newManager := func() *egress.Manager {
		manager, err := egress.New(egress.Options{
			Resolver:    publicAddrResolver{},
			DialContext: upstreamDialer(upstream.Listener.Addr().String()),
			Audit:       func(egress.AuditEvent) {},
		})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	manager := newManager()
	s, err := Open(
		Options{
			StateDir:   t.TempDir(),
			Initialize: true,
			RootToken:  "root-secret",
			Engine:     f,
			Agent:      a,
			Egress:     manager,
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
	return s, f, a, p, newManager
}

// restartEgress reopens the service on the same state directory with a fresh
// egress manager, modeling a daemon restart.
func restartEgress(
	t *testing.T,
	s *Service,
	newManager func() *egress.Manager,
) *Service {
	t.Helper()
	opts := s.opts
	opts.Initialize = false
	opts.Egress = newManager()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

func requireDataReadyBlocked(t *testing.T, s *Service, p Principal, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.DataReady(ctx, p, id); !isCode(err, ErrConflict) {
		t.Fatalf("fenced sandbox admitted to data: %v", err)
	}
}

func claimRelay(
	t *testing.T,
	s *Service,
	p Principal,
	key string,
	env map[string]EnvVar,
	policy egress.Policy,
) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := s.Execute(
		ctx,
		p,
		key,
		Command{
			Action:     "claim",
			TemplateID: "python",
			Env:        env,
			Policy:     policy,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sandbox == nil {
		t.Fatal("claim returned no sandbox")
	}
	return r
}

// proxyHTTPPath locates the live relay listener of a claimed sandbox.
func proxyHTTPPath(t *testing.T, s *Service, f *fakeEngine, id string) string {
	t.Helper()
	b, err := s.Get(
		context.Background(),
		Principal{Root: true, TenantID: "root"},
		id,
	)
	if err != nil {
		t.Fatal(err)
	}
	v, err := f.GetVM(context.Background(), b.VMID)
	if err != nil {
		t.Fatal(err)
	}
	return v.VSockPath + "_2049"
}

func proxyClient(t *testing.T, httpPath string) *http.Client {
	t.Helper()
	proxyURL, _ := url.Parse("http://proxy.invalid")
	tr := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", httpPath)
		},
		DisableKeepAlives: true,
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}
}

func proxyGet(t *testing.T, httpPath, url string) (int, string) {
	t.Helper()
	resp, err := proxyClient(t, httpPath).Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(body)
}

// holdTunnel keeps one hijacked CONNECT stream through the relay open.
func holdTunnel(t *testing.T, httpPath, authority string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("unix", httpPath, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = fmt.Fprintf(
		conn,
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n",
		authority,
		authority,
	); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("CONNECT status: %d", resp.StatusCode)
	}
	if reader.Buffered() != 0 {
		t.Fatal("unexpected CONNECT response bytes")
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// assertDataPathDead proves neither new connections nor held streams can reach
// the upstream through a revoked relay listener.
func assertDataPathDead(t *testing.T, httpPath string, held net.Conn) {
	t.Helper()
	conn, err := net.DialTimeout("unix", httpPath, 500*time.Millisecond)
	if err == nil {
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = io.WriteString(
			conn,
			"GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n",
		)
		resp, readErr := http.ReadResponse(bufio.NewReader(conn), nil)
		_ = conn.Close()
		if readErr == nil {
			_ = resp.Body.Close()
			t.Fatal("new connection reached upstream after revocation")
		}
	}
	if _, err = os.Stat(httpPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("revoked proxy socket remains: %v", err)
	}
	_ = held.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	if n, err := held.Read(one[:]); n != 0 || err == nil {
		t.Fatal("held tunnel survived revocation")
	}
}

func rotatedSecretEnv(value string) map[string]EnvVar {
	no := false
	return map[string]EnvVar{
		"TOKEN": {
			Value: value,
			Guest: &no,
			Inject: &Injection{
				Hosts:       []string{"example.com"},
				Header:      "Authorization",
				Placeholder: "phantom",
			},
		},
	}
}

// gateAgent consumes one gate per call. Cleanup always releases a blocked
// worker, including when an assertion fails before the explicit release.
func gateAgent(
	t *testing.T,
	a *fakeAgent,
	phase string,
) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	a.mu.Lock()
	if phase == "info" {
		a.infoEntered, a.infoGate = entered, release
	} else {
		a.activateEntered, a.activateGate = entered, release
	}
	a.mu.Unlock()
	return entered, open
}

func awaitAgent(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("agent call did not reach channel gate")
	}
}

func awaitWorker(t *testing.T, s *Service, id string) {
	t.Helper()
	s.mu.Lock()
	done := s.active[id]
	s.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("operation worker did not exit")
		}
	}
}

// workloadRelayAgent preserves the lifecycle fixture and supplies a separate
// guest workload stream for the actual Service HTTP Upgrade relay.
type workloadRelayAgent struct {
	*fakeAgent

	peers chan net.Conn
}

func (a *workloadRelayAgent) Connect(
	ctx context.Context,
	_ api.VM,
) (net.Conn, error) {
	up, peer := net.Pipe()
	select {
	case a.peers <- peer:
		return up, nil
	case <-ctx.Done():
		_ = up.Close()
		_ = peer.Close()
		return nil, ctx.Err()
	}
}

// openWorkloadRelay returns the client and guest endpoints only after the real
// Service has admitted and tracked both sides. Cleanup joins the hijacked handler.
func openWorkloadRelay(
	t *testing.T,
	s *Service,
	a *workloadRelayAgent,
	p Principal,
	id string,
) (net.Conn, net.Conn) {
	t.Helper()
	done := make(chan struct{})
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			s.relay(w, r, p, id)
		}),
	)
	var client, peer net.Conn
	started := false
	t.Cleanup(func() {
		if client != nil {
			_ = client.Close()
		}
		if peer != nil {
			_ = peer.Close()
		} else {
			select {
			case pending := <-a.peers:
				_ = pending.Close()
			default:
			}
		}
		server.Close()
		if started {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("workload relay handler did not exit")
			}
		}
	})
	var err error
	client, err = net.DialTimeout(
		"tcp",
		server.Listener.Addr().String(),
		2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.WriteString(
		client,
		"GET /connect HTTP/1.1\r\nHost: sandbox\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n",
	); err != nil {
		t.Fatal(err)
	}
	started = true
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("workload relay upgrade failed: %v %v", response, err)
	}
	select {
	case peer = <-a.peers:
	case <-time.After(5 * time.Second):
		t.Fatal("workload guest endpoint missing")
	}
	return client, peer
}

func exchangeWorkload(t *testing.T, client, guest net.Conn) {
	t.Helper()
	for _, direction := range []struct {
		from, to net.Conn
		payload  string
	}{{client, guest, "client-to-guest"}, {guest, client, "guest-to-client"}} {
		_ = direction.from.SetDeadline(time.Now().Add(3 * time.Second))
		_ = direction.to.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(
			direction.from,
			direction.payload,
		); err != nil {
			t.Fatalf("workload write: %v", err)
		}
		data := make([]byte, len(direction.payload))
		if _, err := io.ReadFull(direction.to, data); err != nil {
			t.Fatalf("workload read: %v", err)
		}
		if string(data) != direction.payload {
			t.Fatal("workload relay changed bytes")
		}
	}
}

func requireStreamClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	n, err := conn.Read(one[:])
	var timeout net.Error
	if n != 0 || err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
		t.Fatalf("revoked stream did not close: bytes=%d error=%v", n, err)
	}
}

// admissionStreamAgent gates the real HTTP consumers after obtaining an owned
// guest stream. Lifecycle calls still use the existing agent fixture.
type admissionStreamAgent struct {
	*fakeAgent

	peers   chan net.Conn
	entered chan struct{}
	release chan struct{}
	armed   atomic.Bool
}

func (a *admissionStreamAgent) stream(ctx context.Context) (net.Conn, error) {
	up, peer := net.Pipe()
	select {
	case a.peers <- peer:
	case <-ctx.Done():
		_ = up.Close()
		_ = peer.Close()
		return nil, ctx.Err()
	}
	if a.armed.CompareAndSwap(true, false) {
		close(a.entered)
		select {
		case <-a.release:
		case <-ctx.Done():
			_ = up.Close()
			return nil, ctx.Err()
		}
	}
	return up, nil
}

func (a *admissionStreamAgent) Connect(
	ctx context.Context,
	_ api.VM,
) (net.Conn, error) {
	return a.stream(ctx)
}

func (a *admissionStreamAgent) Forward(
	ctx context.Context,
	_ api.VM,
	_ int,
) (net.Conn, error) {
	return a.stream(ctx)
}

// configureRevokeGate pauses only scheduling after actual Manager cleanup.
// Submit has already committed its operation/fence and closed registered streams.
type configureRevokeGate struct {
	*egress.Manager

	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (g *configureRevokeGate) Revoke(id string) error {
	hold := g.armed.CompareAndSwap(true, false)
	err := g.Manager.Revoke(id)
	if hold {
		close(g.entered)
		select {
		case <-g.release:
		case <-time.After(5 * time.Second):
			return errors.New("configure scheduling gate timed out")
		}
	}
	return err
}

func openAdmissionGate(t *testing.T) (chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(open)
	return gate, open
}

// startAdmissionHTTP owns all endpoints and joins the actual HTTP handler, also
// when a failure occurs before the upgrade or before receiving the guest peer.
func startAdmissionHTTP(
	t *testing.T,
	s *Service,
	a *admissionStreamAgent,
	p Principal,
	b Sandbox,
	mode string,
	buffered bool,
) (net.Conn, *bufio.Reader) {
	t.Helper()
	path := "/connect"
	if mode == "preview" {
		var err error
		path, err = s.PreviewURL(
			context.Background(),
			p,
			b.ID,
			8080,
			time.Minute,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			if mode == "preview" {
				s.preview(w, r)
			} else {
				s.relay(w, r, p, b.ID)
			}
		}),
	)
	client, err := net.DialTimeout(
		"tcp",
		server.Listener.Addr().String(),
		2*time.Second,
	)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		for {
			select {
			case peer := <-a.peers:
				_ = peer.Close()
			default:
				server.Close()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("admission HTTP handler did not exit")
				}
				return
			}
		}
	})
	_ = client.SetDeadline(time.Now().Add(10 * time.Second))
	// Relay denial must not copy workload bytes buffered with the headers.
	payload := ""
	if buffered {
		payload = "buffered-workload"
	}
	_, err = fmt.Fprintf(
		client,
		"GET %s HTTP/1.1\r\nHost: sandbox\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n%s",
		path,
		payload,
	)
	if err != nil {
		t.Fatal(err)
	}
	return client, bufio.NewReader(client)
}

func admissionPeer(t *testing.T, a *admissionStreamAgent) net.Conn {
	t.Helper()
	select {
	case peer := <-a.peers:
		t.Cleanup(func() { _ = peer.Close() })
		_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
		return peer
	case <-time.After(5 * time.Second):
		t.Fatal("owned admission upstream missing")
		return nil
	}
}

func requireAdmissionSource(
	t *testing.T,
	s *Service,
	f *fakeEngine,
	p Principal,
	before Sandbox,
	activations int,
) {
	t.Helper()
	after, err := s.Get(context.Background(), p, before.ID)
	if err != nil || after.State != "running" || after.Policy.Mode != "none" ||
		after.Instance != before.Instance || after.Epoch != before.Epoch ||
		after.ResumeAttempt != before.ResumeAttempt || !after.Deadline.Equal(before.Deadline) {
		t.Fatal("admission changed source identity, lease or policy", err)
	}
	vm, err := f.GetVM(context.Background(), before.VMID)
	if err != nil || vm.NetworkActual || vm.NetworkEnabled {
		t.Fatal("none admission enabled NIC", err)
	}
	a := s.opts.Agent.(*admissionStreamAgent)
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.activations) != activations {
		t.Fatal("admission replayed activation")
	}
}

func submitGatedConfigure(
	t *testing.T,
	s *Service,
	p Principal,
	id string,
	gate *configureRevokeGate,
) {
	t.Helper()
	gate.armed.Store(true)
	submitted := make(chan Operation, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	go func() {
		o, err := s.Submit(ctx, p, "concurrent-configure", Command{
			Action:    "configure",
			SandboxID: id,
			Env:       map[string]EnvVar{},
			Policy:    egress.Policy{Mode: "none"},
		})
		if err != nil {
			t.Error("configure submit", err)
		}
		submitted <- o
	}()
	t.Cleanup(func() {
		defer cancel()
		select {
		case o := <-submitted:
			if o.ID != "" {
				waitCtx, stop := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stop()
				if _, err := s.Wait(waitCtx, p, o.ID); err != nil {
					t.Error("configure completion", err)
				}
			}
		case <-time.After(5 * time.Second):
			t.Error("configure submit did not exit")
		}
	})
}

func requirePendingConfigure(t *testing.T, s *Service, id string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		fence, err := configFence(ctx, s.store.db, id)
		if err != nil {
			t.Fatal(err)
		}
		if fence != "" {
			o, err := s.store.operation(ctx, fence)
			if err != nil || o.Status != "pending" ||
				o.Command.Action != "configure" {
				t.Fatal("configure fence lacks durable pending intent", err)
			}
			return fence
		}
		if ctx.Err() != nil {
			t.Fatal("configure failed to persist during blocked admission")
		}
		runtime.Gosched()
	}
}

func TestConfigureEgressAdmissionRejectsConcurrentIntent(t *testing.T) {
	for _, boundary := range []string{"none_cleanup", "none_cleanup_autorenew", "relay_connect", "preview_forward"} {
		t.Run(boundary, func(t *testing.T) {
			cleanup := strings.HasPrefix(boundary, "none_cleanup")
			s, f, original, p, _ := egressFixture(t)
			releaseAgent, openAgent := openAdmissionGate(t)
			a := &admissionStreamAgent{
				fakeAgent: original,
				peers:     make(chan net.Conn, 2),
				entered:   make(chan struct{}),
				release:   releaseAgent,
			}
			s.opts.Agent = a
			cleanupEntered := make(chan struct{})
			var cleanupOnce sync.Once
			releaseCleanup, openCleanup := openAdmissionGate(t)
			manager, err := egress.New(egress.Options{
				InstallDirect: func(context.Context, egress.Binding, egress.Policy) (func() error, error) {
					return func() error {
						cleanupOnce.Do(func() { close(cleanupEntered) })
						select {
						case <-releaseCleanup:
							return nil
						case <-time.After(5 * time.Second):
							return errors.New("actual cleanup gate timed out")
						}
					}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.opts.Egress.Close(); err != nil {
				t.Fatal(err)
			}
			releaseSubmit, openSubmit := openAdmissionGate(t)
			gate := &configureRevokeGate{
				Manager: manager,
				entered: make(chan struct{}),
				release: releaseSubmit,
			}
			s.opts.Egress = gate
			b := *claimRelay(t, s, p, "claim", map[string]EnvVar{}, egress.Policy{Mode: "none"}).Sandbox
			a.mu.Lock()
			activations := len(a.activations)
			a.mu.Unlock()
			mode := "relay"
			if boundary == "preview_forward" {
				mode = "preview"
			}
			if boundary == "none_cleanup_autorenew" {
				b.AutoRenew = true
				if err = saveSandbox(
					context.Background(),
					s.store.db,
					b,
				); err != nil {
					t.Fatal(err)
				}
			}
			if cleanup {
				vm, e := f.GetVM(context.Background(), b.VMID)
				if e != nil {
					t.Fatal(e)
				}
				if err = manager.Bind(
					context.Background(),
					b.ID,
					b.TenantID,
					vm.VSockPath,
					egress.Policy{Mode: "direct"},
					nil,
				); err != nil {
					t.Fatal(err)
				}
			} else {
				a.armed.Store(true)
			}
			client, reader := startAdmissionHTTP(
				t,
				s,
				a,
				p,
				b,
				mode,
				mode == "relay",
			)
			if cleanup {
				awaitAgent(t, cleanupEntered)
			} else {
				awaitAgent(t, a.entered)
			}
			submitGatedConfigure(t, s, p, b.ID, gate)
			// Release and join gates before the HTTP and Submit cleanup callbacks.
			t.Cleanup(func() { openCleanup(); openAgent(); openSubmit() })
			fence := requirePendingConfigure(t, s, b.ID)
			openCleanup()
			awaitAgent(t, gate.entered)
			openAgent()
			var peer net.Conn
			if !cleanup {
				peer = admissionPeer(t, a)
			}
			var forwarded <-chan bool
			if mode == "preview" {
				probe := make(chan bool, 1)
				probeDone := make(chan struct{})
				t.Cleanup(func() {
					_ = peer.Close()
					select {
					case <-probeDone:
					case <-time.After(5 * time.Second):
						t.Error("preview denial probe did not exit")
					}
				})
				forwarded = probe
				go func() {
					defer close(probeDone)
					reader := bufio.NewReader(peer)
					first, readErr := reader.Peek(1)
					var req *http.Request
					if readErr == nil {
						req, readErr = http.ReadRequest(reader)
					}
					if readErr == nil {
						_ = req.Body.Close()
						_, _ = io.WriteString(
							peer,
							"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n",
						)
					}
					probe <- len(first) != 0
				}()
			}
			response, err := http.ReadResponse(reader, nil)
			if err != nil {
				t.Fatal("admission response", err)
			}
			_ = response.Body.Close()
			want := http.StatusConflict
			if mode == "preview" {
				want = http.StatusBadGateway
			}
			if response.StatusCode != want {
				t.Errorf(
					"fenced admission status=%d want=%d",
					response.StatusCode,
					want,
				)
				_ = client.Close()
			}
			if peer != nil {
				if forwarded != nil {
					select {
					case copied := <-forwarded:
						if copied {
							t.Error(
								"fenced preview forwarded an upstream request",
							)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("preview probe did not exit")
					}
				}
				requireStreamClosed(t, peer)
			}
			requireFence(t, s, b.ID, fence)
			requirePendingConfigure(t, s, b.ID)
			if _, bound := manager.Binding(b.ID); bound {
				t.Fatal("actual residual cleanup incomplete")
			}
			s.mu.Lock()
			registered := len(s.connections[b.ID])
			s.mu.Unlock()
			if registered != 0 {
				t.Fatal("fenced admission leaked connection registrations")
			}
			requireAdmissionSource(t, s, f, p, b, activations)
			openSubmit()
		})
	}
}

// openAdmittedStream completes the actual relay or preview upgrade and leaves
// both endpoints live, so maintenance and later configure exercise ownership.
func openAdmittedStream(
	t *testing.T,
	s *Service,
	a *admissionStreamAgent,
	p Principal,
	b Sandbox,
	mode string,
) (net.Conn, net.Conn) {
	t.Helper()
	client, reader := startAdmissionHTTP(t, s, a, p, b, mode, false)
	peer := admissionPeer(t, a)
	var handshake <-chan error
	if mode == "preview" {
		done := make(chan error, 1)
		exited := make(chan struct{})
		t.Cleanup(func() {
			_ = peer.Close()
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Error("preview handshake did not exit during cleanup")
			}
		})
		handshake = done
		go func() {
			defer close(exited)
			req, err := http.ReadRequest(bufio.NewReader(peer))
			if err == nil {
				_ = req.Body.Close()
				_, err = io.WriteString(
					peer,
					"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n",
				)
			}
			done <- err
		}()
	}
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatal("healthy admission did not upgrade", response, err)
	}
	if handshake != nil {
		select {
		case err = <-handshake:
			if err != nil {
				t.Fatal("preview upstream handshake", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("preview handshake did not exit")
		}
	}
	exchangeWorkload(t, client, peer)
	return client, peer
}

func TestConfigureEgressAdmissionRegistersBeforeIntent(t *testing.T) {
	for _, mode := range []string{"relay", "preview"} {
		t.Run(mode, func(t *testing.T) {
			s, f, original, p, _ := egressFixture(t)
			a := &admissionStreamAgent{
				fakeAgent: original,
				peers:     make(chan net.Conn, 2),
			}
			s.opts.Agent = a
			releaseSubmit, openSubmit := openAdmissionGate(t)
			gate := &configureRevokeGate{
				Manager: s.opts.Egress.(*egress.Manager),
				entered: make(chan struct{}),
				release: releaseSubmit,
			}
			s.opts.Egress = gate
			b := *claimRelay(t, s, p, "claim", map[string]EnvVar{}, egress.Policy{Mode: "none"}).Sandbox
			a.mu.Lock()
			activations := len(a.activations)
			a.mu.Unlock()
			client, peer := openAdmittedStream(t, s, a, p, b, mode)
			for range 2 {
				s.reconcile()
				ctx, cancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				_, err := s.DataReady(ctx, p, b.ID)
				cancel()
				if err != nil {
					t.Fatal("healthy repeated admission", err)
				}
				exchangeWorkload(t, client, peer)
			}
			fresh, freshPeer := openAdmittedStream(t, s, a, p, b, mode)
			exchangeWorkload(t, client, peer)
			requireFence(t, s, b.ID, "")
			if _, bound := gate.Binding(b.ID); bound {
				t.Fatal("healthy none created a persistent proxy binding")
			}
			requireAdmissionSource(t, s, f, p, b, activations)
			submitGatedConfigure(t, s, p, b.ID, gate)
			t.Cleanup(openSubmit)
			awaitAgent(t, gate.entered)
			requirePendingConfigure(t, s, b.ID)
			for _, stream := range []net.Conn{client, peer, fresh, freshPeer} {
				requireStreamClosed(t, stream)
			}
			s.mu.Lock()
			registered := len(s.connections[b.ID])
			s.mu.Unlock()
			if registered != 0 {
				t.Fatal("configure admission retained registered connections")
			}
			requireAdmissionSource(t, s, f, p, b, activations)
			openSubmit()
		})
	}
}

func TestConfigureEgressNonePolicyKeepsWorkloadConnections(t *testing.T) {
	for _, scenario := range []string{"claim", "fork", "residual_tracked", "residual_untracked"} {
		t.Run(scenario, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			a := &workloadRelayAgent{
				fakeAgent: agent,
				peers:     make(chan net.Conn, 1),
			}
			s.opts.Agent = a
			r := claimRelay(
				t,
				s,
				p,
				"claim",
				map[string]EnvVar{},
				egress.Policy{Mode: "none"},
			)
			before := *r.Sandbox
			if scenario == "fork" {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					10*time.Second,
				)
				defer cancel()
				fork, err := s.Execute(
					ctx,
					p,
					"fork",
					Command{
						Action:    "fork",
						SandboxID: before.ID,
						Count:     1,
						Policy:    egress.Policy{Mode: "none"},
					},
				)
				if err != nil || len(fork.Sandboxes) != 1 {
					t.Fatalf("fork: %v %v", fork, err)
				}
				before = fork.Sandboxes[0]
			}
			s.mu.Lock()
			initialBinding := s.bindings[before.ID] != ""
			s.mu.Unlock()
			if initialBinding {
				t.Error(
					"healthy none activation created a Service proxy binding",
				)
			}
			if _, bound := s.opts.Egress.(*egress.Manager).Binding(
				before.ID,
			); bound {
				t.Error(
					"healthy none activation created a Manager proxy binding",
				)
			}
			client, guest := openWorkloadRelay(t, s, a, p, before.ID)
			exchangeWorkload(t, client, guest)
			v, err := f.GetVM(context.Background(), before.VMID)
			if err != nil {
				t.Fatal(err)
			}
			manager := s.opts.Egress.(*egress.Manager)
			var held net.Conn
			if strings.HasPrefix(scenario, "residual_") {
				// Model an actual leftover wide proxy, independently of workload
				// connections and even when local fingerprint bookkeeping is lost.
				if err = manager.Revoke(before.ID); err != nil {
					t.Fatal(err)
				}
				if err = manager.Bind(
					context.Background(),
					before.ID,
					before.TenantID,
					v.VSockPath,
					egress.Policy{
						Mode:         "relay",
						AllowedHosts: []string{"example.com"},
					},
					nil,
				); err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				delete(s.bindings, before.ID)
				if scenario == "residual_tracked" {
					s.bindings[before.ID] = "residual"
				}
				s.mu.Unlock()
				held = holdTunnel(t, v.VSockPath+"_2049", "example.com:443")
			}
			agent.mu.Lock()
			activations := len(agent.activations)
			agent.mu.Unlock()
			for pass := 1; pass <= 2; pass++ {
				// Explicit maintenance gates make connection closure deterministic.
				s.reconcile()
				t.Logf("completed explicit reconcile pass %d", pass)
				exchangeWorkload(t, client, guest)
				if held != nil {
					assertDataPathDead(t, v.VSockPath+"_2049", held)
				}
				s.mu.Lock()
				bound := s.bindings[before.ID] != ""
				s.mu.Unlock()
				if bound {
					t.Fatal("none retained a Service proxy binding")
				}
				if _, bound = manager.Binding(before.ID); bound {
					t.Fatal("none retained a Manager proxy binding")
				}
			}
			fresh, freshGuest := openWorkloadRelay(t, s, a, p, before.ID)
			exchangeWorkload(t, fresh, freshGuest)
			agent.mu.Lock()
			afterActivations := len(agent.activations)
			agent.mu.Unlock()
			if afterActivations != activations {
				t.Fatal("maintenance replayed guest activation")
			}
			after, err := s.Get(context.Background(), p, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Instance != before.Instance ||
				after.Epoch != before.Epoch ||
				!after.Deadline.Equal(before.Deadline) {
				t.Fatal("maintenance changed identity or lease")
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, op := range f.ops {
				if op.Request.Action == "network" && op.Request.NetworkEnabled {
					t.Fatal("none enabled the engine NIC")
				}
			}
		})
	}
}

func TestConfigureEgressClosesOldWorkloadConnections(t *testing.T) {
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	for _, target := range []string{"none", "narrow", "secret"} {
		t.Run(target, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			a := &workloadRelayAgent{
				fakeAgent: agent,
				peers:     make(chan net.Conn, 1),
			}
			s.opts.Agent = a
			env, nextEnv := map[string]EnvVar{}, map[string]EnvVar{}
			policy := egress.Policy{Mode: "none"}
			switch target {
			case "narrow":
				policy = egress.Policy{
					Mode:         "relay",
					AllowedHosts: []string{"other.example"},
				}
			case "secret":
				env, nextEnv = rotatedSecretEnv(
					"unused-old-credential",
				), rotatedSecretEnv(
					"unused-new-credential",
				)
				policy = wide
			}
			r := claimRelay(t, s, p, "claim", env, wide)
			path := proxyHTTPPath(t, s, f, r.Sandbox.ID)
			held := holdTunnel(t, path, "example.com:443")
			client, guest := openWorkloadRelay(t, s, a, p, r.Sandbox.ID)
			exchangeWorkload(t, client, guest)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			if _, err := s.Execute(
				ctx,
				p,
				"configure",
				Command{
					Action:    "configure",
					SandboxID: r.Sandbox.ID,
					Env:       nextEnv,
					Policy:    policy,
				},
			); err != nil {
				t.Fatal(err)
			}
			requireStreamClosed(t, client)
			requireStreamClosed(t, held)
			requireFence(t, s, r.Sandbox.ID, "")
			if target == "none" {
				assertDataPathDead(t, path, held)
			} else {
				host := "example.com"
				if target == "narrow" {
					host = "other.example"
					if status, _ := proxyGet(
						t,
						path,
						"http://example.com/",
					); status != http.StatusForbidden {
						t.Fatalf("old wide permission survived: %d", status)
					}
				}
				if status, _ := proxyGet(
					t,
					path,
					"http://"+host+"/",
				); status != http.StatusOK {
					t.Fatalf("new policy not delivered: %d", status)
				}
			}
			fresh, freshGuest := openWorkloadRelay(t, s, a, p, r.Sandbox.ID)
			for range 2 {
				s.reconcile()
				exchangeWorkload(t, fresh, freshGuest)
			}
		})
	}
}

func TestConfigureEgressRevokeFailureStaysClosed(t *testing.T) {
	for _, phase := range []string{"explicit_configure", "none_maintenance"} {
		t.Run(phase, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			a := &workloadRelayAgent{
				fakeAgent: agent,
				peers:     make(chan net.Conn, 1),
			}
			s.opts.Agent = a
			var fail atomic.Bool
			fail.Store(true)
			// Use the actual Manager's retryable teardown error. The installer
			// models host rule ownership without changing host networking.
			manager, err := egress.New(
				egress.Options{
					InstallDirect: func(context.Context, egress.Binding, egress.Policy) (func() error, error) {
						return func() error {
							if fail.Load() {
								return errors.New("injected teardown failure")
							}
							return nil
						}, nil
					},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.opts.Egress.Close(); err != nil {
				t.Fatal(err)
			}
			s.opts.Egress = manager
			t.Cleanup(func() { fail.Store(false) })
			policy := egress.Policy{Mode: "direct"}
			if phase == "none_maintenance" {
				policy.Mode = "none"
			}
			r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, policy)
			before := *r.Sandbox
			client, guest := openWorkloadRelay(t, s, a, p, before.ID)
			exchangeWorkload(t, client, guest)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			if phase == "none_maintenance" {
				v, err := f.GetVM(ctx, before.VMID)
				if err != nil {
					t.Fatal(err)
				}
				if err = manager.Revoke(before.ID); err != nil {
					t.Fatal(err)
				}
				if err = manager.Bind(
					ctx,
					before.ID,
					before.TenantID,
					v.VSockPath,
					egress.Policy{Mode: "direct"},
					nil,
				); err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				s.bindings[before.ID] = "residual"
				s.mu.Unlock()
				s.reconcile()
			} else {
				result, err := s.Execute(
					ctx,
					p,
					"configure-fails",
					Command{
						Action:    "configure",
						SandboxID: before.ID,
						Env:       map[string]EnvVar{},
						Policy:    egress.Policy{Mode: "none"},
					},
				)
				if err == nil {
					t.Fatal("configure masked the actual Manager Revoke error")
				}
				requireFence(t, s, before.ID, result.OperationID)
			}
			requireStreamClosed(t, client)
			blocked, err := s.Get(ctx, p, before.ID)
			if err != nil || blocked.LastError == "" ||
				blocked.Policy.Mode != before.Policy.Mode {
				t.Fatalf(
					"revoke failure lost saved policy or diagnostic: %v",
					err,
				)
			}
			if _, err = s.DataReady(
				ctx,
				p,
				before.ID,
			); !isCode(
				err,
				ErrConflict,
			) {
				t.Fatalf("revoke failure admitted workload: %v", err)
			}
			if _, bound := manager.Binding(before.ID); !bound {
				t.Fatal("failed teardown was reported as removed")
			}
			fail.Store(false)
			if _, err = s.Execute(
				ctx,
				p,
				"configure-retry",
				Command{
					Action:    "configure",
					SandboxID: before.ID,
					Env:       map[string]EnvVar{},
					Policy:    egress.Policy{Mode: "none"},
				},
			); err != nil {
				t.Fatal(err)
			}
			requireFence(t, s, before.ID, "")
			if _, bound := manager.Binding(before.ID); bound {
				t.Fatal("explicit retry retained proxy ownership")
			}
			fresh, freshGuest := openWorkloadRelay(t, s, a, p, before.ID)
			exchangeWorkload(t, fresh, freshGuest)
		})
	}
}

// requireWorkloadAdmissionBlocked exercises the real HTTP upgrade. If admission
// regresses, exchange bytes before reporting it and join the hijacked handler.
func requireWorkloadAdmissionBlocked(
	t *testing.T,
	s *Service,
	a *workloadRelayAgent,
	p Principal,
	id string,
) {
	t.Helper()
	done := make(chan struct{})
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(done)
			s.relay(w, r, p, id)
		}),
	)
	var client, peer net.Conn
	started := false
	defer func() {
		if client != nil {
			_ = client.Close()
		}
		if peer != nil {
			_ = peer.Close()
		}
		server.Close()
		if started {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("admission probe handler did not exit")
			}
		}
	}()
	var err error
	client, err = net.DialTimeout(
		"tcp",
		server.Listener.Addr().String(),
		2*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = client.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.WriteString(
		client,
		"GET /connect HTTP/1.1\r\nHost: sandbox\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n",
	); err != nil {
		t.Fatal(err)
	}
	started = true
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode == http.StatusSwitchingProtocols {
		select {
		case peer = <-a.peers:
		case <-time.After(5 * time.Second):
			t.Fatal("admitted probe has no guest endpoint")
		}
		exchangeWorkload(t, client, peer)
		t.Error(
			"fresh HTTP 101 workload transferred while direct teardown still fails",
		)
		return
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf(
			"admission returned HTTP %d, want conflict",
			response.StatusCode,
		)
	}
}

func TestConfigureEgressNoneAdmissionConfirmsRevocation(t *testing.T) {
	for _, backend := range []string{"manager", "install_direct"} {
		for _, fault := range []string{"save_ok", "save_abort", "save_ignored"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				s, f, agent, p, _ := egressFixture(t)
				a := &workloadRelayAgent{
					fakeAgent: agent,
					peers:     make(chan net.Conn, 1),
				}
				s.opts.Agent = a
				var fail, rulesOwned atomic.Bool
				var cleanupCalls atomic.Int32
				fail.Store(true)
				install := func(context.Context, egress.Binding, egress.Policy) (func() error, error) {
					rulesOwned.Store(true)
					return func() error {
						cleanupCalls.Add(1)
						if fail.Load() {
							return errors.New(
								"injected direct teardown failure",
							)
						}
						rulesOwned.Store(false)
						return nil
					}, nil
				}
				engine := &directTestEngine{fakeEngine: f}
				arm := func() {}
				if backend == "install_direct" {
					s.opts.Engine = engine
					install = s.InstallDirect
					// The existing engine boundary fails one operation. Rearm at
					// every synchronous admission gate to keep cleanup failing.
					arm = func() {
						engine.requestMu.Lock()
						engine.failRevoke = true
						engine.requestMu.Unlock()
					}
				}
				manager, err := egress.New(
					egress.Options{InstallDirect: install},
				)
				if err != nil {
					t.Fatal(err)
				}
				if err = s.opts.Egress.Close(); err != nil {
					t.Fatal(err)
				}
				s.opts.Egress = manager
				t.Cleanup(func() {
					fail.Store(false)
					engine.requestMu.Lock()
					engine.failRevoke = false
					engine.requestMu.Unlock()
				})
				r := claimRelay(
					t,
					s,
					p,
					"claim",
					map[string]EnvVar{},
					egress.Policy{Mode: "none"},
				)
				before := *r.Sandbox
				old, guest := openWorkloadRelay(t, s, a, p, before.ID)
				exchangeWorkload(t, old, guest)
				agent.mu.Lock()
				activations := len(agent.activations)
				agent.mu.Unlock()
				ctx, cancel := context.WithTimeout(
					context.Background(),
					15*time.Second,
				)
				defer cancel()
				v, err := f.GetVM(ctx, before.VMID)
				if err != nil {
					t.Fatal(err)
				}
				if err = manager.Bind(
					ctx,
					before.ID,
					before.TenantID,
					v.VSockPath,
					egress.Policy{
						Mode:         "direct",
						AllowedCIDRs: []string{"1.1.1.1/32"},
					},
					nil,
				); err != nil {
					t.Fatal(err)
				}
				// An untracked residual must be confirmed by the actual Manager.
				s.mu.Lock()
				delete(s.bindings, before.ID)
				s.mu.Unlock()
				switch fault {
				case "save_abort":
					_, err = s.store.db.Exec(
						`CREATE TRIGGER reject_diagnostic BEFORE UPDATE ON sandboxes
						WHEN json_extract(NEW.body,'$.last_error') IS NOT NULL
						BEGIN SELECT RAISE(ABORT,'injected diagnostic write failure'); END`,
					)
				case "save_ignored":
					_, err = s.store.db.Exec(
						`CREATE TRIGGER reject_diagnostic BEFORE UPDATE ON sandboxes
						WHEN json_extract(NEW.body,'$.last_error') IS NOT NULL
						BEGIN SELECT RAISE(IGNORE); END`,
					)
				}
				if err != nil {
					t.Fatal(err)
				}
				arm()
				s.reconcile()
				requireStreamClosed(t, old)
				after, err := s.Get(ctx, p, before.ID)
				if err != nil {
					t.Fatal(err)
				}
				diagnostic := after.LastError
				if (fault == "save_ok") != (diagnostic != "") {
					t.Fatal(
						"diagnostic write control/fault did not take effect",
					)
				}
				requireFence(t, s, before.ID, "")
				for range 2 {
					arm()
					if _, err = s.DataReady(
						ctx,
						p,
						before.ID,
					); !isCode(
						err,
						ErrConflict,
					) {
						t.Errorf(
							"incomplete teardown admitted DataReady: %v",
							err,
						)
					}
					arm()
					requireWorkloadAdmissionBlocked(t, s, a, p, before.ID)
					if info, bound := manager.Binding(
						before.ID,
					); !bound ||
						info.Mode != "direct" {
						t.Fatal("failed teardown lost actual Manager ownership")
					}
					vm, lookupErr := f.GetVM(ctx, before.VMID)
					if lookupErr != nil || vm.NetworkActual ||
						vm.NetworkEnabled {
						t.Fatalf("none NIC state changed: %v", lookupErr)
					}
					if backend == "install_direct" && vm.NetworkPolicy == nil {
						t.Fatal(
							"production teardown fault did not retain engine policy",
						)
					}
				}
				if backend == "manager" &&
					(!rulesOwned.Load() || cleanupCalls.Load() == 0) {
					t.Fatal(
						"fixture did not retain actual retryable direct ownership",
					)
				}
				if backend == "install_direct" {
					engine.requestMu.Lock()
					revokes := 0
					for _, request := range engine.requests {
						if request.Action == "network_policy" &&
							request.Policy == nil {
							revokes++
						}
					}
					engine.failRevoke = false
					engine.requestMu.Unlock()
					if revokes == 0 {
						t.Fatal(
							"production teardown fault did not reach policy revocation",
						)
					}
				}
				fail.Store(false)
				if fault != "save_ok" {
					if _, err = s.store.db.Exec(
						"DROP TRIGGER reject_diagnostic",
					); err != nil {
						t.Fatal(err)
					}
				}
				_, err = s.DataReady(ctx, p, before.ID)
				if (fault == "save_ok" && !isCode(err, ErrConflict)) ||
					(fault != "save_ok" && err != nil) {
					t.Fatalf(
						"cleanup retry changed diagnostic admission semantics: %v",
						err,
					)
				}
				after, err = s.Get(ctx, p, before.ID)
				if err != nil || after.LastError != diagnostic {
					t.Fatal("implicit cleanup changed the persisted diagnostic")
				}
				if _, bound := manager.Binding(
					before.ID,
				); bound &&
					fault != "save_ok" {
					t.Fatal(
						"successful admission cleanup retained Manager ownership",
					)
				}
				if fault == "save_ok" {
					if _, err = s.Execute(ctx, p, "configure-retry", Command{
						Action:    "configure",
						SandboxID: before.ID,
						Env:       map[string]EnvVar{},
						Policy:    egress.Policy{Mode: "none"},
					}); err != nil {
						t.Fatal(err)
					}
				}
				fresh, freshGuest := openWorkloadRelay(t, s, a, p, before.ID)
				for range 2 {
					s.reconcile()
					if _, err = s.DataReady(ctx, p, before.ID); err != nil {
						t.Fatal(err)
					}
					exchangeWorkload(t, fresh, freshGuest)
				}
				after, err = s.Get(ctx, p, before.ID)
				if err != nil || after.Instance != before.Instance ||
					after.Epoch != before.Epoch ||
					!after.Deadline.Equal(before.Deadline) ||
					after.ResumeAttempt != before.ResumeAttempt {
					t.Fatal("cleanup changed guest identity or lease")
				}
				agent.mu.Lock()
				afterActivations := len(agent.activations)
				agent.mu.Unlock()
				wantActivations := activations
				if fault == "save_ok" {
					wantActivations++
				}
				if afterActivations != wantActivations {
					t.Fatal("implicit cleanup replayed guest activation")
				}
				vm, err := f.GetVM(ctx, before.VMID)
				if err != nil || vm.NetworkActual || vm.NetworkEnabled ||
					vm.NetworkPolicy != nil {
					t.Fatal("cleanup left engine authorization behind")
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				for _, op := range f.ops {
					if op.Request.Action == "network" &&
						op.Request.NetworkEnabled {
						t.Fatal("none admission enabled NIC")
					}
				}
			})
		}
	}
}

func TestConfigureEgressNoneAdmissionWithoutManager(t *testing.T) {
	s, _, agent, p, _ := egressFixture(t)
	if err := s.opts.Egress.Close(); err != nil {
		t.Fatal(err)
	}
	s.opts.Egress = nil
	a := &workloadRelayAgent{fakeAgent: agent, peers: make(chan net.Conn, 1)}
	s.opts.Agent = a
	r := claimRelay(
		t,
		s,
		p,
		"claim",
		map[string]EnvVar{},
		egress.Policy{Mode: "none"},
	)
	client, guest := openWorkloadRelay(t, s, a, p, r.Sandbox.ID)
	for range 2 {
		s.reconcile()
		if _, err := s.DataReady(
			context.Background(),
			p,
			r.Sandbox.ID,
		); err != nil {
			t.Fatal(err)
		}
		exchangeWorkload(t, client, guest)
	}
}

func TestConfigureEgressPersistenceFailures(t *testing.T) {
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	narrow := egress.Policy{
		Mode:         "relay",
		AllowedHosts: []string{"other.example"},
	}
	for _, stage := range []string{"save_intent", "running_save", "final_save", "fence_delete", "fence_delete_ignored", "operation_result", "commit"} {
		t.Run(stage, func(t *testing.T) {
			s, f, agent, p, manager := egressFixture(t)
			r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
			before := *r.Sandbox
			path := proxyHTTPPath(t, s, f, before.ID)
			held := holdTunnel(t, path, "example.com:443")
			var published net.Conn
			var sql string
			switch stage {
			case "save_intent":
				// Fail only publication of the new environment. Diagnostic saves
				// of the old policy remain possible.
				sql = `CREATE TRIGGER fail_config BEFORE UPDATE ON sandboxes
				WHEN json_extract(NEW.body,'$.guest_env.REVISION')='new'
				BEGIN SELECT RAISE(ABORT,'injected config save failure'); END`
			case "final_save":
				// commitRunning performs the first write after Activate; fail
				// only the second write, in the final delivery transaction.
				sql = `CREATE TABLE fault_writes(n INTEGER);
				INSERT INTO fault_writes VALUES(0);
				CREATE TRIGGER count_config AFTER UPDATE ON sandboxes
				WHEN json_extract(NEW.body,'$.last_error') IS NULL
				BEGIN UPDATE fault_writes SET n=n+1; END;
				CREATE TRIGGER fail_config BEFORE UPDATE ON sandboxes
				WHEN (SELECT n FROM fault_writes)>0 AND json_extract(NEW.body,'$.last_error') IS NULL
				BEGIN SELECT RAISE(ABORT,'injected final save failure'); END`
			case "running_save":
				sql = `CREATE TRIGGER fail_config BEFORE UPDATE ON sandboxes
				WHEN json_extract(NEW.body,'$.last_error') IS NULL
				BEGIN SELECT RAISE(ABORT,'injected final save failure'); END`
			case "fence_delete":
				sql = `CREATE TRIGGER fail_config BEFORE DELETE ON configuration_fences
				BEGIN SELECT RAISE(ABORT,'injected fence retirement failure'); END`
			case "fence_delete_ignored":
				sql = `CREATE TRIGGER fail_config BEFORE DELETE ON configuration_fences
				BEGIN SELECT RAISE(IGNORE); END`
			case "operation_result":
				sql = `CREATE TRIGGER fail_config BEFORE UPDATE ON operations
				WHEN NEW.status='succeeded'
				BEGIN SELECT RAISE(ABORT,'injected operation result failure'); END`
			case "commit":
				// SQLite accepts the DELETE and result writes, then rejects COMMIT
				// because this deferred foreign key cannot be satisfied.
				sql = `CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);
				CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
				CREATE TRIGGER fail_config AFTER DELETE ON configuration_fences
				BEGIN INSERT INTO fault_child VALUES(1); END`
			}
			if stage == "save_intent" {
				if _, err := s.store.db.Exec(sql); err != nil {
					t.Fatal(err)
				}
			}
			entered, release := gateAgent(t, agent, "activate")
			ctx, cancel := context.WithTimeout(
				context.Background(),
				12*time.Second,
			)
			defer cancel()
			command := Command{
				Action:    "configure",
				SandboxID: before.ID,
				Env: map[string]EnvVar{
					"REVISION": {Value: "new"},
				},
				Policy: narrow,
			}
			op, err := s.Submit(ctx, p, "configure", command)
			if err != nil {
				t.Fatal(err)
			}
			if stage != "save_intent" {
				awaitAgent(t, entered)
				// The newly published binding really reaches upstream before the
				// final transaction fails, including an existing CONNECT stream.
				if status, _ := proxyGet(
					t,
					path,
					"http://other.example/",
				); status != 200 {
					t.Fatalf("new binding never served: %d", status)
				}
				published = holdTunnel(t, path, "other.example:443")
				if _, err = s.store.db.Exec(sql); err != nil {
					t.Fatal(err)
				}
			}
			release()
			awaitWorker(t, s, op.ID)
			stored, err := s.Operation(ctx, p, op.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != "failed" {
				t.Errorf(
					"failed persistence was not recorded as failure: %s",
					stored.Status,
				)
			}
			// Remove the injected fault before recovery checks. A missing fence
			// cannot be concealed by leaving a failing DELETE trigger installed.
			if _, err = s.store.db.Exec(
				"DROP TRIGGER fail_config",
			); err != nil {
				t.Fatal(err)
			}
			requireFence(t, s, before.ID, op.ID)
			b, err := s.Get(ctx, p, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			if b.LastError == "" {
				t.Error("persistence failure lost LastError")
			}
			if stage == "save_intent" &&
				(b.Env["REVISION"] != "" || strings.Join(b.Policy.AllowedHosts, ",") != "example.com") {
				t.Fatal("failed intent save overwrote old wide configuration")
			}
			assertDataPathDead(t, path, held)
			if published != nil {
				assertDataPathDead(t, path, published)
			}
			requireDataReadyBlocked(t, s, p, before.ID)
			s.reconcile()
			assertDataPathDead(t, path, held)
			restarted := restartEgress(t, s, manager)
			requireFence(t, restarted, before.ID, op.ID)
			requireDataReadyBlocked(t, restarted, p, before.ID)
			restarted.reconcile()
			assertDataPathDead(t, path, held)
			after, err := restarted.Get(ctx, p, before.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Instance != before.Instance ||
				after.Epoch != before.Epoch ||
				!after.Deadline.Equal(before.Deadline) {
				t.Fatal(
					"persistence failure recovery changed identity or lease",
				)
			}
			if _, err = restarted.Execute(ctx, p, "retry-new", Command{
				Action: "configure", SandboxID: before.ID,
				Env: map[string]EnvVar{}, Policy: narrow,
			}); err != nil {
				t.Fatal(err)
			}
			requireFence(t, restarted, before.ID, "")
			// The old operation's idempotency result remains fixed after repair.
			if _, err = restarted.Execute(
				ctx,
				p,
				"configure",
				command,
			); err == nil {
				t.Fatal("failed operation was changed to success")
			}
		})
	}
}

func TestConfigureEgressFenceRevokesWithoutVerifiedInventory(t *testing.T) {
	for _, inventory := range []string{"unverified", "unavailable", "mismatched"} {
		t.Run(inventory, func(t *testing.T) {
			s, f, _, p, _ := egressFixture(t)
			r := claimRelay(
				t,
				s,
				p,
				"claim",
				map[string]EnvVar{},
				egress.Policy{
					Mode:         "relay",
					AllowedHosts: []string{"example.com"},
				},
			)
			path := proxyHTTPPath(t, s, f, r.Sandbox.ID)
			held := holdTunnel(t, path, "example.com:443")
			// A recovered fence may coexist with a local proxy. Maintenance owns
			// revocation of that proxy even when it cannot prove VM authority.
			if err := recordConfigFence(
				context.Background(),
				s.store.db,
				r.Sandbox.ID,
				"op_crashed",
			); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			v := f.vms[r.Sandbox.VMID]
			switch inventory {
			case "unavailable":
				f.unavailable = true
			case "mismatched":
				v.Instance = "unproven"
			default:
				v.Verified = false
			}
			f.vms[v.ID] = v
			calls := len(f.ops)
			f.mu.Unlock()
			s.reconcile()
			assertDataPathDead(t, path, held)
			f.mu.Lock()
			afterCalls := len(f.ops)
			f.mu.Unlock()
			if afterCalls != calls {
				t.Fatal("maintenance mutated an unproven engine instance")
			}
			requireFence(t, s, r.Sandbox.ID, "op_crashed")
		})
	}
}

func TestConfigureEgressSupersedingFence(t *testing.T) {
	for _, phase := range []string{"info", "activate"} {
		t.Run(phase, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			wide := egress.Policy{
				Mode:         "relay",
				AllowedHosts: []string{"example.com"},
			}
			narrow := egress.Policy{
				Mode:         "relay",
				AllowedHosts: []string{"other.example"},
			}
			r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
			path := proxyHTTPPath(t, s, f, r.Sandbox.ID)
			held := holdTunnel(t, path, "example.com:443")
			entered, release := gateAgent(t, agent, phase)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				12*time.Second,
			)
			defer cancel()
			oldCommand := Command{
				Action:    "configure",
				SandboxID: r.Sandbox.ID,
				Env:       map[string]EnvVar{},
				Policy:    wide,
			}
			old, err := s.Submit(ctx, p, "old", oldCommand)
			if err != nil {
				t.Fatal(err)
			}
			awaitAgent(t, entered)
			var published net.Conn
			if phase == "activate" {
				published = holdTunnel(t, path, "example.com:443")
			}
			newEntered, newRelease := gateAgent(t, agent, "info")
			newOp, err := s.Submit(
				ctx,
				p,
				"new",
				Command{
					Action:    "configure",
					SandboxID: r.Sandbox.ID,
					Env:       map[string]EnvVar{},
					Policy:    narrow,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			requireFence(t, s, r.Sandbox.ID, newOp.ID)
			assertDataPathDead(t, path, held)
			if published != nil {
				assertDataPathDead(t, path, published)
			}
			release()
			if _, err = s.Wait(ctx, p, old.ID); err == nil {
				t.Error("superseded operation claimed successful delivery")
			}
			awaitAgent(t, newEntered)
			requireFence(t, s, r.Sandbox.ID, newOp.ID)
			requireDataReadyBlocked(t, s, p, r.Sandbox.ID)
			assertDataPathDead(t, path, held)
			newRelease()
			if _, err = s.Wait(ctx, p, newOp.ID); err != nil {
				t.Fatal(err)
			}
			requireFence(t, s, r.Sandbox.ID, "")
			if status, _ := proxyGet(
				t,
				path,
				"http://example.com/",
			); status != 403 {
				t.Fatalf("old policy reopened: %d", status)
			}
			if status, _ := proxyGet(
				t,
				path,
				"http://other.example/",
			); status != 200 {
				t.Fatalf("new policy not delivered: %d", status)
			}
			if _, err = s.Execute(ctx, p, "old", oldCommand); err == nil {
				t.Fatal("superseded operation terminal result changed")
			}
			after, err := s.Get(ctx, p, r.Sandbox.ID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Instance != r.Sandbox.Instance ||
				after.Epoch != r.Sandbox.Epoch ||
				!after.Deadline.Equal(r.Sandbox.Deadline) {
				t.Fatal("supersession changed lease or identity")
			}
		})
	}
}

func TestConfigureEgressStaleQueuedIntentCannotReopen(t *testing.T) {
	s, f, agent, p, manager := egressFixture(t)
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	narrow := egress.Policy{
		Mode:         "relay",
		AllowedHosts: []string{"other.example"},
	}
	r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
	path := proxyHTTPPath(t, s, f, r.Sandbox.ID)
	// A worker that cannot claim its operation leaves a durable queued intent.
	// Retry it only after a newer actual configure has completed delivery.
	if _, err := s.store.db.Exec(
		`CREATE TRIGGER hold_queued BEFORE UPDATE ON operations WHEN NEW.status='running'
		BEGIN SELECT RAISE(ABORT,'queued worker unavailable'); END`,
	); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	staleCommand := Command{
		Action:    "configure",
		SandboxID: r.Sandbox.ID,
		Env:       map[string]EnvVar{},
		Policy:    wide,
	}
	stale, err := s.Submit(ctx, p, "stale", staleCommand)
	if err != nil {
		t.Fatal(err)
	}
	awaitWorker(t, s, stale.ID)
	queued, err := s.Operation(ctx, p, stale.ID)
	if err != nil || queued.Status != "pending" {
		t.Fatalf("intent was not queued: %s %v", queued.Status, err)
	}
	if _, err = s.store.db.Exec("DROP TRIGGER hold_queued"); err != nil {
		t.Fatal(err)
	}
	winnerEntered, winnerRelease := gateAgent(t, agent, "activate")
	winner, err := s.Submit(
		ctx,
		p,
		"winner",
		Command{
			Action:    "configure",
			SandboxID: r.Sandbox.ID,
			Env:       map[string]EnvVar{},
			Policy:    narrow,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	awaitAgent(t, winnerEntered)
	requireFence(t, s, r.Sandbox.ID, winner.ID)
	winnerRelease()
	if _, err = s.Wait(ctx, p, winner.ID); err != nil {
		t.Fatal(err)
	}
	awaitWorker(t, s, winner.ID)
	requireFence(t, s, r.Sandbox.ID, "")
	if _, err = s.Execute(ctx, p, "stale", staleCommand); err == nil {
		t.Error("stale queued intent succeeded without its own fence")
	}
	if status, _ := proxyGet(t, path, "http://example.com/"); status != 403 {
		t.Fatalf("stale queued policy reopened: %d", status)
	}
	if status, _ := proxyGet(t, path, "http://other.example/"); status != 200 {
		t.Fatalf("new effective policy lost: %d", status)
	}
	// A stale in-memory copy must not overwrite the coherent durable success.
	s.start(winner)
	awaitWorker(t, s, winner.ID)
	completed, err := s.Operation(ctx, p, winner.ID)
	if err != nil || completed.Status != "succeeded" {
		t.Fatalf(
			"fixed delivery result was overwritten: %s %v",
			completed.Status,
			err,
		)
	}
	restarted := restartEgress(t, s, manager)
	requireFence(t, restarted, r.Sandbox.ID, "")
	completed, err = restarted.Operation(ctx, p, winner.ID)
	if err != nil || completed.Status != "succeeded" {
		t.Fatalf(
			"restart lost successful delivery responsibility: %s %v",
			completed.Status,
			err,
		)
	}
	if _, err = restarted.DataReady(ctx, p, r.Sandbox.ID); err != nil {
		t.Fatal(err)
	}
	if status, _ := proxyGet(t, path, "http://example.com/"); status != 403 {
		t.Fatalf("restart reopened old policy: %d", status)
	}
	if status, _ := proxyGet(t, path, "http://other.example/"); status != 200 {
		t.Fatalf("restart lost delivered policy: %d", status)
	}
}

func TestConfigureEgressInfoFailureRevokesOldBinding(t *testing.T) {
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	for _, tc := range []struct {
		name   string
		env    map[string]EnvVar
		newEnv map[string]EnvVar
		policy egress.Policy
	}{
		{
			name:   "none",
			env:    map[string]EnvVar{},
			newEnv: map[string]EnvVar{},
			policy: egress.Policy{Mode: "none"},
		},
		{
			name:   "narrow_allowlist",
			env:    map[string]EnvVar{},
			newEnv: map[string]EnvVar{},
			policy: egress.Policy{
				Mode:         "relay",
				AllowedHosts: []string{"other.example"},
			},
		},
		{
			name:   "credential_rotation",
			env:    rotatedSecretEnv("rotated-unused-credential-one"),
			newEnv: rotatedSecretEnv("rotated-unused-credential-two"),
			policy: wide,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			r := claimRelay(t, s, p, "claim", tc.env, wide)
			httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
			if status, body := proxyGet(
				t,
				httpPath,
				"http://example.com/",
			); status != 200 || body != "upstream example.com" {
				t.Fatalf("relay never served: %d %q", status, body)
			}
			held := holdTunnel(t, httpPath, "example.com:443")
			agent.mu.Lock()
			agent.failInfo = true
			agent.mu.Unlock()
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			_, err := s.Execute(
				ctx,
				p,
				"configure-"+tc.name,
				Command{
					Action:    "configure",
					SandboxID: r.Sandbox.ID,
					Env:       tc.newEnv,
					Policy:    tc.policy,
				},
			)
			if err == nil {
				t.Fatal("configure succeeded despite the info failure")
			}
			b, err := s.Get(context.Background(), p, r.Sandbox.ID)
			if err != nil {
				t.Fatal(err)
			}
			if b.LastError == "" {
				t.Fatal("failed configure left no diagnostic")
			}
			for _, value := range []string{
				"rotated-unused-credential-one",
				"rotated-unused-credential-two",
			} {
				if strings.Contains(b.LastError, value) {
					t.Fatal("diagnostic leaked credential material")
				}
			}
			assertDataPathDead(t, httpPath, held)
		})
	}
}

func TestConfigureEgressFailureMatrix(t *testing.T) {
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	narrow := egress.Policy{
		Mode:         "relay",
		AllowedHosts: []string{"other.example"},
	}
	none := egress.Policy{Mode: "none"}
	for _, tc := range []struct {
		name   string
		policy egress.Policy
		inject func(f *fakeEngine, a *fakeAgent)
	}{
		{
			name:   "getvm",
			policy: none,
			inject: func(f *fakeEngine, a *fakeAgent) {
				f.mu.Lock()
				f.unavailable = true
				f.mu.Unlock()
			},
		},
		{
			name:   "network_off",
			policy: none,
			inject: func(f *fakeEngine, a *fakeAgent) {
				f.mu.Lock()
				f.failNetwork = true
				f.mu.Unlock()
			},
		},
		{
			name:   "info",
			policy: none,
			inject: func(f *fakeEngine, a *fakeAgent) {
				a.mu.Lock()
				a.failInfo = true
				a.mu.Unlock()
			},
		},
		{
			// The new binding exists when Activate fails; it must be revoked.
			name:   "activate",
			policy: narrow,
			inject: func(f *fakeEngine, a *fakeAgent) {
				a.mu.Lock()
				a.fail = true
				a.mu.Unlock()
			},
		},
		{
			// The relay manager rejects the new policy during Bind.
			name:   "bind",
			policy: egress.Policy{Mode: "relay", MaxConnections: 70000},
			inject: func(f *fakeEngine, a *fakeAgent) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, agent, p, _ := egressFixture(t)
			r := claimRelay(
				t,
				s,
				p,
				"claim",
				map[string]EnvVar{},
				wide,
			)
			httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
			if status, _ := proxyGet(
				t,
				httpPath,
				"http://example.com/",
			); status != 200 {
				t.Fatalf("relay never served: %d", status)
			}
			held := holdTunnel(t, httpPath, "example.com:443")
			tc.inject(f, agent)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()
			command := Command{
				Action:    "configure",
				SandboxID: r.Sandbox.ID,
				Env:       map[string]EnvVar{},
				Policy:    tc.policy,
			}
			res, err := s.Execute(ctx, p, "configure-"+tc.name, command)
			if err == nil {
				t.Fatal("configure succeeded despite the injected failure")
			}
			b, err := s.Get(context.Background(), p, r.Sandbox.ID)
			if err != nil {
				t.Fatal(err)
			}
			if b.LastError == "" || b.State != "running" {
				t.Fatalf(
					"failed configure lost its diagnostic: state=%s error=%q",
					b.State,
					b.LastError,
				)
			}
			requireFence(t, s, r.Sandbox.ID, res.OperationID)
			assertDataPathDead(t, httpPath, held)
			requireDataReadyBlocked(t, s, p, r.Sandbox.ID)
			// The accepted operation and its key keep their terminal result.
			again, err := s.Execute(
				ctx,
				p,
				"configure-"+tc.name,
				command,
			)
			if err == nil || again.OperationID != res.OperationID {
				t.Fatalf(
					"terminal configure replay changed: id=%s err=%v",
					again.OperationID,
					err,
				)
			}
		})
	}
}

func TestConfigureEgressRecoveryHonorsSavedPolicy(t *testing.T) {
	wide := egress.Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	narrow := egress.Policy{
		Mode:         "relay",
		AllowedHosts: []string{"other.example"},
	}
	none := egress.Policy{Mode: "none"}
	configure := func(
		t *testing.T,
		s *Service,
		p Principal,
		key, id string,
		policy egress.Policy,
	) (Result, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		return s.Execute(
			ctx,
			p,
			key,
			Command{
				Action:    "configure",
				SandboxID: id,
				Env:       map[string]EnvVar{},
				Policy:    policy,
			},
		)
	}
	// revokedCrashed models a crash after revocation succeeded but before the
	// new configuration was persisted: the database still holds the old wide
	// relay policy and the fence.
	revokedCrashed := func(t *testing.T, s *Service, id string) {
		t.Helper()
		if err := recordConfigFence(
			context.Background(),
			s.store.db,
			id,
			"op_crashed",
		); err != nil {
			t.Fatal(err)
		}
		s.closeConnections(id)
		if err := s.revoke(id); err != nil {
			t.Fatal(err)
		}
	}
	t.Run("revoked_before_save_stays_closed", func(t *testing.T) {
		s, f, _, p, newManager := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
		if status, _ := proxyGet(
			t,
			httpPath,
			"http://example.com/",
		); status != 200 {
			t.Fatalf("relay never served: %d", status)
		}
		revokedCrashed(t, s, r.Sandbox.ID)
		before, err := s.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if before.Policy.Mode != "relay" {
			t.Fatal("the old wide policy was not retained")
		}
		requireDataReadyBlocked(t, s, p, r.Sandbox.ID)
		s.reconcile()
		if conn, err := net.DialTimeout(
			"unix",
			httpPath,
			300*time.Millisecond,
		); err == nil {
			_ = conn.Close()
			t.Fatal("maintenance reopened the revoked proxy")
		}
		restarted := restartEgress(t, s, newManager)
		requireDataReadyBlocked(t, restarted, p, r.Sandbox.ID)
		restarted.reconcile()
		entries, err := os.ReadDir(f.sockDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("restart recreated a proxy listener behind the fence")
		}
		after, err := restarted.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !after.Deadline.Equal(before.Deadline) ||
			after.Instance != before.Instance ||
			after.Epoch != before.Epoch {
			t.Fatal("the fence changed the lease or identity")
		}
		// Only a new explicit configure delivers the saved intent.
		if _, err = configure(
			t,
			restarted,
			p,
			"configure-none",
			r.Sandbox.ID,
			none,
		); err != nil {
			t.Fatal(err)
		}
		requireFence(t, restarted, r.Sandbox.ID, "")
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		if _, err = restarted.DataReady(ctx, p, r.Sandbox.ID); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("restart_keeps_failed_configure_closed", func(t *testing.T) {
		s, f, agent, p, newManager := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
		before, err := s.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		agent.mu.Lock()
		agent.failInfo = true
		agent.mu.Unlock()
		res, err := configure(t, s, p, "configure-none", r.Sandbox.ID, none)
		if err == nil {
			t.Fatal("configure succeeded despite the info failure")
		}
		requireFence(t, s, r.Sandbox.ID, res.OperationID)
		restarted := restartEgress(t, s, newManager)
		requireFence(t, restarted, r.Sandbox.ID, res.OperationID)
		requireDataReadyBlocked(t, restarted, p, r.Sandbox.ID)
		restarted.reconcile()
		if _, err = os.Stat(httpPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("restart recreated the revoked listener: %v", err)
		}
		after, err := restarted.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if !after.Deadline.Equal(before.Deadline) ||
			after.Instance != before.Instance ||
			after.Epoch != before.Epoch {
			t.Fatal("recovery changed the lease or identity")
		}
		agent.mu.Lock()
		agent.failInfo = false
		agent.mu.Unlock()
		// A new key admits the replacement intent; it must deliver the new
		// narrow policy rather than reviving the old wide one.
		if _, err = configure(
			t,
			restarted,
			p,
			"configure-narrow",
			r.Sandbox.ID,
			narrow,
		); err != nil {
			t.Fatal(err)
		}
		requireFence(t, restarted, r.Sandbox.ID, "")
		if status, _ := proxyGet(
			t,
			httpPath,
			"http://example.com/",
		); status != 403 {
			t.Fatalf("old wide policy still served: %d", status)
		}
		if status, body := proxyGet(
			t,
			httpPath,
			"http://other.example/",
		); status != 200 || body != "upstream other.example" {
			t.Fatalf("narrow policy not delivered: %d %q", status, body)
		}
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		ready, err := restarted.DataReady(ctx, p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ready.Instance != before.Instance || ready.Epoch != before.Epoch {
			t.Fatal("delivery changed the sandbox identity")
		}
	})
	t.Run("old_operation_cannot_clear_new_fence", func(t *testing.T) {
		s, f, agent, p, _ := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		_ = f
		agent.mu.Lock()
		agent.failInfo = true
		agent.mu.Unlock()
		res, err := configure(t, s, p, "configure-old", r.Sandbox.ID, none)
		if err == nil {
			t.Fatal("configure succeeded despite the info failure")
		}
		requireFence(t, s, r.Sandbox.ID, res.OperationID)
		// A newer admission replaces the fence; the old operation's terminal
		// clear is rejected against it.
		if err = recordConfigFence(
			context.Background(),
			s.store.db,
			r.Sandbox.ID,
			"op_newer",
		); err != nil {
			t.Fatal(err)
		}
		if err = clearConfigFence(
			context.Background(),
			s.store.db,
			r.Sandbox.ID,
			res.OperationID,
		); !isCode(err, ErrConflict) {
			t.Fatalf("superseded clear was not rejected: %v", err)
		}
		requireFence(t, s, r.Sandbox.ID, "op_newer")
		if err = clearConfigFence(
			context.Background(),
			s.store.db,
			r.Sandbox.ID,
			"op_newer",
		); err != nil {
			t.Fatal(err)
		}
		requireFence(t, s, r.Sandbox.ID, "")
	})
	t.Run("paused_configure_does_not_wake", func(t *testing.T) {
		s, f, _, p, _ := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		if _, err := s.Execute(
			ctx,
			p,
			"pause",
			Command{Action: "pause", SandboxID: r.Sandbox.ID},
		); err != nil {
			t.Fatal(err)
		}
		clones := f.count("clone")
		if _, err := configure(
			t,
			s,
			p,
			"configure-none",
			r.Sandbox.ID,
			none,
		); err != nil {
			t.Fatal(err)
		}
		requireFence(t, s, r.Sandbox.ID, "")
		if f.count("clone") != clones {
			t.Fatal("paused configuration woke the sandbox")
		}
		b, err := s.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if b.State != "paused" || b.VMID != "" {
			t.Fatalf("paused configuration changed residency: %+v", b)
		}
		entries, err := os.ReadDir(f.sockDir)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatal("paused configuration created a proxy listener")
		}
		if _, err = s.Execute(
			ctx,
			p,
			"resume",
			Command{Action: "resume", SandboxID: r.Sandbox.ID},
		); err != nil {
			t.Fatal(err)
		}
		if entries, err = os.ReadDir(
			f.sockDir,
		); err != nil ||
			len(entries) != 0 {
			t.Fatalf(
				"none policy created listeners on resume: %v %v",
				entries,
				err,
			)
		}
		readyCtx, readyCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer readyCancel()
		ready, err := s.DataReady(readyCtx, p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ready.Instance == "" || ready.Policy.Mode != "none" {
			t.Fatalf("resume did not deliver the saved none policy: %+v", ready)
		}
	})
	t.Run("reconcile_revokes_none_residual_binding", func(t *testing.T) {
		s, f, _, p, _ := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
		if _, err := configure(
			t,
			s,
			p,
			"configure-none",
			r.Sandbox.ID,
			none,
		); err != nil {
			t.Fatal(err)
		}
		requireFence(t, s, r.Sandbox.ID, "")
		// A residual relay binding behind a none policy must not survive
		// maintenance.
		b, err := s.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		v, err := f.GetVM(context.Background(), b.VMID)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.opts.Egress.Revoke(b.ID); err != nil {
			t.Fatal(err)
		}
		if err = s.opts.Egress.Bind(
			context.Background(),
			b.ID,
			b.TenantID,
			v.VSockPath,
			wide,
			nil,
		); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		s.bindings[b.ID] = "residual"
		s.mu.Unlock()
		if status, _ := proxyGet(
			t,
			httpPath,
			"http://example.com/",
		); status != 200 {
			t.Fatalf("residual binding never served: %d", status)
		}
		s.reconcile()
		s.mu.Lock()
		bound := s.bindings[b.ID] != ""
		s.mu.Unlock()
		if bound {
			t.Fatal("maintenance kept a residual binding behind none policy")
		}
		if _, err = os.Stat(httpPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("residual listener survived maintenance: %v", err)
		}
	})
	t.Run("fingerprint_mismatch_rebinds", func(t *testing.T) {
		s, f, agent, p, _ := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
		held := holdTunnel(t, httpPath, "example.com:443")
		b := *r.Sandbox
		b.Policy = narrow
		if err := saveSandbox(context.Background(), s.store.db, b); err != nil {
			t.Fatal(err)
		}
		agent.mu.Lock()
		activations := len(agent.activations)
		agent.mu.Unlock()
		// Maintenance must compare the saved effective policy against a
		// nonempty binding; no explicit bind or activation repairs this test.
		s.reconcile()
		if status, _ := proxyGet(
			t,
			httpPath,
			"http://example.com/",
		); status != 403 {
			t.Fatalf("maintenance retained old wide authorization: %d", status)
		}
		if status, body := proxyGet(
			t,
			httpPath,
			"http://other.example/",
		); status != 200 ||
			body != "upstream other.example" {
			t.Fatalf("saved policy was not delivered: %d %q", status, body)
		}
		_ = held.SetReadDeadline(time.Now().Add(2 * time.Second))
		var one [1]byte
		if n, err := held.Read(one[:]); n != 0 || err == nil {
			t.Fatal("old tunnel survived maintenance policy replacement")
		}
		agent.mu.Lock()
		afterActivations := len(agent.activations)
		agent.mu.Unlock()
		if afterActivations != activations {
			t.Fatal("fingerprint repair replayed guest activation")
		}
		after, err := s.Get(context.Background(), p, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		if after.Instance != b.Instance || after.Epoch != b.Epoch ||
			!after.Deadline.Equal(b.Deadline) {
			t.Fatal("maintenance changed identity or lease")
		}
	})
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
		ErrConflict,
	) {
		t.Fatalf("restart served sandbox without credentials: %v", err)
	}
	_ = f
}
