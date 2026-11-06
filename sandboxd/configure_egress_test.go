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
	"strings"
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

func requireDataReadyBlocked(t *testing.T, s *Service, p Principal, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := s.DataReady(ctx, p, id); !isCode(err, api.Conflict) {
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
		// clear is a no-op against it.
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
		); err != nil {
			t.Fatal(err)
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
		s, f, _, p, _ := egressFixture(t)
		r := claimRelay(t, s, p, "claim", map[string]EnvVar{}, wide)
		httpPath := proxyHTTPPath(t, s, f, r.Sandbox.ID)
		s.mu.Lock()
		s.bindings[r.Sandbox.ID] = "corrupted"
		s.mu.Unlock()
		b, err := s.Get(context.Background(), p, r.Sandbox.ID)
		if err != nil {
			t.Fatal(err)
		}
		v, err := f.GetVM(context.Background(), b.VMID)
		if err != nil {
			t.Fatal(err)
		}
		// A fingerprint mismatch revokes and replaces the binding rather than
		// trusting the stale record.
		if err = s.bind(context.Background(), b, v.VSockPath); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		fingerprint := s.bindings[r.Sandbox.ID]
		s.mu.Unlock()
		if fingerprint == "" || fingerprint == "corrupted" {
			t.Fatal("stale binding fingerprint was trusted")
		}
		if status, body := proxyGet(
			t,
			httpPath,
			"http://example.com/",
		); status != 200 || body != "upstream example.com" {
			t.Fatalf("rebind did not restore service: %d %q", status, body)
		}
	})
}
