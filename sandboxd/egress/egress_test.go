package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(
	c context.Context,
	n, h string,
) ([]netip.Addr, error) {
	return f(c, n, h)
}

func publicResolver() Resolver {
	return resolverFunc(
		func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		},
	)
}

func targetDialer(
	target string,
	count *atomic.Int32,
) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, n, addr string) (net.Conn, error) {
		h, _, err := net.SplitHostPort(addr)
		if err != nil || net.ParseIP(h) == nil {
			return nil, errors.New("dialer received a DNS name")
		}
		if count != nil {
			count.Add(1)
		}
		return (&net.Dialer{}).DialContext(ctx, n, target)
	}
}

func newTestManager(
	t *testing.T,
	opts Options,
	p Policy,
	secrets map[string]Secret,
) (*Manager, Binding) {
	t.Helper()
	dir, err := os.MkdirTemp("", "egress-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if opts.Resolver == nil {
		opts.Resolver = publicResolver()
	}
	if len(secrets) != 0 && opts.Audit == nil && opts.AuditRecord == nil {
		opts.Audit = func(AuditEvent) {}
	}
	m, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := m.Bind(
		context.Background(),
		"sandbox-1",
		"tenant-1",
		filepath.Join(dir, "vsock"),
		p,
		secrets,
	); err != nil {
		t.Fatal(err)
	}
	b, ok := m.Binding("sandbox-1")
	if !ok {
		t.Fatal("missing activation metadata")
	}
	return m, b
}

func proxyClient(t *testing.T, m *Manager, b Binding) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CACertPEM()) {
		t.Fatal("invalid CA PEM")
	}
	proxyURL, _ := url.Parse("http://proxy.invalid")
	tr := &http.Transport{
		Proxy: http.ProxyURL(proxyURL),
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", b.HTTPPath)
		},
		TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		},
		DisableKeepAlives: true,
	}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{
		Transport:     tr,
		Timeout:       5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func readResponse(t *testing.T, resp *http.Response, err error) string {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func get(t *testing.T, c *http.Client, u string) (*http.Response, string) {
	t.Helper()
	r, err := c.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	return r, readResponse(t, r, nil)
}

func TestHTTPProxyAndHostValidation(t *testing.T) {
	up := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Proxy-Authorization") != "" {
				t.Error("proxy authorization escaped")
			}
			_, _ = io.WriteString(w, "upstream "+r.Host)
		}),
	)
	defer up.Close()
	var dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), &dials)},
		Policy{Mode: "relay", AllowedHosts: []string{"example.com"}},
		nil,
	)
	r, body := get(t, proxyClient(t, m, b), "http://example.com/hello")
	if r.StatusCode != 200 || body != "upstream example.com" ||
		dials.Load() != 1 {
		t.Fatalf("status=%d body=%q dials=%d", r.StatusCode, body, dials.Load())
	}
	conn, err := net.Dial("unix", b.HTTPPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(
		conn,
		"GET http://example.com/ HTTP/1.1\r\nHost: attacker.example.com\r\nConnection: close\r\n\r\n",
	)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	// net/http uses the absolute request target as Host (RFC 9112). The
	// conflicting wire Host must never influence the upstream authority.
	body = readResponse(t, resp, nil)
	if resp.StatusCode != 200 || body != "upstream example.com" ||
		dials.Load() != 2 {
		t.Fatalf("upstream authority changed: %d %q", resp.StatusCode, body)
	}
}

func TestHTTPSSecretInjectionAndNonLeak(t *testing.T) {
	const real = "Bearer host-only-real-secret"
	var observed atomic.Int32
	up := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != real ||
				r.Header.Get("X-API-Key") != "second-host-only-secret" {
				t.Error("missing injected credentials")
			}
			observed.Add(1)
			w.Header().Set("X-Reflection", real)
			w.Header().Set("Trailer", "X-Trailer-Reflection")
			_, _ = io.WriteString(w, "prefix "+real+" suffix")
			w.Header().Set("X-Trailer-Reflection", real)
		}),
	)
	defer up.Close()
	pool := x509.NewCertPool()
	pool.AddCert(up.Certificate())
	var mu sync.Mutex
	var events []AuditEvent
	m, b := newTestManager(
		t,
		Options{
			RootCAs:     pool,
			DialContext: targetDialer(up.Listener.Addr().String(), nil),
			Audit:       func(e AuditEvent) { mu.Lock(); events = append(events, e); mu.Unlock() },
		},
		Policy{Mode: "relay"},
		map[string]Secret{
			"API_TOKEN": {
				Value:       real,
				Hosts:       []string{"example.com"},
				Header:      "Authorization",
				Placeholder: "Bearer phantom",
			},
			"API_KEY": {
				Value:  "second-host-only-secret",
				Hosts:  []string{"example.com"},
				Header: "X-API-Key",
			},
		},
	)
	client := proxyClient(t, m, b)
	req, _ := http.NewRequest("GET", "https://example.com/check", nil)
	req.Header.Set("Authorization", "Bearer phantom")
	resp, err := client.Do(req)
	body := readResponse(t, resp, err)
	if resp.StatusCode != 200 || observed.Load() != 1 ||
		!strings.Contains(body, "[redacted]") ||
		strings.Contains(body, real) ||
		strings.Contains(resp.Header.Get("X-Reflection"), real) ||
		strings.Contains(resp.Trailer.Get("X-Trailer-Reflection"), real) {
		t.Fatalf(
			"unexpected response: %d %q %#v",
			resp.StatusCode,
			body,
			resp.Header,
		)
	}
	mu.Lock()
	data, _ := json.Marshal(events)
	mu.Unlock()
	if strings.Contains(string(data), real) ||
		strings.Contains(string(data), "second-host-only-secret") ||
		strings.Contains(string(data), "API_TOKEN") {
		t.Fatalf("audit leaked a secret: %s", data)
	}
	if !strings.Contains(string(data), `"action":"inject"`) ||
		!strings.Contains(string(data), `"tenant_id":"tenant-1"`) {
		t.Fatalf("injection was not audited: %s", data)
	}
	for _, u := range []string{"https://attacker.example.com/", "http://example.com/", "http://attacker.example.com/"} {
		req, _ := http.NewRequest("GET", u, nil)
		req.Header.Set("Authorization", "Bearer phantom")
		resp, err := client.Do(req)
		body := readResponse(t, resp, err)
		if resp.StatusCode != 403 || strings.Contains(body, real) {
			t.Fatalf("placeholder escaped: %s %d %q", u, resp.StatusCode, body)
		}
	}
	if observed.Load() != 1 {
		t.Fatal("unauthorized requests reached upstream")
	}
}

func connectTunnel(t *testing.T, path, host string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprintf(
		conn,
		"CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n",
		host,
		host,
	)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: "CONNECT"})
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		_ = conn.Close()
		t.Fatalf("CONNECT status: %d", resp.StatusCode)
	}
	if reader.Buffered() != 0 {
		t.Fatal("unexpected CONNECT response bytes")
	}
	return conn
}

func TestCONNECTRejectsPlaintextSNIAndInnerHostMismatch(t *testing.T) {
	var dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("should not dial")
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	conn := connectTunnel(t, b.HTTPPath, "example.com:443")
	_, _ = io.WriteString(conn, "G")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	_ = conn.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("plaintext accepted: %d", resp.StatusCode)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(m.CACertPEM())
	for _, serverName := range []string{"attacker.example.com", "93.184.216.34"} {
		t.Run("sni_"+serverName, func(t *testing.T) {
			// Numeric ServerName omits SNI. A remote alert proves that the proxy
			// rejected the ClientHello before client certificate verification.
			conn := connectTunnel(t, b.HTTPPath, "example.com:443")
			badTLS := tls.Client(conn, &tls.Config{
				ServerName: serverName,
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
			})
			defer badTLS.Close()
			var remote *net.OpError
			if err := badTLS.Handshake(); !errors.As(err, &remote) ||
				remote.Op != "remote error" {
				t.Error(
					"proxy did not reject missing or mismatched SNI with a TLS alert",
				)
			}
		})
	}
	for _, tc := range []struct {
		name    string
		request string
	}{
		{"host", "GET / HTTP/1.1\r\nHost: attacker.example.com\r\n\r\n"},
		{"port", "GET / HTTP/1.1\r\nHost: example.com:444\r\n\r\n"},
		{"scheme", "GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n"},
		{"nested_connect", "CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n"},
		{"upgrade", "GET / HTTP/1.1\r\nHost: example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"},
		{"forged_tunnel_header", "GET / HTTP/1.1\r\nHost: attacker.example.com\r\nX-Egress-Tunnel: example.com:443\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			goodTLS := mitmClient(t, m, b)
			_, _ = io.WriteString(goodTLS, tc.request)
			resp, err := http.ReadResponse(bufio.NewReader(goodTLS), nil)
			if err != nil {
				t.Fatal(err)
			}
			body := readResponse(t, resp, nil)
			if resp.StatusCode != http.StatusForbidden ||
				body != "egress: request denied\n" {
				t.Errorf(
					"inner authority/protocol rejection: status=%d",
					resp.StatusCode,
				)
			}
		})
	}
	conn, err = net.Dial("unix", b.HTTPPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(
		conn,
		"GET https://example.com/ HTTP/1.1\r\nHost: example.com\r\nX-Egress-Tunnel: example.com:443\r\nConnection: close\r\n\r\n",
	)
	resp, err = http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = readResponse(t, resp, nil)
	if resp.StatusCode != http.StatusForbidden || dials.Load() != 0 {
		t.Fatal("untrusted tunnel authority or protocol reached upstream")
	}
}

func TestGuardRejectsSpecialAndMixedAnswers(t *testing.T) {
	blocked := []string{
		"0.0.0.0",
		"10.1.2.3",
		"100.64.1.1",
		"100.100.100.200",
		"127.0.0.1",
		"169.254.169.254",
		"172.16.0.1",
		"192.168.1.1",
		"192.0.0.8",
		"192.0.2.1",
		"198.18.0.1",
		"198.51.100.1",
		"203.0.113.1",
		"224.0.0.1",
		"240.0.0.1",
		"255.255.255.255",
		"168.63.129.16",
		"::",
		"::1",
		"::ffff:127.0.0.1",
		"::ffff:10.0.0.1",
		"64:ff9b::808:808",
		"64:ff9b:1::a00:1",
		"2001::1",
		"2001:db8::1",
		"2002:808:808::1",
		"3fff::1",
		"fc00::1",
		"fe80::1",
		"ff02::1",
	}
	p, err := compilePolicy(Policy{Mode: "relay"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range blocked {
		if p.checkIP(netip.MustParseAddr(raw)) == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{"8.8.8.8", "::ffff:8.8.8.8", "2606:4700:4700::1111"} {
		if p.checkIP(netip.MustParseAddr(raw)) != nil {
			t.Errorf("blocked public address %s", raw)
		}
	}
	var dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{
			Resolver: resolverFunc(
				func(context.Context, string, string) ([]netip.Addr, error) {
					return []netip.Addr{
						netip.MustParseAddr("8.8.8.8"),
						netip.MustParseAddr("127.0.0.1"),
					}, nil
				},
			),
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected")
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	resp, _ := get(t, proxyClient(t, m, b), "http://example.com/")
	if resp.StatusCode != 403 || dials.Load() != 0 {
		t.Fatal("mixed DNS answer allowed a dial")
	}
}

func TestDNSRebindingAndUpstreamTLSVerification(t *testing.T) {
	var lookups, dials atomic.Int32
	m, b := newTestManager(
		t,
		Options{
			Resolver: resolverFunc(
				func(context.Context, string, string) ([]netip.Addr, error) {
					if lookups.Add(1) == 1 {
						return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
					}
					return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
				},
			),
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, errors.New("unexpected")
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	r, _ := get(t, proxyClient(t, m, b), "http://example.com/")
	if r.StatusCode != 502 || dials.Load() != 0 {
		t.Fatalf(
			"rebinding was not denied: %d dials=%d",
			r.StatusCode,
			dials.Load(),
		)
	}
	var reached atomic.Int32
	up := httptest.NewTLSServer(
		http.HandlerFunc(
			func(http.ResponseWriter, *http.Request) { reached.Add(1) },
		),
	)
	defer up.Close()
	m2, b2 := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), nil)},
		Policy{Mode: "relay"},
		map[string]Secret{
			"key": {
				Value:  "host-secret",
				Hosts:  []string{"example.com"},
				Header: "X-Key",
			},
		},
	)
	r, _ = get(t, proxyClient(t, m2, b2), "https://example.com/")
	if r.StatusCode != 502 || reached.Load() != 0 {
		t.Fatal("untrusted upstream received credential")
	}
}

func socksConnect(
	t *testing.T,
	path, host string,
	command byte,
) (net.Conn, byte) {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = conn.Write([]byte{5, 1, 0})
	var auth [2]byte
	if _, err := io.ReadFull(conn, auth[:]); err != nil {
		t.Fatal(err)
	}
	if auth != [2]byte{5, 0} {
		t.Fatalf("unexpected auth: %v", auth)
	}
	req := append([]byte{5, command, 0, 3, byte(len(host))}, []byte(host)...)
	req = append(req, 0, 80)
	_, _ = conn.Write(req)
	var reply [4]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatal(err)
	}
	var remainder int
	switch reply[3] {
	case 1:
		remainder = 6
	case 4:
		remainder = 18
	default:
		t.Fatalf("unexpected address type %d", reply[3])
	}
	if _, err := io.CopyN(io.Discard, conn, int64(remainder)); err != nil {
		t.Fatal(err)
	}
	return conn, reply[1]
}

func TestSOCKS5TCPAndGuards(t *testing.T) {
	up := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "socks upstream") },
		),
	)
	defer up.Close()
	var dials atomic.Int32
	_, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), &dials)},
		Policy{Mode: "relay", AllowedHosts: []string{"example.com"}},
		nil,
	)
	conn, status := socksConnect(t, b.SOCKSPath, "example.com", 1)
	if status != 0 {
		t.Fatalf("SOCKS status: %d", status)
	}
	_, _ = io.WriteString(
		conn,
		"GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n",
	)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if body := readResponse(t, resp, err); body != "socks upstream" {
		t.Fatal(body)
	}
	_ = conn.Close()
	for _, tc := range []struct {
		host    string
		command byte
	}{{"127.0.0.1", 1}, {"attacker.example.com", 1}, {"example.com", 3}, {"example.com", 2}} {
		conn, status := socksConnect(t, b.SOCKSPath, tc.host, tc.command)
		_ = conn.Close()
		if status == 0 {
			t.Errorf("SOCKS accepted %+v", tc)
		}
	}
	if dials.Load() != 1 {
		t.Fatal("guarded SOCKS made an unexpected dial")
	}
	_, b2 := newTestManager(
		t,
		Options{},
		Policy{Mode: "relay"},
		map[string]Secret{
			"key": {
				Value:  "host-secret",
				Hosts:  []string{"example.com"},
				Header: "Authorization",
			},
		},
	)
	conn, status = socksConnect(t, b2.SOCKSPath, "example.com", 1)
	_ = conn.Close()
	if status == 0 {
		t.Fatal("raw SOCKS bypassed credential checks")
	}
}

func TestConnectionBudgetAndRevoke(t *testing.T) {
	up := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "ok") },
		),
	)
	defer up.Close()
	m, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Listener.Addr().String(), nil)},
		Policy{Mode: "relay", MaxConnections: 1},
		nil,
	)
	conn, status := socksConnect(t, b.SOCKSPath, "example.com", 1)
	if status != 0 {
		t.Fatal(status)
	}
	defer conn.Close()
	extra, err := net.Dial("unix", b.HTTPPath)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetDeadline(time.Now().Add(time.Second))
	_, _ = io.WriteString(
		extra,
		"GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n",
	)
	var one [1]byte
	if n, err := extra.Read(one[:]); n != 0 || err == nil {
		t.Fatal("shared connection budget exceeded")
	}
	if err := m.Revoke("sandbox-1"); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := conn.Read(one[:]); n != 0 || err == nil {
		t.Fatal("active SOCKS connection survived revoke")
	}
	for _, path := range []string{b.HTTPPath, b.SOCKSPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("socket survived revoke: %s %v", path, err)
		}
	}
	if err := m.Revoke("sandbox-1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Binding("sandbox-1"); ok {
		t.Fatal("revoked binding retained")
	}
}

func TestRevokeClosesHijackedCONNECT(t *testing.T) {
	m, b := newTestManager(t, Options{}, Policy{Mode: "relay"}, nil)
	conn := connectTunnel(t, b.HTTPPath, "example.com:443")
	defer conn.Close()
	if err := m.Revoke("sandbox-1"); err != nil {
		t.Fatal(err)
	}
	var one [1]byte
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := conn.Read(one[:]); n != 0 || err == nil {
		t.Fatal("hijacked CONNECT survived revoke")
	}
}

func TestModesActivationAndExistingPaths(t *testing.T) {
	m, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, mode := range []string{"", "unknown", "direct"} {
		if err := m.Bind(
			context.Background(),
			mode+"id",
			"t",
			"/tmp/not-used",
			Policy{Mode: mode},
			nil,
		); err == nil {
			t.Errorf("accepted mode %q", mode)
		}
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "vsock")
	if err := m.Bind(
		context.Background(),
		"none",
		"t",
		base,
		Policy{Mode: "none"},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + "_2049"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("none mode created a listener")
	}
	if err := os.WriteFile(
		base+"_2050",
		[]byte("owner data"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(
		context.Background(),
		"relay",
		"t",
		base,
		Policy{Mode: "relay"},
		nil,
	); err == nil {
		t.Fatal("overwrote existing path")
	}
	if _, err := os.Stat(base + "_2049"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial bind left socket")
	}
	data, _ := os.ReadFile(base + "_2050")
	if string(data) != "owner data" {
		t.Fatal("existing path changed")
	}
	var installed, removed atomic.Bool
	direct, err := New(
		Options{
			InstallDirect: func(context.Context, Binding, Policy) (func() error, error) {
				installed.Store(true)
				return func() error { removed.Store(true); return nil }, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	if err := direct.Bind(
		context.Background(),
		"direct",
		"t",
		base,
		Policy{Mode: "direct"},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if !installed.Load() {
		t.Fatal("direct activated before installation")
	}
	if err := direct.Revoke("direct"); err != nil || !removed.Load() {
		t.Fatal("direct teardown missing")
	}
}

func TestPrivateCIDRExemptionsAndSecretFormatting(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		p, err := compilePolicy(
			Policy{
				Mode: "relay",
				AllowedCIDRs: []string{
					"10.2.0.0/16",
					"169.254.0.0/16",
					"64:ff9b::/96",
				},
			},
			enabled,
		)
		if err != nil {
			t.Fatal(err)
		}
		if allowed := p.checkIP(
			netip.MustParseAddr("10.2.3.4"),
		) == nil; allowed != enabled {
			t.Fatal("private CIDR opt-in ignored")
		}
		for _, raw := range []string{"10.3.3.4", "169.254.169.254", "64:ff9b::808:808"} {
			if p.checkIP(netip.MustParseAddr(raw)) == nil {
				t.Fatalf("exempted %s", raw)
			}
		}
	}
	s := Secret{
		Value:  "do-not-persist",
		Hosts:  []string{"example.com"},
		Header: "Authorization",
	}
	data, _ := json.Marshal(s)
	for _, out := range []string{string(data), fmt.Sprintf("%v", s), fmt.Sprintf("%+v", s), fmt.Sprintf("%#v", s)} {
		if strings.Contains(out, s.Value) {
			t.Fatalf("credential serialization leaked: %s", out)
		}
	}
}

func TestRedactionAcrossReadBoundaries(t *testing.T) {
	body := &redactingBody{
		source: io.NopCloser(
			&oneByteReader{
				data: []byte("prefix-host-secret-suffix-host-secret"),
			},
		),
		values: []string{"host-secret"},
	}
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "prefix-[redacted]-suffix-[redacted]" {
		t.Fatal(string(data))
	}
}

type oneByteReader struct{ data []byte }

func (r *oneByteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	p[0] = r.data[0]
	r.data = r.data[1:]
	return 1, nil
}
