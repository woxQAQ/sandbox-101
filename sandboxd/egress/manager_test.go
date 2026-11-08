package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
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

func TestPolicyIsCopiedAndBindContextIsInstallationOnly(t *testing.T) {
	up := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") },
		),
	)
	defer up.Close()
	m, err := New(
		Options{
			Resolver:    publicResolver(),
			DialContext: targetDialer(up.Listener.Addr().String(), nil),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	dir, err := os.MkdirTemp("", "egress-copy-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	p := Policy{Mode: "relay", AllowedHosts: []string{"example.com"}}
	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Bind(
		ctx,
		"s",
		"t",
		filepath.Join(dir, "vsock"),
		p,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	cancel()
	p.AllowedHosts[0] = "attacker.example.com"
	b, _ := m.Binding("s")
	resp, _ := get(t, proxyClient(t, m, b), "http://example.com/")
	if resp.StatusCode != 200 {
		t.Fatal("binding changed after caller mutation or context cancellation")
	}
	if err := m.Bind(
		context.Background(),
		"s",
		"t",
		filepath.Join(dir, "vsock"),
		p,
		nil,
	); err == nil {
		t.Fatal("duplicate binding was silently accepted")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.Bind(
		context.Background(),
		"s2",
		"t",
		filepath.Join(dir, "vsock"),
		p,
		nil,
	); !errors.Is(
		err,
		ErrClosed,
	) {
		t.Fatalf("closed manager accepted binding: %v", err)
	}
}

func mitmClient(t *testing.T, m *Manager, b Binding) *tls.Conn {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CACertPEM()) {
		t.Fatal("invalid proxy CA")
	}
	conn := tls.Client(
		connectTunnel(t, b.HTTPPath, "example.com:443"),
		&tls.Config{
			ServerName: "example.com",
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		},
	)
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Handshake(); err != nil {
		t.Fatal(err)
	}
	return conn
}
