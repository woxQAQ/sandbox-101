package egress

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testMITMHeaderBytes = 1 << 20

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

func mitmBoundsFixture(
	t *testing.T,
) (*Manager, Binding, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	up := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	)
	t.Cleanup(up.Close)
	pool := x509.NewCertPool()
	pool.AddCert(up.Certificate())
	var dials, injections atomic.Int32
	m, b := newTestManager(t, Options{
		RootCAs:     pool,
		IdleTimeout: 2 * time.Second,
		DialContext: targetDialer(up.Listener.Addr().String(), &dials),
		Audit: func(e AuditEvent) {
			if e.Action == "inject" {
				injections.Add(1)
			}
		},
	}, Policy{Mode: "relay"}, map[string]Secret{
		"TOKEN": {
			Value:  "host-only-test-value",
			Hosts:  []string{"example.com"},
			Header: "Authorization",
		},
	})
	return m, b, &dials, &injections
}

func expectMITMHeaderRejection(t *testing.T, r *bufio.Reader) {
	t.Helper()
	resp, err := http.ReadResponse(r, nil)
	if err != nil {
		var timeout net.Error
		if errors.As(err, &timeout) && timeout.Timeout() {
			t.Error("header parser did not reject before client deadline")
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Errorf("oversized header accepted: status=%d", resp.StatusCode)
	}
}

func TestMITMHeaderBounds(t *testing.T) {
	const oversized = testMITMHeaderBytes + 16*1024
	for _, kind := range []string{"request_line", "single_header", "aggregate_headers"} {
		t.Run(kind, func(t *testing.T) {
			m, b, dials, injections := mitmBoundsFixture(t)
			conn := mitmClient(t, m, b)
			var request string
			switch kind {
			case "request_line":
				request = "GET /" + strings.Repeat(
					"x",
					oversized,
				) + " HTTP/1.1\r\nHost: example.com\r\n\r\n"
			case "single_header":
				request = "GET / HTTP/1.1\r\nHost: example.com\r\nX-Padding: " + strings.Repeat(
					"x",
					oversized,
				) + "\r\n\r\n"
			case "aggregate_headers":
				line := "X-Padding: " + strings.Repeat("x", 1000) + "\r\n"
				request = "GET / HTTP/1.1\r\nHost: example.com\r\n" + strings.Repeat(
					line,
					oversized/len(line)+1,
				) + "\r\n"
			}
			_, _ = io.WriteString(conn, request)
			expectMITMHeaderRejection(t, bufio.NewReader(conn))
			if dials.Load() != 0 || injections.Load() != 0 {
				t.Errorf(
					"header rejection happened after upstream work: dials=%d injections=%d",
					dials.Load(),
					injections.Load(),
				)
			}
		})
	}
}

func TestMITMHeaderCompletionDeadline(t *testing.T) {
	m, b, dials, injections := mitmBoundsFixture(t)
	conn := mitmClient(t, m, b)
	started := time.Now()
	_ = conn.SetDeadline(started.Add(14 * time.Second))
	if _, err := io.WriteString(
		conn,
		"GET / HTTP/1.1\r\nHost: example.com\r\nX-Slow: ",
	); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if resp != nil {
			_ = resp.Body.Close()
		}
		readDone <- err
	}()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	limit := time.NewTimer(12 * time.Second)
	defer limit.Stop()
	defer func() {
		if dials.Load() != 0 || injections.Load() != 0 {
			t.Errorf(
				"incomplete headers reached upstream: dials=%d injections=%d",
				dials.Load(),
				injections.Load(),
			)
		}
	}()
	for {
		select {
		case err := <-readDone:
			if time.Since(started) < 9*time.Second {
				t.Error(
					"active header input was closed before the configured completion deadline",
				)
			}
			var timeout net.Error
			if err == nil || (errors.As(err, &timeout) && timeout.Timeout()) {
				t.Error(
					"incomplete headers were not closed by the header deadline",
				)
			}
			return
		case <-ticker.C:
			_, _ = io.WriteString(conn, "x")
		case <-limit.C:
			t.Error(
				"continuous header input extended the 10s completion deadline",
			)
			_ = conn.Close()
			<-readDone
			return
		}
	}
}

func TestMITMStreamingBodyAndReuse(t *testing.T) {
	const chunks = 48
	chunk := strings.Repeat("b", 32*1024)
	bodySize := chunks * len(chunk)
	wantHash := sha256.Sum256([]byte(strings.Repeat(chunk, chunks)))
	firstBody := make(chan struct{})
	var requests atomic.Int32
	up := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if r.Method == http.MethodPost {
				hash := sha256.New()
				first := make([]byte, len(chunk))
				if _, err := io.ReadFull(r.Body, first); err != nil {
					t.Error("upstream did not receive the first body chunk")
					return
				}
				_, _ = hash.Write(first)
				close(firstBody)
				n, err := io.Copy(hash, r.Body)
				if err != nil || n+int64(len(first)) != int64(bodySize) ||
					!bytes.Equal(hash.Sum(nil), wantHash[:]) {
					t.Error("large streaming body was truncated or changed")
					return
				}
			}
			_, _ = io.WriteString(w, "ok")
		}),
	)
	t.Cleanup(up.Close)
	pool := x509.NewCertPool()
	pool.AddCert(up.Certificate())
	var dials atomic.Int32
	m, b := newTestManager(t, Options{
		RootCAs:     pool,
		IdleTimeout: 30 * time.Second,
		DialContext: targetDialer(up.Listener.Addr().String(), &dials),
	}, Policy{Mode: "relay", MaxConnections: 1}, nil)
	conn := mitmClient(t, m, b)
	_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
	if _, err := fmt.Fprintf(
		conn,
		"POST /upload HTTP/1.1\r\nHost: example.com\r\nContent-Length: %d\r\n\r\n%s",
		bodySize,
		chunk,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstBody:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy buffered the body instead of streaming it upstream")
	}
	// Body I/O stays active below the idle timeout, but takes longer than the
	// header deadline. The first upstream chunk arrives before we send the rest.
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for i := 1; i < chunks; i++ {
		<-ticker.C
		if _, err := io.WriteString(conn, chunk); err != nil {
			t.Fatal("slow body was interrupted")
		}
	}
	reader := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		if i != 0 {
			if _, err := io.WriteString(
				conn,
				"GET /reuse HTTP/1.1\r\nHost: example.com\r\n\r\n",
			); err != nil {
				t.Fatal(err)
			}
		}
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body := readResponse(t, resp, nil)
		if resp.StatusCode != http.StatusOK || body != "ok" || resp.Close {
			t.Fatalf(
				"streaming/reuse response: status=%d close=%t",
				resp.StatusCode,
				resp.Close,
			)
		}
	}
	_, _ = io.WriteString(
		conn,
		"GET / HTTP/1.1\r\nHost: example.com\r\nX-Padding: "+strings.Repeat(
			"x",
			testMITMHeaderBytes+16*1024,
		)+"\r\n\r\n",
	)
	expectMITMHeaderRejection(t, reader)
	if requests.Load() != 2 || dials.Load() != 2 {
		t.Errorf(
			"reuse or per-request header boundary failed: requests=%d dials=%d",
			requests.Load(),
			dials.Load(),
		)
	}
	// The same boundary also applies to the second request in a fresh tunnel.
	_ = conn.Close()
	waitMITMInboundClosed(t, m.bindings[b.SandboxID])
	second := mitmClient(t, m, b)
	if _, err := io.WriteString(
		second,
		"GET /first HTTP/1.1\r\nHost: example.com\r\n\r\n",
	); err != nil {
		t.Fatal(err)
	}
	reader = bufio.NewReader(second)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = readResponse(t, resp, nil)
	_, _ = io.WriteString(
		second,
		"GET / HTTP/1.1\r\nHost: example.com\r\nX-Padding: "+strings.Repeat(
			"x",
			testMITMHeaderBytes+16*1024,
		)+"\r\n\r\n",
	)
	expectMITMHeaderRejection(t, reader)
	if requests.Load() != 3 || dials.Load() != 3 {
		t.Error("oversized second request reached upstream")
	}
}

// Wait on the binding's connection accounting, rather than timing goroutine
// counts. Revoke subsequently waits for the registered tunnel worker as well.
func waitMITMInboundClosed(t *testing.T, b *binding) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	limit := time.NewTimer(2 * time.Second)
	defer limit.Stop()
	for {
		b.mu.Lock()
		inbound := b.inbound
		b.mu.Unlock()
		if inbound == 0 {
			return
		}
		select {
		case <-ticker.C:
		case <-limit.C:
			t.Fatal("inner connection remained tracked after peer shutdown")
		}
	}
}

func TestMITMRevokeClosesParser(t *testing.T) {
	for _, stage := range []string{"header", "body", "idle", "handshake", "half_close_read", "half_close_write", "upstream_upgrade"} {
		for _, operation := range []string{"revoke", "close"} {
			t.Run(stage+"/"+operation, func(t *testing.T) {
				bodyStarted := make(chan struct{})
				up := httptest.NewTLSServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							if stage == "upstream_upgrade" {
								w.Header().Set("Connection", "Upgrade")
								w.Header().Set("Upgrade", "websocket")
								w.WriteHeader(http.StatusSwitchingProtocols)
								return
							}
							if r.Method == http.MethodPost {
								var first [1]byte
								if _, err := io.ReadFull(
									r.Body,
									first[:],
								); err == nil {
									close(bodyStarted)
								}
								_, _ = io.Copy(io.Discard, r.Body)
							}
							_, _ = io.WriteString(w, "ok")
						},
					),
				)
				t.Cleanup(up.Close)
				pool := x509.NewCertPool()
				pool.AddCert(up.Certificate())
				m, b := newTestManager(t, Options{
					RootCAs:     pool,
					DialContext: targetDialer(up.Listener.Addr().String(), nil),
				}, Policy{Mode: "relay", MaxConnections: 1}, nil)
				active := m.bindings[b.SandboxID]
				var conn net.Conn
				if stage == "handshake" {
					conn = connectTunnel(t, b.HTTPPath, "example.com:443")
					defer conn.Close()
					_, _ = conn.Write([]byte{0x16})
				} else {
					secure := mitmClient(t, m, b)
					conn = secure
					raw := secure.NetConn().(*net.UnixConn)
					switch stage {
					case "header", "half_close_write":
						_, _ = io.WriteString(
							conn,
							"GET / HTTP/1.1\r\nHost: example.com\r\nX-Pending: ",
						)
						if stage == "half_close_write" {
							if err := raw.CloseWrite(); err != nil {
								t.Fatal(err)
							}
							waitMITMInboundClosed(t, active)
						}
					case "body":
						_, _ = io.WriteString(
							conn,
							"POST / HTTP/1.1\r\nHost: example.com\r\nContent-Length: 100\r\n\r\nx",
						)
						select {
						case <-bodyStarted:
						case <-time.After(2 * time.Second):
							t.Fatal("upstream did not start reading the body")
						}
					case "idle":
						_, _ = io.WriteString(
							conn,
							"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
						)
						resp, err := http.ReadResponse(
							bufio.NewReader(conn),
							nil,
						)
						_ = readResponse(t, resp, err)
					case "upstream_upgrade":
						_, _ = io.WriteString(
							conn,
							"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
						)
						resp, err := http.ReadResponse(
							bufio.NewReader(conn),
							nil,
						)
						body := readResponse(t, resp, err)
						if resp.StatusCode != http.StatusBadGateway ||
							body != "egress: upstream unavailable\n" {
							t.Error(
								"unsolicited upstream Upgrade escaped the response hook",
							)
						}
					case "half_close_read":
						if err := raw.CloseRead(); err != nil {
							t.Fatal(err)
						}
						_, _ = io.WriteString(
							conn,
							"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n",
						)
						waitMITMInboundClosed(t, active)
					}
				}
				done := make(chan error, 1)
				go func() {
					if operation == "revoke" {
						done <- m.Revoke(b.SandboxID)
					} else {
						done <- m.Close()
					}
				}()
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal(
						"teardown did not wait for and finish the inner server",
					)
				}
				if stage != "half_close_read" && stage != "half_close_write" {
					_ = conn.SetReadDeadline(time.Now().Add(time.Second))
					_, err := conn.Read(make([]byte, 1))
					var timeout net.Error
					if err == nil ||
						(errors.As(err, &timeout) && timeout.Timeout()) {
						t.Fatal("parser connection survived teardown")
					}
				}
				active.mu.Lock()
				remaining := len(active.conns)
				active.mu.Unlock()
				if remaining != 0 {
					t.Fatal("tracked connections survived teardown")
				}
			})
		}
	}
}
