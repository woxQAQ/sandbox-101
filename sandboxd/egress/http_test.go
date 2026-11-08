package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPSAuditFailureBlocksDialAndRecoveryAllowsNextRequest(t *testing.T) {
	var available, persisted atomic.Bool
	var dials, requests, injections, legacyCalls atomic.Int32
	up := httptest.NewTLSServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if !persisted.Load() {
				t.Error("upstream reached before durable injection audit")
			}
			if r.Header.Get("Authorization") != "Bearer host-explicit-secret" ||
				r.Header.Get("X-API-Key") != "host-implicit-secret" {
				t.Error("upstream received wrong credential headers")
			}
			_, _ = io.WriteString(w, "ok")
		}),
	)
	defer up.Close()
	roots := x509.NewCertPool()
	roots.AddCert(up.Certificate())
	dial := targetDialer(up.Listener.Addr().String(), &dials)
	m, b := newTestManager(t, Options{
		RootCAs: roots,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if !persisted.Load() {
				t.Error("upstream dial started before durable injection audit")
			}
			return dial(ctx, network, address)
		},
		Audit: func(AuditEvent) { legacyCalls.Add(1) },
		AuditRecord: func(event AuditEvent) error {
			if event.Action != "inject" {
				return nil
			}
			injections.Add(1)
			if !available.Load() {
				return errors.New("journal unavailable: host-explicit-secret")
			}
			persisted.Store(true)
			return nil
		},
	}, Policy{Mode: "relay"}, map[string]Secret{
		"explicit": {
			Value:       "Bearer host-explicit-secret",
			Hosts:       []string{"example.com"},
			Header:      "Authorization",
			Placeholder: "Bearer phantom",
		},
		"implicit": {
			Value:  "host-implicit-secret",
			Hosts:  []string{"example.com"},
			Header: "X-API-Key",
		},
	})
	client := proxyClient(t, m, b)
	send := func() (*http.Response, string) {
		r, err := http.NewRequest("POST", "https://example.com/", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer phantom")
		resp, err := client.Do(r)
		return resp, readResponse(t, resp, err)
	}
	resp, body := send()
	if resp.StatusCode != http.StatusServiceUnavailable ||
		!strings.Contains(body, "audit unavailable") ||
		strings.Contains(body, "host-explicit-secret") ||
		strings.Contains(body, "journal") {
		t.Fatalf(
			"unexpected audit failure response: %d %q",
			resp.StatusCode,
			body,
		)
	}
	if dials.Load() != 0 || requests.Load() != 0 || persisted.Load() {
		t.Fatal("failed audit allowed upstream access")
	}
	available.Store(true)
	resp, body = send()
	if resp.StatusCode != http.StatusOK || body != "ok" || dials.Load() != 1 ||
		requests.Load() != 1 ||
		injections.Load() != 2 {
		t.Fatalf(
			"audit recovery failed: status=%d dials=%d requests=%d injections=%d",
			resp.StatusCode,
			dials.Load(),
			requests.Load(),
			injections.Load(),
		)
	}
	if legacyCalls.Load() != 0 {
		t.Fatal("AuditRecord unexpectedly fell back to Audit")
	}
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
