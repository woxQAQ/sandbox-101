package egress

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestInjectionPersistsAuditBeforeChangingAnyHeader(t *testing.T) {
	for _, mode := range []string{"record_success", "record_failure", "legacy_success"} {
		t.Run(mode, func(t *testing.T) {
			r, err := http.NewRequest(
				"POST",
				"https://example.com/private-path?private=query",
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer phantom")
			r.Header.Set("X-API-Key", "guest-value")
			creds, err := compileSecrets(map[string]Secret{
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
			if err != nil {
				t.Fatal(err)
			}
			calls, legacyCalls := 0, 0
			check := func(event AuditEvent) {
				calls++
				if r.Header.Get("Authorization") != "Bearer phantom" ||
					r.Header.Get("X-API-Key") != "guest-value" {
					t.Error(
						"credential headers changed before audit confirmation",
					)
				}
				if event.Action != "inject" || event.Host != "example.com" ||
					event.Method != "POST" ||
					event.TenantID != "tenant" ||
					event.SandboxID != "sandbox" ||
					event.Time.IsZero() {
					t.Error("incomplete injection audit metadata")
				}
				data, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				for _, private := range []string{"host-explicit-secret", "host-implicit-secret", "private-path", "private=query"} {
					if strings.Contains(string(data), private) {
						t.Error("audit contained private request material")
					}
				}
			}
			opts := Options{Audit: func(AuditEvent) { legacyCalls++ }}
			if mode == "legacy_success" {
				opts.Audit = func(event AuditEvent) { legacyCalls++; check(event) }
			} else {
				opts.AuditRecord = func(event AuditEvent) error {
					check(event)
					if mode == "record_failure" {
						return errors.New(
							"storage error including host-explicit-secret",
						)
					}
					return nil
				}
			}
			b := &binding{
				info:    Binding{SandboxID: "sandbox", TenantID: "tenant"},
				manager: &Manager{opts: opts},
				secrets: creds,
			}
			err = b.inject(r, "example.com")
			if mode == "record_failure" {
				if !errors.Is(err, ErrAudit) ||
					strings.Contains(err.Error(), "host-explicit-secret") {
					t.Error("audit failure did not return a sanitized error")
				}
				if r.Header.Get("Authorization") != "Bearer phantom" ||
					r.Header.Get("X-API-Key") != "guest-value" {
					t.Error("failed audit still changed credential headers")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if r.Header.Get(
					"Authorization",
				) != "Bearer host-explicit-secret" ||
					r.Header.Get("X-API-Key") != "host-implicit-secret" {
					t.Error("persisted audit did not permit injection")
				}
			}
			if calls != 1 {
				t.Fatalf("audit called %d times", calls)
			}
			if mode != "legacy_success" && legacyCalls != 0 {
				t.Error(
					"AuditRecord invoked the legacy sink or fell back after failure",
				)
			}
		})
	}
}

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
