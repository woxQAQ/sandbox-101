package egress

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
