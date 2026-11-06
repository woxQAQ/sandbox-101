package sandboxd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestHTTPRoutesAndAuthentication(t *testing.T) {
	s, _, _, _ := fixture(t)
	h := s.Handler()
	for _, tc := range []struct {
		method, path, token string
		status              int
	}{
		{http.MethodGet, "/healthz", "", 200},
		{http.MethodHead, "/healthz", "", 200},
		{http.MethodGet, "/metrics", "root-secret", 200},
		{http.MethodGet, "/metrics", "alice-token", 403},
		{http.MethodGet, "/metrics", "", 404},
		{http.MethodGet, "/v1/sandboxes", "alice-token", 200},
		{http.MethodGet, "/v1/templates", "alice-token", 200},
		{http.MethodGet, "/v1/checkpoints", "alice-token", 200},
		{http.MethodGet, "/v1/sandboxes/missing", "alice-token", 404},
		{http.MethodGet, "/v1/operations/missing", "alice-token", 404},
		{http.MethodGet, "/v1/sandboxes", "", 404},
		{http.MethodGet, "/v1/unknown", "", 404},
		{http.MethodPut, "/v1/sandboxes", "", 404},
		{http.MethodPut, "/v1/sandboxes", "alice-token", 405},
		{http.MethodPost, "/v1/sandboxes/missing", "alice-token", 405},
		{http.MethodDelete, "/v1/checkpoints/missing/fork", "alice-token", 405},
		{http.MethodGet, "/preview/invalid/path", "", 404},
		{http.MethodGet, "/v1/sandboxes/", "alice-token", 404},
	} {
		t.Run(tc.method+" "+tc.path+" "+tc.token, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.path != "/metrics" && w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("missing cache protection: %v", w.Header())
			}
		})
	}
}

func TestHTTPClaimIdempotencyAndOperationRoute(t *testing.T) {
	s, f, _, _ := fixture(t)
	h := s.Handler()
	claim := func(prefer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/v1/claim", strings.NewReader(`{"template_id":"python","policy":{"mode":"none"}}`))
		r.Header.Set("Authorization", "Bearer alice-token")
		r.Header.Set("Idempotency-Key", "http-claim")
		r.Header.Set("Prefer", prefer)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := claim("respond-async")
	if first.Code != http.StatusAccepted || !strings.HasPrefix(first.Header().Get("Location"), "/v1/operations/") {
		t.Fatalf("asynchronous claim: %d %v %s", first.Code, first.Header(), first.Body.String())
	}
	r := httptest.NewRequest(http.MethodGet, first.Header().Get("Location"), nil)
	r.Header.Set("Authorization", "Bearer alice-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("operation route: %d %s", w.Code, w.Body.String())
	}
	var results [2]Result
	for i := range results {
		w := claim("")
		if err := json.Unmarshal(w.Body.Bytes(), &results[i]); err != nil || w.Code != http.StatusOK || results[i].Sandbox == nil {
			t.Fatalf("claim: %d %s decode=%v", w.Code, w.Body.String(), err)
		}
	}
	if results[0].Sandbox.ID != results[1].Sandbox.ID || f.count("create") != 1 {
		t.Fatalf("claim replay created a new sandbox: %+v creations=%d", results, f.count("create"))
	}
}

func TestHTTPUpgradeRejectsHTTP2BeforeResuming(t *testing.T) {
	s, f, _, _ := fixture(t)
	r := httptest.NewRequest(http.MethodGet, "/v1/sandboxes/missing/connect", nil)
	r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/2.0", 2, 0
	r.Header.Set("Authorization", "Bearer alice-token")
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "sandbox-v1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var result api.Error
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != http.StatusUnprocessableEntity || result.Code != api.Incompatible || f.count("clone") != 0 {
		t.Fatalf("status=%d result=%+v clones=%d decode=%v", w.Code, result, f.count("clone"), err)
	}
}

func TestHTTPRejectsInvalidJSONBeforeMutation(t *testing.T) {
	s, f, _, _ := fixture(t)
	h := s.Handler()
	for _, body := range []string{
		`{"template_id":"python","unexpected":true}`,
		`{"template_id":"python"} {}`,
		`{"template_id":`,
		`{"template_id":"` + strings.Repeat("x", 1<<20) + `"}`,
	} {
		r := httptest.NewRequest(http.MethodPost, "/v1/claim", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer alice-token")
		r.Header.Set("Idempotency-Key", "invalid-claim")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var result api.Error
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != http.StatusBadRequest || result.Code != api.Invalid || f.count("create") != 0 {
			t.Fatalf("status=%d result=%+v creations=%d decode=%v", w.Code, result, f.count("create"), err)
		}
	}
}
