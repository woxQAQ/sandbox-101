package sandboxd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
)

func TestSandboxTokenCanPollOwnLeaseOperationsHTTP(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	token := source.Tokens[source.Sandbox.ID]
	sp, err := s.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	h := s.Handler()
	get := func(path, credential string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Authorization", "Bearer "+credential)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	other := claimTest(t, s, p, "other", nil)
	for _, action := range []string{"renew", "release"} {
		o, err := s.Submit(
			context.Background(),
			sp,
			action,
			Command{Action: action, SandboxID: sp.SandboxID, TTLSeconds: 60},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Wait(context.Background(), sp, o.ID); err != nil {
			t.Fatal(err)
		}
		own := get("/v1/operations/"+o.ID, token)
		if own.Code != 200 {
			t.Fatalf(
				"%s polling failed: %d %s",
				action,
				own.Code,
				own.Body.String(),
			)
		}
		var got Operation
		if err = json.Unmarshal(own.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if action == "release" && len(got.Result.Tokens) != 0 {
			t.Fatal("terminated credential issued")
		}
		foreign := get("/v1/operations/"+o.ID, other.Tokens[other.Sandbox.ID])
		missing := get("/v1/operations/missing", token)
		if foreign.Code != 404 || missing.Code != 404 ||
			foreign.Body.String() != missing.Body.String() {
			t.Fatal("foreign lease operation disclosed")
		}
	}
	if got := get("/v1/sandboxes/"+sp.SandboxID, token); got.Code != 404 {
		t.Fatal("terminated credential accessed sandbox", got.Code)
	}
	if got := get(
		"/v1/operations/"+source.OperationID,
		token,
	); got.Code != 404 {
		t.Fatal("terminated credential read claim", got.Code)
	}
	if _, err = s.Authenticate(
		context.Background(),
		token,
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("ordinary authentication allowed terminated credential", err)
	}
}

func TestRootCapturesPreserveSourceTenant(t *testing.T) {
	for _, action := range []string{"pause", "checkpoint"} {
		for _, selected := range []string{"", "bob"} {
			t.Run(action+"/tenant="+selected, func(t *testing.T) {
				s, _, _, alice := fixture(t)
				source := claimTest(t, s, alice, "source", nil)
				if _, err := s.PutTenant(
					context.Background(),
					Principal{Root: true},
					Tenant{
						ID:            "bob",
						Token:         "bob-token",
						MaxSandboxes:  10,
						MaxStorageMiB: 1 << 20,
					},
				); err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(
					http.MethodPost,
					"/v1/sandboxes/"+source.Sandbox.ID+"/"+action,
					strings.NewReader(`{}`),
				)
				req.Header.Set("Authorization", "Bearer root-secret")
				req.Header.Set("Idempotency-Key", "root-capture")
				if selected != "" {
					req.Header.Set("X-Tenant-ID", selected)
				}
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, req)
				if w.Code != 200 {
					t.Fatalf(
						"root management failed: status=%d body=%s",
						w.Code,
						w.Body.String(),
					)
				}
				var result Result
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				snapshot := result.Sandbox.SnapshotID
				if action == "checkpoint" {
					snapshot = result.Checkpoint.SnapshotID
				}
				var owner string
				if err := s.store.db.QueryRow("SELECT tenant_id FROM snapshot_owners WHERE snapshot_id=?", snapshot).
					Scan(&owner); err != nil {
					t.Fatal(err)
				}
				if owner != alice.TenantID {
					t.Fatalf(
						"root capture changed owner: got=%s want=%s",
						owner,
						alice.TenantID,
					)
				}
				if _, err := s.Execute(
					context.Background(),
					Principal{TenantID: "bob"},
					"bob-import",
					Command{
						Action: "template_put",
						Template: &Template{
							ID:         "stolen",
							SnapshotID: snapshot,
							Shape:      source.Sandbox.Shape,
						},
					},
				); !isCode(
					err,
					api.NotFound,
				) {
					t.Fatal("Bob imported Alice capture", err)
				}
				if action == "checkpoint" {
					if _, err := s.Execute(
						context.Background(),
						alice,
						"alice-pause",
						Command{Action: "pause", SandboxID: source.Sandbox.ID},
					); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := s.Execute(
					context.Background(),
					alice,
					"alice-resume",
					Command{Action: "resume", SandboxID: source.Sandbox.ID},
				); err != nil {
					t.Fatal("Alice cannot resume after root capture", err)
				}
			})
		}
	}
}

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
			if tc.path != "/metrics" &&
				w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("missing cache protection: %v", w.Header())
			}
		})
	}
}

func TestHTTPClaimIdempotencyAndOperationRoute(t *testing.T) {
	s, f, _, _ := fixture(t)
	h := s.Handler()
	claim := func(prefer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(
			http.MethodPost,
			"/v1/claim",
			strings.NewReader(
				`{"template_id":"python","policy":{"mode":"none"}}`,
			),
		)
		r.Header.Set("Authorization", "Bearer alice-token")
		r.Header.Set("Idempotency-Key", "http-claim")
		r.Header.Set("Prefer", prefer)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := claim("respond-async")
	if first.Code != http.StatusAccepted ||
		!strings.HasPrefix(first.Header().Get("Location"), "/v1/operations/") {
		t.Fatalf(
			"asynchronous claim: %d %v %s",
			first.Code,
			first.Header(),
			first.Body.String(),
		)
	}
	r := httptest.NewRequest(
		http.MethodGet,
		first.Header().Get("Location"),
		nil,
	)
	r.Header.Set("Authorization", "Bearer alice-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("operation route: %d %s", w.Code, w.Body.String())
	}
	var results [2]Result
	for i := range results {
		w := claim("")
		if err := json.Unmarshal(
			w.Body.Bytes(),
			&results[i],
		); err != nil || w.Code != http.StatusOK ||
			results[i].Sandbox == nil {
			t.Fatalf("claim: %d %s decode=%v", w.Code, w.Body.String(), err)
		}
	}
	if results[0].Sandbox.ID != results[1].Sandbox.ID ||
		f.count("create") != 1 {
		t.Fatalf(
			"claim replay created a new sandbox: %+v creations=%d",
			results,
			f.count("create"),
		)
	}
}

func TestHTTPUpgradeRejectsHTTP2BeforeResuming(t *testing.T) {
	s, f, _, _ := fixture(t)
	r := httptest.NewRequest(
		http.MethodGet,
		"/v1/sandboxes/missing/connect",
		nil,
	)
	r.Proto, r.ProtoMajor, r.ProtoMinor = "HTTP/2.0", 2, 0
	r.Header.Set("Authorization", "Bearer alice-token")
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "sandbox-v1")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	var result api.Error
	if err := json.Unmarshal(
		w.Body.Bytes(),
		&result,
	); err != nil || w.Code != http.StatusUnprocessableEntity || result.Code != api.Incompatible ||
		f.count("clone") != 0 {
		t.Fatalf(
			"status=%d result=%+v clones=%d decode=%v",
			w.Code,
			result,
			f.count("clone"),
			err,
		)
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
		r := httptest.NewRequest(
			http.MethodPost,
			"/v1/claim",
			strings.NewReader(body),
		)
		r.Header.Set("Authorization", "Bearer alice-token")
		r.Header.Set("Idempotency-Key", "invalid-claim")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		var result api.Error
		if err := json.Unmarshal(
			w.Body.Bytes(),
			&result,
		); err != nil || w.Code != http.StatusBadRequest || result.Code != api.Invalid ||
			f.count("create") != 0 {
			t.Fatalf(
				"status=%d result=%+v creations=%d decode=%v",
				w.Code,
				result,
				f.count("create"),
				err,
			)
		}
	}
}
