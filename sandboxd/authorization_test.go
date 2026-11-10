package sandboxd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticatedForeignTenantCannotDiscoverObjects(t *testing.T) {
	s, _, _, p := fixture(t)
	r := claimTest(t, s, p, "claim", nil)
	if _, err := s.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            "bob",
			Token:         "bob-token",
			MaxSandboxes:  5,
			MaxStorageMiB: 4096,
		},
	); err != nil {
		t.Fatal(err)
	}
	request := func(path string) *httptest.ResponseRecorder {
		q := httptest.NewRequest(http.MethodGet, path, nil)
		q.Header.Set("Authorization", "Bearer bob-token")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, q)
		return w
	}
	foreign, missing := request(
		"/v1/sandboxes/"+r.Sandbox.ID,
	), request(
		"/v1/sandboxes/missing",
	)
	if foreign.Code != 404 || missing.Code != 404 ||
		foreign.Body.String() != missing.Body.String() {
		t.Fatal("authenticated tenant can distinguish foreign object")
	}
}

func TestSandboxTokenCannotReadForkOperation(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	sp := Principal{TenantID: p.TenantID, SandboxID: source.Sandbox.ID}
	if _, err := s.Operation(
		context.Background(),
		sp,
		r.OperationID,
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatalf("fork operation visible to source: %v", err)
	}
	h := s.Handler()
	get := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/v1/operations/"+id, nil)
		req.Header.Set(
			"Authorization",
			"Bearer "+source.Tokens[source.Sandbox.ID],
		)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	foreign, missing := get(r.OperationID), get("missing")
	if foreign.Code != 404 || foreign.Body.String() != missing.Body.String() {
		t.Fatalf(
			"foreign response differs: %d %s",
			foreign.Code,
			foreign.Body.String(),
		)
	}
}

func TestSandboxTokenCanPollOwnLeaseOperations(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	sp := Principal{TenantID: p.TenantID, SandboxID: source.Sandbox.ID}
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
		r, err := s.Wait(context.Background(), sp, o.ID)
		if err != nil {
			t.Fatal(err)
		}
		if action == "release" && len(r.Tokens) != 0 {
			t.Fatal("terminated token issued")
		}
		for _, other := range []Principal{{TenantID: p.TenantID, SandboxID: "other"}, {TenantID: "other"}} {
			if _, err := s.Operation(
				context.Background(),
				other,
				o.ID,
			); !isCode(
				err,
				ErrNotFound,
			) {
				t.Fatalf("foreign read: %v", err)
			}
		}
	}
}

func TestOperationResultTokensRespectPrincipal(t *testing.T) {
	s, _, _, p := fixture(t)
	source := claimTest(t, s, p, "source", nil)
	r, err := s.Execute(
		context.Background(),
		p,
		"fork",
		Command{Action: "fork", SandboxID: source.Sandbox.ID, Count: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.store.operation(context.Background(), r.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	o.Result.Tokens = map[string]string{"forged": "credential"}
	if err := saveOp(context.Background(), s.store.db, o); err != nil {
		t.Fatal(err)
	}
	for _, principal := range []Principal{p, {Root: true}} {
		got, err := s.Wait(context.Background(), principal, o.ID)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Tokens) != 2 || got.Tokens["forged"] != "" {
			t.Fatal("tokens were not rebuilt")
		}
	}
	for _, credential := range []string{"alice-token", "root-secret"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/operations/"+o.ID, nil)
		req.Header.Set("Authorization", "Bearer "+credential)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		var got Operation
		if err := json.Unmarshal(
			w.Body.Bytes(),
			&got,
		); err != nil || w.Code != 200 || len(got.Result.Tokens) != 2 ||
			got.Result.Tokens["forged"] != "" {
			t.Fatalf(
				"HTTP result tokens were not rebuilt: status=%d decode=%v",
				w.Code,
				err,
			)
		}
	}
	o.Result.Sandboxes[0].TenantID = "foreign"
	if err := saveOp(context.Background(), s.store.db, o); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Operation(
		context.Background(),
		p,
		o.ID,
	); !isCode(
		err,
		ErrNotFound,
	) {
		t.Fatalf("foreign result exposed: %v", err)
	}
}
