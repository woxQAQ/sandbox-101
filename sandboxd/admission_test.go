package sandboxd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func TestClaimIdempotencyAndTenantIsolation(t *testing.T) {
	s, f, _, p := fixture(t)
	r := claimTest(t, s, p, "claim-1", nil)
	again := claimTest(t, s, p, "claim-1", nil)
	if r.Sandbox.ID != again.Sandbox.ID ||
		r.Tokens[r.Sandbox.ID] != again.Tokens[r.Sandbox.ID] ||
		f.count("create") != 1 {
		t.Fatal("retry created a second sandbox or changed token")
	}
	_, err := s.Execute(
		context.Background(),
		p,
		"claim-1",
		Command{Action: "claim", TemplateID: "python", TTLSeconds: 20},
	)
	if !isCode(err, api.Conflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	h := s.Handler()
	foreign := httptest.NewRequest(
		http.MethodGet,
		"/v1/sandboxes/"+r.Sandbox.ID,
		nil,
	)
	foreign.Header.Set("Authorization", "Bearer unknown")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, foreign)
	missing := httptest.NewRequest(http.MethodGet, "/v1/sandboxes/missing", nil)
	missing.Header.Set("Authorization", "Bearer alice-token")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, missing)
	if w1.Code != 404 || w2.Code != 404 ||
		w1.Body.String() != w2.Body.String() {
		t.Fatal("foreign and missing responses differ")
	}
	principal, err := s.Authenticate(
		context.Background(),
		r.Tokens[r.Sandbox.ID],
	)
	if err != nil || principal.SandboxID != r.Sandbox.ID {
		t.Fatal("sandbox token rejected")
	}
	if _, err = s.Execute(
		context.Background(),
		principal,
		"forbidden-claim",
		Command{Action: "claim", TemplateID: "python"},
	); !isCode(
		err,
		api.NotFound,
	) {
		t.Fatal("sandbox token created resources")
	}
}

func TestConcurrentQuotaAdmission(t *testing.T) {
	s, _, _, p := fixture(t)
	_, err := s.PutTenant(
		context.Background(),
		Principal{Root: true},
		Tenant{
			ID:            p.TenantID,
			MaxSandboxes:  2,
			MaxStorageMiB: 10000,
			Token:         "alice-token",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	success := 0
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.Execute(
				context.Background(),
				p,
				fmt.Sprint("claim", i),
				Command{
					Action:     "claim",
					TemplateID: "python",
					Policy:     egress.Policy{Mode: "none"},
				},
			)
			if e == nil {
				mu.Lock()
				success++
				mu.Unlock()
			} else if !isCode(e, api.Exhausted) {
				t.Errorf("unexpected admission error %v", e)
			}
		}(i)
	}
	wg.Wait()
	if success != 2 {
		t.Fatalf("quota admitted %d", success)
	}
}
