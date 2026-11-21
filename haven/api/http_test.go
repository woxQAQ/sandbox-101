package api_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/testapp"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/types"
)

func TestClaimReplayDuringDrain(t *testing.T) {
	app, _, _, _ := testapp.SandboxFixture(t)
	principal := authorization.Principal{
		Root:     true,
		TenantID: "tenant",
	}
	hostOnly := false
	accepted, err := app.Sandboxes.AdmitClaim(t.Context(), principal, "original", operation.Intent{
		Action:     "claim",
		TemplateID: "cold",
		TTLSeconds: 60,
		Env: map[string]types.EnvVar{
			"TOKEN": {
				Value: "fixture-value",
				Guest: &hostOnly,
				Inject: &types.Injection{
					Hosts: []string{
						"example.test",
					},
					Header: "Authorization",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Node.Drain(principal); err != nil {
		t.Fatal(err)
	}
	handler := api.New(api.Services{
		Authentication: app.Authentication,
		Requests:       app.CommandRouter,
		Operations:     app.Operations,
		Delivery:       app.Delivery,
	})
	const body = `{"template_id":"cold","ttl_seconds":60,"env":{"TOKEN":{"guest":false,"value":"fixture-value","inject":{"hosts":["example.test"],"header":"Authorization"}}}}`
	post := func(key, payload string, status int) []byte {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/sandboxes", strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer fixture-root-token")
		req.Header.Set("X-Tenant-ID", principal.TenantID)
		req.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != status {
			t.Fatalf("POST claim returned %d, want %d: %s", w.Code, status, w.Body)
		}
		return w.Body.Bytes()
	}
	var result types.Result
	if err := json.Unmarshal(post("original", body, http.StatusOK), &result); err != nil || result.OperationID != accepted.ID {
		t.Fatalf("replay returned operation %q, want %q: %v", result.OperationID, accepted.ID, err)
	}
	post("original", strings.Replace(body, "fixture-value", "changed", 1), http.StatusConflict)
	post("new-key", body, http.StatusTooManyRequests)
}
