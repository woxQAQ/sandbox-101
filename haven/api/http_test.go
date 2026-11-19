package api_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/types"
)

func TestTemplateAdmissionFailures(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cause   error
		status  int
		code    string
		message string
	}{
		{"quota", errs.ErrExhausted, 429, types.CodeExhausted, "admission capacity exhausted"},
		{"invalid with diagnostic", errs.ErrInvalid, 400, types.CodeInvalid, "invalid request"},
		{"public input feedback", errs.InvalidInput("template id required"), 400, types.CodeInvalid, "template id required"},
		{"internal row missing", sql.ErrNoRows, 500, types.CodeInternal, "internal service error"},
		{"wait ended", context.Canceled, 503, types.CodeUnknown, "operation wait ended"},
		{"retained for recovery", errs.ErrInterrupted, 503, types.CodeUnknown, "accepted operation retained for recovery"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := api.New(api.Services{
				Authentication: authorization.NewAuthentication("test-admin", nil, nil, nil),
				Requests: sandbox.NewSandboxRequests(sandbox.SandboxRequestDependencies{
					Supply: rejectedTemplate{cause: fmt.Errorf("%w: private fixture detail", tc.cause)},
				}),
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/templates", strings.NewReader(`{"template":{"id":"base"}}`))
			req.Header.Set("Authorization", "Bearer test-admin")
			req.Header.Set("Idempotency-Key", "template-request")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			var failure types.ApiError
			if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
				t.Fatal(err)
			}
			if response.Code != tc.status || failure.Code != tc.code || failure.Message != tc.message || strings.Contains(response.Body.String(), "private fixture detail") {
				t.Fatalf("got HTTP %d %s, want HTTP %d %s / %s", response.Code, response.Body, tc.status, tc.code, tc.message)
			}
		})
	}
}

type rejectedTemplate struct {
	cause error
}

func (s rejectedTemplate) AdmitTemplate(context.Context, authorization.Principal, string, operation.Command) (operation.Record, error) {
	return operation.Record{}, s.cause
}

func (s rejectedTemplate) AdmitRefill(context.Context, authorization.Principal, string, operation.Command) (operation.Record, error) {
	panic("template submission routed to refill")
}
