package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

func TestPublicFailureResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
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
		{"absent object", errs.ErrNotFound, 404, types.CodeNotFound, "not found"},
		{"foreign object", errs.ErrNotVisible, 404, types.CodeNotFound, "not found"},
		{"internal row missing", sql.ErrNoRows, 500, types.CodeInternal, "internal service error"},
		{"wait ended", context.Canceled, 503, types.CodeUnknown, "operation wait ended"},
		{"retained for recovery", errs.ErrInterrupted, 503, types.CodeUnknown, "accepted operation retained for recovery"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			writeError(c, fmt.Errorf("%w: private fixture detail", tc.cause))
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
