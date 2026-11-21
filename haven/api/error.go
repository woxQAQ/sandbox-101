package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

// errInvalidJSON and errExtraJSON mark request bodies that fail strict decode.
var (
	errInvalidJSON = errors.New("invalid JSON request")
	errExtraJSON   = errors.New("request must contain one JSON value")
)

// translateError maps any service error onto the public status and payload.
// Dependency diagnostics stay private; this message is the whole contract, so
// both adapters share the one table.
func translateError(err error) (int, types.ApiError) {
	switch {
	case errors.Is(err, errs.ErrNotFound), errors.Is(err, errs.ErrNotVisible):
		return http.StatusNotFound, types.ApiError{
			Code:    types.CodeNotFound,
			Message: "not found",
		}
	case errors.Is(err, errs.ErrForbidden):
		return http.StatusForbidden, types.ApiError{
			Code:    types.CodeForbidden,
			Message: "root access required",
		}
	case errors.Is(err, errs.ErrConflict):
		return http.StatusConflict, types.ApiError{
			Code:    types.CodeConflict,
			Message: "operation conflicts with current state",
		}
	case errors.Is(err, errs.ErrInvalid):
		return http.StatusBadRequest, types.ApiError{
			Code:    types.CodeInvalid,
			Message: errs.InvalidInputMessage(err),
		}
	case errors.Is(err, errs.ErrExhausted):
		return http.StatusTooManyRequests, types.ApiError{
			Code:    types.CodeExhausted,
			Message: "admission capacity exhausted",
		}
	case errors.Is(err, errs.ErrIncompatible):
		return http.StatusUnprocessableEntity, types.ApiError{
			Code:    types.CodeIncompatible,
			Message: "incompatible resource",
		}
	case errors.Is(err, errs.ErrUnknown):
		return http.StatusServiceUnavailable, types.ApiError{
			Code:    types.CodeUnknown,
			Message: "effect outcome unknown",
		}
	case errors.Is(err, errs.ErrManualRecovery):
		return http.StatusServiceUnavailable, types.ApiError{
			Code:    types.CodeManualRecovery,
			Message: "manual recovery required",
		}
	case errors.Is(err, errInvalidJSON):
		return http.StatusBadRequest, types.ApiError{
			Code:    types.CodeInvalid,
			Message: errInvalidJSON.Error(),
		}
	case errors.Is(err, errExtraJSON):
		return http.StatusBadRequest, types.ApiError{
			Code:    types.CodeInvalid,
			Message: errExtraJSON.Error(),
		}
	case errors.Is(err, errs.ErrInterrupted):
		return http.StatusServiceUnavailable, types.ApiError{
			Code:    types.CodeUnknown,
			Message: "accepted operation retained for recovery",
		}
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return http.StatusServiceUnavailable, types.ApiError{
			Code:    types.CodeUnknown,
			Message: "operation wait ended",
		}
	default:
		return http.StatusInternalServerError, types.ApiError{
			Code:    types.CodeInternal,
			Message: "internal service error",
		}
	}
}

// writeError answers the request with the translated public failure and ends
// the handler chain; nothing after a service error may still write.
func writeError(c *gin.Context, err error) {
	status, payload := translateError(err)
	c.AbortWithStatusJSON(status, payload)
}

// invalidRequest rejects the request with one public client error message.
func invalidRequest(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, types.ApiError{
		Code:    types.CodeInvalid,
		Message: message,
	})
}

// notFound hides both absent and invisible objects behind one answer.
func notFound(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusNotFound, types.ApiError{
		Code:    types.CodeNotFound,
		Message: "not found",
	})
}

// internalFailure reports a terminal operation failure without diagnostics.
func internalFailure(c *gin.Context) {
	c.AbortWithStatusJSON(http.StatusInternalServerError, types.ApiError{
		Code:    types.CodeInternal,
		Message: "internal service error",
	})
}

// decodeJSON reads exactly one bounded JSON value: 1 MiB at most, no unknown
// fields, no trailing values. Callers report the returned error with
// writeError; the body stays unread after a failure.
func decodeJSON(c *gin.Context, value any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errInvalidJSON
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errExtraJSON
	}
	return nil
}

// noStore marks every control response uncacheable, success or failure.
func noStore(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
}
