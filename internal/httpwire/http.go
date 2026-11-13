// Package httpwire shares bounded decoding and error translation between adapters.
package httpwire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/internal/haven"
	"github.com/woxQAQ/haven/loom"
)

func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func PublicError(err error) *api.Error {
	if visible, ok := errors.AsType[*api.Error](err); ok {
		return &api.Error{Code: visible.Code, Message: visible.Message}
	}
	if errors.Is(err, haven.ErrInterrupted) {
		return &api.Error{Code: api.ErrUnknown, Message: "accepted operation retained for recovery"}
	}
	if effect, ok := errors.AsType[*loom.Error](err); ok {
		return &api.Error{Code: api.ErrorCode(effect.Code), Message: effect.Message}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &api.Error{Code: api.ErrNotFound, Message: "not found"}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &api.Error{Code: api.ErrUnknown, Message: "operation wait ended"}
	}
	return &api.Error{Code: api.ErrInternal, Message: "internal service error"}
}

func Error(w http.ResponseWriter, err error) {
	failure := PublicError(err)
	status := 500
	switch failure.Code {
	case api.ErrNotFound:
		status = 404
	case api.ErrForbidden:
		status = 403
	case api.ErrInvalid:
		status = 400
	case api.ErrConflict:
		status = 409
	case api.ErrExhausted:
		status = 429
	case api.ErrIncompatible:
		status = 422
	case api.ErrUnknown, api.ErrManualRecovery:
		status = 503
	}
	JSON(w, status, failure)
}

func ReadJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return api.Fail(api.ErrInvalid, "invalid JSON request")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return api.Fail(api.ErrInvalid, "request must contain one JSON value")
	}
	return nil
}

func Respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		Error(w, err)
		return
	}
	JSON(w, 200, value)
}
