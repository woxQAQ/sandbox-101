// Package httpwire shares bounded decoding and error translation between adapters.
package httpwire

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// Failure pairs an HTTP status with the payload that can also appear in an operation view.
type Failure struct {
	status  int
	Payload types.ApiError
}

// NewFailure takes a public message chosen by the handler. Dependency diagnostics stay private.
func NewFailure(status int, code, publicMessage string) Failure {
	return Failure{status: status, Payload: types.ApiError{Code: code, Message: publicMessage}}
}

func (f Failure) Write(w http.ResponseWriter) {
	JSON(w, f.status, f.Payload)
}

func InvalidRequest(message string) Failure {
	return NewFailure(http.StatusBadRequest, types.CodeInvalid, message)
}

func NotFound() Failure {
	return NewFailure(http.StatusNotFound, types.CodeNotFound, "not found")
}

func AdminRequired() Failure {
	return NewFailure(http.StatusForbidden, types.CodeForbidden, "root access required")
}

func InternalFailure() Failure {
	return NewFailure(http.StatusInternalServerError, types.CodeInternal, "internal service error")
}

var (
	errInvalidJSON = errors.New("invalid JSON request")
	errExtraJSON   = errors.New("request must contain one JSON value")
)

func classify(err error) Failure {
	switch {
	case errors.Is(err, errs.ErrNotFound), errors.Is(err, errs.ErrNotVisible):
		return NotFound()
	case errors.Is(err, errs.ErrForbidden):
		return AdminRequired()
	case errors.Is(err, errs.ErrConflict):
		return NewFailure(http.StatusConflict, types.CodeConflict, "operation conflicts with current state")
	case errors.Is(err, errs.ErrInvalid):
		return InvalidRequest(errs.InvalidInputMessage(err))
	case errors.Is(err, errs.ErrExhausted):
		return NewFailure(http.StatusTooManyRequests, types.CodeExhausted, "admission capacity exhausted")
	case errors.Is(err, errs.ErrIncompatible):
		return NewFailure(http.StatusUnprocessableEntity, types.CodeIncompatible, "incompatible resource")
	case errors.Is(err, errs.ErrUnknown):
		return NewFailure(http.StatusServiceUnavailable, types.CodeUnknown, "effect outcome unknown")
	case errors.Is(err, errs.ErrManualRecovery):
		return NewFailure(http.StatusServiceUnavailable, types.CodeManualRecovery, "manual recovery required")
	case errors.Is(err, errInvalidJSON):
		return InvalidRequest(errInvalidJSON.Error())
	case errors.Is(err, errExtraJSON):
		return InvalidRequest(errExtraJSON.Error())
	case errors.Is(err, errs.ErrInterrupted):
		return NewFailure(http.StatusServiceUnavailable, types.CodeUnknown, "accepted operation retained for recovery")
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return NewFailure(http.StatusServiceUnavailable, types.CodeUnknown, "operation wait ended")
	default:
		return InternalFailure()
	}
}

func Error(w http.ResponseWriter, err error) {
	classify(err).Write(w)
}

func ReadJSON(w http.ResponseWriter, r *http.Request, value any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
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

func Respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		Error(w, err)
		return
	}
	JSON(w, http.StatusOK, value)
}
