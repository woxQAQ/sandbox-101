// Package httpwire shares bounded decoding and error translation between adapters.
package httpwire

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/types"
)

func JSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

var (
	errInvalidJSON = errors.New("invalid JSON request")
	errExtraJSON   = errors.New("request must contain one JSON value")
)

// PublicError formats only recognized admission failures. Unclassified host
// diagnostics remain private and cannot become tenant-resource absence proofs.
func PublicError(err error) *types.ApiError {
	for _, failure := range []struct {
		reason error
		code   string
	}{
		{errs.ErrNotFound, types.CodeNotFound},
		{errs.ErrForbidden, types.CodeForbidden},
		{errs.ErrConflict, types.CodeConflict},
		{errs.ErrInvalid, types.CodeInvalid},
		{errs.ErrExhausted, types.CodeExhausted},
		{errs.ErrIncompatible, types.CodeIncompatible},
		{errs.ErrUnknown, types.CodeUnknown},
		{errs.ErrManualRecovery, types.CodeManualRecovery},
	} {
		if errors.Is(err, failure.reason) {
			return &types.ApiError{
				Code:    failure.code,
				Message: strings.TrimPrefix(err.Error(), failure.reason.Error()+": "),
			}
		}
	}
	if errors.Is(err, errInvalidJSON) {
		return &types.ApiError{
			Code:    types.CodeInvalid,
			Message: errInvalidJSON.Error(),
		}
	}
	if errors.Is(err, errExtraJSON) {
		return &types.ApiError{
			Code:    types.CodeInvalid,
			Message: errExtraJSON.Error(),
		}
	}
	if errors.Is(err, errs.ErrInterrupted) {
		return &types.ApiError{
			Code:    types.CodeUnknown,
			Message: "accepted operation retained for recovery",
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &types.ApiError{
			Code:    types.CodeNotFound,
			Message: "not found",
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &types.ApiError{
			Code:    types.CodeUnknown,
			Message: "operation wait ended",
		}
	}
	return &types.ApiError{
		Code:    types.CodeInternal,
		Message: "internal service error",
	}
}

func Error(w http.ResponseWriter, err error) {
	Failure(w, *PublicError(err))
}

// Failure writes already sanitized wire data without constructing a Go error.
func Failure(w http.ResponseWriter, failure types.ApiError) {
	status := http.StatusInternalServerError
	switch failure.Code {
	case types.CodeNotFound:
		status = http.StatusNotFound
	case types.CodeForbidden:
		status = http.StatusForbidden
	case types.CodeInvalid:
		status = http.StatusBadRequest
	case types.CodeConflict:
		status = http.StatusConflict
	case types.CodeExhausted:
		status = http.StatusTooManyRequests
	case types.CodeIncompatible:
		status = http.StatusUnprocessableEntity
	case types.CodeUnknown, types.CodeManualRecovery:
		status = http.StatusServiceUnavailable
	}
	JSON(w, status, failure)
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
