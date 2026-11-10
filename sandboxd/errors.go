package sandboxd

import (
	"errors"
	"fmt"

	"github.com/woxQAQ/sandbox-engine/api"
)

// ErrorCode describes the action available to a sandboxd caller, not an Engine
// resource observation. Equal wire strings do not make the two domains equivalent.
type ErrorCode string

const (
	ErrNotFound       ErrorCode = "not_found"
	ErrForbidden      ErrorCode = "forbidden"
	ErrConflict       ErrorCode = "conflict"
	ErrInvalid        ErrorCode = "invalid"
	ErrExhausted      ErrorCode = "resource_exhausted"
	ErrIncompatible   ErrorCode = "incompatible"
	ErrUnknown        ErrorCode = "result_unknown"
	ErrManualRecovery ErrorCode = "manual_recovery_required"
	ErrInternal       ErrorCode = "internal"
)

// Error is the sandboxd HTTP and durable-operation error contract. Cause is
// process-local diagnostic evidence, never part of a tenant response or receipt.
// Deliberately no Unwrap: translating an error ends dependency classification.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Cause   error     `json:"-"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func Fail(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Wrap assigns a tenant-safe message while retaining private dependency evidence.
func Wrap(code ErrorCode, message string, cause error) *Error {
	return &Error{Code: code, Message: message, Cause: cause}
}

func isCode(err error, code ErrorCode) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && e.Code == code
}

// Only an unconsumed Engine response can establish physical absence. Joined
// errors and translated sandboxd errors are not authoritative Engine answers.
func isEngineCode(err error, code api.Code) bool {
	for err != nil {
		switch e := err.(type) {
		case *Error:
			return false
		case *api.Error:
			return e.Code == code
		case interface{ Unwrap() error }:
			err = e.Unwrap()
		default:
			return false
		}
	}
	return false
}

// engineFailure is the fallback after the caller has consumed any expected
// absence or generation-fence outcome. Missing internal resources are not a
// tenant-facing 404, nor are Engine argument failures tenant input validation.
func engineFailure(err error) error {
	if err == nil {
		return nil
	}
	code, message := ErrInternal, "engine operation failed"
	var dependency *api.Error
	errors.As(err, &dependency)
	var kind api.Code
	if dependency != nil {
		kind = dependency.Code
	}
	switch kind {
	case api.Unknown:
		code, message = ErrUnknown, "engine result unknown; retain the same operation key"
	case api.Exhausted:
		code, message = ErrExhausted, "engine resource capacity exhausted"
	case api.Incompatible:
		code, message = ErrIncompatible, "engine configuration is incompatible"
	case api.NotFound, api.Conflict, api.ManualRecovery:
		code, message = ErrManualRecovery, "engine resource state requires operator recovery"
	}
	return Wrap(code, message, err)
}

func internalError(err error) *Error {
	var dependency *api.Error
	if errors.As(err, &dependency) && dependency.Code == api.Unknown {
		return engineFailure(err).(*Error)
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	if _, ok := errors.AsType[*api.Error](err); ok {
		return engineFailure(err).(*Error)
	}
	return Wrap(ErrInternal, "internal error", err)
}

// Public projections also cover historical receipts, whose diagnostic messages
// must not be rewritten on disk merely to serve a safe tenant response.
func publicError(err error) *Error {
	e := internalError(err)
	message := e.Message
	switch e.Code {
	case ErrInternal:
		message = "internal error"
	case ErrManualRecovery:
		message = "manual recovery required"
	case ErrUnknown:
		message = "result unknown; retain the same operation or exec key"
	case ErrNotFound:
		message = "not found"
	case ErrForbidden, ErrConflict, ErrInvalid, ErrExhausted, ErrIncompatible:
	default:
		return &Error{Code: ErrInternal, Message: "internal error"}
	}
	return &Error{Code: e.Code, Message: message}
}

func publicOperation(o Operation) Operation {
	if o.Error != nil {
		o.Error = publicError(o.Error)
		// Persisted messages have no tenant-safety provenance. Even an old
		// conflict/invalid receipt may contain a raw dependency diagnostic.
		switch o.Error.Code {
		case ErrForbidden:
			o.Error.Message = "forbidden"
		case ErrConflict:
			o.Error.Message = "operation state conflict"
		case ErrInvalid:
			o.Error.Message = "invalid request"
		case ErrExhausted:
			o.Error.Message = "resource capacity exhausted"
		case ErrIncompatible:
			o.Error.Message = "incompatible configuration"
		}
	}
	o.Result = publicResult(o.Result)
	return o
}

func publicSandbox(b Sandbox) Sandbox {
	if b.LastError != "" {
		b.LastError = "sandbox requires operator review"
	}
	return b
}

func publicResult(r Result) Result {
	if r.Sandbox != nil {
		b := publicSandbox(*r.Sandbox)
		r.Sandbox = &b
	}
	if r.Sandboxes != nil {
		r.Sandboxes = append([]Sandbox(nil), r.Sandboxes...)
		for i := range r.Sandboxes {
			r.Sandboxes[i] = publicSandbox(r.Sandboxes[i])
		}
	}
	return r
}

// Private journal diagnostics follow Cause explicitly without reopening the
// dependency classification boundary used for recovery decisions.
func privateDiagnostic(err error) string {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok && e == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) && e != nil && e.Cause != nil {
		return err.Error() + ": " + privateDiagnostic(e.Cause)
	}
	return err.Error()
}
