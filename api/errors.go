package api

import "fmt"

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

// Error ends dependency classification at the public boundary. Cause remains private.
type Error struct {
	Code    ErrorCode `json:"code"`
	Message string    `json:"message"`
	Cause   error     `json:"-"`
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }
func Fail(code ErrorCode, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}
