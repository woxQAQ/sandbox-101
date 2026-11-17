package errs

import "errors"

// Sentinels identify decisions that callers must handle explicitly. Ordinary
// failures add context with fmt.Errorf; no custom error type or code is used.
var (
	ErrInterrupted    = errors.New("accepted Haven intent retained for recovery")
	ErrNotFound       = errors.New("not found")
	ErrForbidden      = errors.New("root access required")
	ErrConflict       = errors.New("operation conflicts with current state")
	ErrInvalid        = errors.New("invalid request")
	ErrExhausted      = errors.New("admission capacity exhausted")
	ErrIncompatible   = errors.New("incompatible resource")
	ErrUnknown        = errors.New("effect outcome unknown")
	ErrManualRecovery = errors.New("manual recovery required")
)
