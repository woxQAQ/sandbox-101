package errs

import "errors"

// Sentinels identify decisions that callers must handle explicitly.
// Ordinary failures add private context with fmt.Errorf.
var (
	ErrInterrupted    = errors.New("accepted Haven intent retained for recovery")
	ErrNotFound       = errors.New("not found")
	ErrNotVisible     = errors.New("object outside principal scope")
	ErrForbidden      = errors.New("root access required")
	ErrConflict       = errors.New("operation conflicts with current state")
	ErrInvalid        = errors.New("invalid request")
	ErrExhausted      = errors.New("admission capacity exhausted")
	ErrIncompatible   = errors.New("incompatible resource")
	ErrUnknown        = errors.New("effect outcome unknown")
	ErrManualRecovery = errors.New("manual recovery required")
)

// Public input feedback is selected at the validation site. The private string
// type lets other packages pass string constants, but not runtime diagnostics.
type inputProblem string

func InvalidInput(message inputProblem) error { return message }

func (p inputProblem) Error() string { return ErrInvalid.Error() + ": " + string(p) }
func (p inputProblem) Unwrap() error { return ErrInvalid }

func InvalidInputMessage(err error) string {
	if problem, ok := errors.AsType[inputProblem](err); ok {
		return string(problem)
	}
	return "invalid request"
}
