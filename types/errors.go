package types

// ApiError is the sanitized HTTP failure payload used by responses and operation views.
// It carries wire data only; dependency causes remain at the host boundary.
type ApiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes are stable wire values shared by HTTP clients and operation views.
const (
	ErrNotFound       = "not_found"
	ErrForbidden      = "forbidden"
	ErrConflict       = "conflict"
	ErrInvalid        = "invalid"
	ErrExhausted      = "resource_exhausted"
	ErrIncompatible   = "incompatible"
	ErrUnknown        = "result_unknown"
	ErrManualRecovery = "manual_recovery_required"
	ErrInternal       = "internal"
)
