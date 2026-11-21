package types

// ApiError is the sanitized HTTP failure payload used by responses and operation views.
// It carries wire data only; dependency causes remain at the host boundary.
type ApiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes are stable wire values shared by HTTP clients and operation views.
const (
	CodeNotFound       = "not_found"
	CodeForbidden      = "forbidden"
	CodeConflict       = "conflict"
	CodeInvalid        = "invalid"
	CodeExhausted      = "resource_exhausted"
	CodeIncompatible   = "incompatible"
	CodeUnknown        = "result_unknown"
	CodeManualRecovery = "manual_recovery_required"
	CodeInternal       = "internal"
)
