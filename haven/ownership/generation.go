package ownership

// GenerationRef names an immutable completed resource step without copying ancestry.
type GenerationRef struct {
	OperationID string `json:"operation_id"`
	InputHash   string `json:"input_hash"`
}
