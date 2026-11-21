package imageimport

// ImportWork carries one frozen import plan and its original prepared directory
// identities through explicit effects. It owns no catalog or recovery loop.
type ImportWork struct {
	StateRoot   string
	OperationID string
	Plan        ImageImportPlan
	Boundary    ImageImportBoundary
}
