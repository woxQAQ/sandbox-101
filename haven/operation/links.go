package operation

import (
	"context"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// ResourceLinkRecord retains the persisted dependency payload and its identity.
// The service verifies the decoded child against the original request hash.
type ResourceLinkRecord struct {
	Input     []byte
	Hash      string
	Status    string
	Candidate string
}

func (r *Repository) ResourceLink(ctx context.Context, parent, key string) (ResourceLinkRecord, error) {
	row, err := r.queries.GetResourceLinkStep(ctx, dbgen.GetResourceLinkStepParams{
		OperationID: parent,
		StepKey:     key,
	})
	return ResourceLinkRecord{
		Input:     row.Input,
		Hash:      row.InputHash,
		Status:    row.Status,
		Candidate: row.CandidateResourceID,
	}, err
}

func (r *Repository) RequestHash(ctx context.Context, id string) (string, error) {
	return r.queries.GetRequestHash(ctx, id)
}

func (r *Repository) StepStatus(ctx context.Context, id, key string) (string, error) {
	return r.queries.GetStepStatus(ctx, dbgen.GetStepStatusParams{
		OperationID: id,
		StepKey:     key,
	})
}

func (r *Repository) CompletePendingLink(ctx context.Context, parent, key, status string, body []byte) error {
	return store.RequireOne(r.queries.CompletePendingLink(ctx, dbgen.CompletePendingLinkParams{
		OperationID: parent,
		StepKey:     key,
		Status:      status,
		Body:        body,
	}))
}
