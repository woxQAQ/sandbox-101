package operation

import "context"

// PublicationProof contains only frozen operation identity and delivery. Recovery
// validates this history without reconstructing results from current resources.
type PublicationProof struct {
	ID          string
	Key         string
	ScopeKind   string
	ScopeID     string
	Status      string
	CreatedAt   string
	FinishedAt  string
	Publication []byte
	Request     []byte
	Result      []byte
}

func (r *Repository) PublicationProofs(ctx context.Context) ([]PublicationProof, error) {
	rows, err := r.queries.ListPublicationProofs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]PublicationProof, 0, len(rows))
	for _, row := range rows {
		out = append(out, PublicationProof{
			ID:          row.ID,
			Key:         row.IdemKey,
			ScopeKind:   row.ScopeKind,
			ScopeID:     row.ScopeID,
			Status:      row.Status,
			CreatedAt:   row.CreatedAt,
			FinishedAt:  row.FinishedAt,
			Publication: row.Publication,
			Request:     row.Request,
			Result:      row.Result,
		})
	}
	return out, nil
}

func (r *Repository) MissingPublications(ctx context.Context) (int64, error) {
	return r.queries.CountMissingPublications(ctx)
}

func (r *Repository) RecoverySteps(ctx context.Context) ([]Step, error) {
	rows, err := r.queries.ListStepsForRecovery(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Step, 0, len(rows))
	for _, row := range rows {
		out = append(out, Step{
			OperationID:      row.OperationID,
			Key:              row.StepKey,
			Action:           row.Action,
			Status:           row.Status,
			Hash:             row.InputHash,
			Candidate:        row.CandidateResourceID,
			ExpectedInstance: row.ExpectedInstance,
			Input:            row.Input,
			Body:             row.Body,
		})
	}
	return out, nil
}
