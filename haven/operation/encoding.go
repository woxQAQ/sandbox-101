package operation

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func NewID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes[:])
}

func Encode(value any) []byte {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}

func Digest(body []byte) string {
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:])
}

func Terminal(status string) bool {
	return status == "succeeded" || status == "failed"
}

func (r *Repository) Run(ctx context.Context, operationID string) error {
	return store.RequireOne(r.queries.RunLiveOperation(ctx, operationID))
}

func (r *Repository) FailResult(ctx context.Context, operationID string, body []byte) error {
	return store.RequireOne(r.queries.FailResult(ctx, dbgen.FailResultParams{
		ID:         operationID,
		Body:       body,
		FinishedAt: clockNow(),
	}))
}

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

// LinkCompletion fixes the child status and body written to one pending parent
// dependency. Completion still uses the original conditional SQL update.
type LinkCompletion struct {
	OperationID string
	StepKey     string
	Status      string
	Body        []byte
}

func (r *Repository) CompletePendingLink(ctx context.Context, completion LinkCompletion) error {
	return store.RequireOne(r.queries.CompletePendingLink(ctx, dbgen.CompletePendingLinkParams{
		OperationID: completion.OperationID,
		StepKey:     completion.StepKey,
		Status:      completion.Status,
		Body:        completion.Body,
	}))
}
