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

func (r *Repository) FailResult(ctx context.Context, operationID string, result Result) error {
	return r.updateResult(ctx, operationID, "failed", result)
}

func (r *Repository) StepStatus(ctx context.Context, id, key string) (string, error) {
	return r.queries.GetStepStatus(ctx, dbgen.GetStepStatusParams{
		OperationID: id,
		StepKey:     key,
	})
}

// LinkCompletion records the verified terminal child status on a pending parent link.
type LinkCompletion struct {
	OperationID string
	StepKey     string
	Status      string
}

func (r *Repository) CompletePendingLink(ctx context.Context, completion LinkCompletion) error {
	return store.RequireOne(r.queries.CompletePendingLink(ctx, dbgen.CompletePendingLinkParams{
		OperationID: completion.OperationID,
		StepKey:     completion.StepKey,
		Status:      completion.Status,
	}))
}
