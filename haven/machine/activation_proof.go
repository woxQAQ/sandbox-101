package machine

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
)

// ActivationProof carries only the original step identity and resource linkage.
// Settings and business transition decisions remain with sandbox.
type ActivationProof struct {
	OperationID       string
	Key               string
	Hash              string
	Status            string
	Phase             string
	ResourceOperation string
	ResourceHash      string
	ExpectedInstance  string
}

// ActivationProofReader validates the complete frozen activation before exposing
// its resource linkage. The borrowed DBTX is not statically read-only: readers
// must not write, commit, checkpoint, dispatch work, or capture a running App.
type ActivationProofReader interface {
	ReadActivationProof(context.Context, store.DBTX, string) (ActivationProof, error)
}

func (proofs ResourceProofs) Activation(ctx context.Context, query store.DBTX, id string) (ActivationProof, error) {
	if proofs.activations == nil {
		return ActivationProof{}, fmt.Errorf("%w: activation history reader is required", errs.ErrManualRecovery)
	}
	return proofs.activations.ReadActivationProof(ctx, query, id)
}
