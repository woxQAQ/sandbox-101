package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/store"
)

// ActivationHistory is a stateless proof reader, usable with the read-only
// startup probe before application services or workers exist.
type ActivationHistory struct{}

func (ActivationHistory) ReadActivationProof(ctx context.Context, query store.DBTX, id string) (machine.ActivationProof, error) {
	step, err := LoadActivation(ctx, query, id)
	if err != nil {
		return machine.ActivationProof{}, err
	}
	return step.ResourceProof(), nil
}

// ResourceProof projects an already decoded frozen step without consulting
// current sandbox ownership or deriving a replacement resource witness.
func (step ActivationStep) ResourceProof() machine.ActivationProof {
	return machine.ActivationProof{
		OperationID:       step.OperationID,
		Key:               step.Key,
		Hash:              step.Hash,
		Status:            step.Status,
		Phase:             step.Phase,
		ResourceOperation: step.Input.ResourceOperation,
		ResourceHash:      step.Input.ResourceHash,
		ExpectedInstance:  step.Input.ExpectedInstance,
	}
}
