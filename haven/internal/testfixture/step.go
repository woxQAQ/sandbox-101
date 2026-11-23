package testfixture

import (
	"context"

	"github.com/woxQAQ/haven/store"
)

// StepObservation exposes raw durable bytes for adversarial SQLite assertions;
// production loaders remain responsible for decoding and validating their owner.
type StepObservation struct {
	Hash             string
	Input            []byte
	Status           string
	Candidate        string
	ExpectedInstance string
}

func ObserveStep(ctx context.Context, query store.DBTX, id, key string) (StepObservation, error) {
	var step StepObservation
	err := query.QueryRowContext(ctx, `SELECT input_hash,input,status,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key=?`, id, key).Scan(&step.Hash, &step.Input, &step.Status, &step.Candidate, &step.ExpectedInstance)
	if err != nil {
		return StepObservation{}, err
	}
	return step, nil
}
