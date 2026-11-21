package testfixture

import (
	"context"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// StepObservation exposes raw durable bytes for adversarial SQLite assertions;
// production loaders remain responsible for decoding and validating their owner.
type StepObservation struct {
	Hash             string
	Input            []byte
	Body             []byte
	Status           string
	Candidate        string
	ExpectedInstance string
}

func ObserveStep(ctx context.Context, query store.DBTX, id, key string) (StepObservation, error) {
	var step StepObservation
	err := query.QueryRowContext(ctx, `SELECT input_hash,input,body,status,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key=?`, id, key).Scan(&step.Hash, &step.Input, &step.Body, &step.Status, &step.Candidate, &step.ExpectedInstance)
	if err != nil {
		return StepObservation{}, err
	}
	return step, nil
}

// InsertStep seeds a private fixture with exact frozen bytes; production owners
// admit their own intent through private generated queries.
func InsertStep(ctx context.Context, query store.DBTX, step operation.Step) error {
	result, err := query.ExecContext(ctx, `INSERT INTO operation_steps(operation_id,step_key,action,input_hash,input,candidate_resource_id,expected_instance,status,body) VALUES(?,?,?,?,?,?,?,?,?)`, step.OperationID, step.Key, step.Action, step.Hash, step.Input, step.Candidate, step.ExpectedInstance, step.Status, step.Body)
	if err != nil {
		return err
	}
	return store.RequireOne(result.RowsAffected())
}
