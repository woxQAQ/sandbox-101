package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
)

// ProvisionOwnershipReader checks the retained resource and sandbox intent in
// the caller's transaction. It never changes business or physical state.
type ProvisionOwnershipReader interface {
	VerifyProvisionIntent(ResourceProofs, context.Context, *sql.Tx, ResourceStep) (VMRecord, ownership.Sandbox, operation.Result, error)
}

// ProvisionParticipant joins physical progress without committing, checkpointing,
// dispatching effects, or obtaining a workflow lock.
type ProvisionParticipant interface {
	PublishInstance(context.Context, *sql.Tx, ownership.Sandbox, string) error
	BlockActivation(context.Context, *sql.Tx, string) error
}
