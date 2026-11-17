package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// AdmitProvisionIntent freezes the physical intent before the coordinator's
// sandbox and quota writes. It borrows the existing admission transaction.
func AdmitProvisionIntent(ctx context.Context, tx *sql.Tx, id, action, expectedInstance string, input ResourceInput) error {
	payload := operation.Encode(input)
	return store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         id,
		StepKey:             "resource",
		Action:              action,
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: input.Resource.VMID,
		ExpectedInstance:    expectedInstance,
		Status:              "pending",
		Body:                operation.Encode(ResourceState{Phase: "admitted"}),
	}))
}
