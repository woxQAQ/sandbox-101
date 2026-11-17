package machine

import (
	"context"
	"database/sql"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// ProvisionIntent fixes the generation candidate and predecessor before any
// ownership or cost transfer. ExpectedInstance is empty for a new logical VM.
type ProvisionIntent struct {
	OperationID      string
	Action           string
	ExpectedInstance string
	Input            ResourceInput
}

// AdmitProvisionIntent freezes the physical intent before the coordinator's
// sandbox and quota writes. It borrows the existing admission transaction.
func AdmitProvisionIntent(ctx context.Context, tx *sql.Tx, intent ProvisionIntent) error {
	payload := operation.Encode(intent.Input)
	return store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         intent.OperationID,
		StepKey:             "resource",
		Action:              intent.Action,
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: intent.Input.Resource.VMID,
		ExpectedInstance:    intent.ExpectedInstance,
		Status:              "pending",
		Body: operation.Encode(ResourceState{
			Phase: "admitted",
		}),
	}))
}
