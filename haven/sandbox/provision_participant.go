package sandbox

import (
	"context"
	"database/sql"
	"errors"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// ProvisionHistory retains no connection or running service.
type ProvisionHistory struct{}

func (ProvisionHistory) VerifyProvisionIntent(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, step machine.ResourceStep) (machine.VMRecord, ownership.Sandbox, operation.Result, error) {
	return VerifyProvisionIntent(proofs, ctx, tx, step)
}

// ProvisionHandoff writes only through the transaction owned by the resource
// workflow; activation delivery remains under the coordinator's VM guard.
type ProvisionHandoff struct{}

func (ProvisionHandoff) PublishInstance(ctx context.Context, tx *sql.Tx, record ownership.Sandbox, instance string) error {
	record.Instance = instance
	return store.RequireOne(dbgen.New(tx).UpdateProvisioningSandboxBody(ctx, dbgen.UpdateProvisioningSandboxBodyParams{
		Body:   operation.Encode(record),
		ID:     record.ID,
		Tenant: record.TenantID,
	}))
}

func (ProvisionHandoff) BlockActivation(ctx context.Context, tx *sql.Tx, id string) error {
	if activation, lookup := LoadActivation(ctx, tx, id); lookup == nil && !operation.Terminal(activation.Status) {
		if err := store.RequireOne(dbgen.New(tx).FailLiveStep(ctx, dbgen.FailLiveStepParams{
			OperationID: id,
			StepKey:     activation.Key,
			InputHash:   activation.Hash,
			Body:        operation.Encode("blocked"),
		})); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	return nil
}
