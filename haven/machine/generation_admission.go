package machine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// AvailableNetworkSlot selects within the caller's admission snapshot. The
// candidate is inserted in that same transaction before responsibility passes.
func AvailableNetworkSlot(ctx context.Context, tx *sql.Tx) (int, error) {
	allocatedSlots, err := dbgen.New(tx).ListSlots(ctx)
	if err != nil {
		return 0, err
	}
	slot := 1
	for _, allocated := range allocatedSlots {
		if int64(slot) == allocated {
			slot++
		}
	}
	if slot >= 32768 {
		return 0, fmt.Errorf("%w: network address pool exhausted", errs.ErrExhausted)
	}
	return slot, nil
}

// AdmitCandidate records the physical candidate and its original generation
// intent. The caller retains the transaction, quota admission and commit.
func AdmitCandidate(ctx context.Context, tx *sql.Tx, owner string, record VMRecord) error {
	if err := store.RequireOne(dbgen.New(tx).CreateCandidate(ctx, dbgen.CreateCandidateParams{
		ID:   record.Resource.VMID,
		Slot: int64(record.Resource.Slot),
		OperationID: sql.NullString{
			String: owner,
			Valid:  true,
		},
		Body: operation.Encode(record),
	})); err != nil {
		return err
	}
	return RetainGenerationIntent(ctx, tx, owner, record)
}

// RetainGenerationIntent records the instance before its version. Restore calls
// this after claiming the stopped VM and updating sandbox ownership; those
// intervening business writes remain in the caller's transaction.
func RetainGenerationIntent(ctx context.Context, tx *sql.Tx, owner string, record VMRecord) error {
	queries := dbgen.New(tx)
	if err := store.RequireOne(queries.InsertInstance(ctx, dbgen.InsertInstanceParams{
		ID:          record.Resource.Instance,
		VmID:        record.Resource.VMID,
		OperationID: owner,
		Body:        operation.Encode(record),
	})); err != nil {
		return err
	}
	return store.RequireOne(queries.InsertVersion(ctx, dbgen.InsertVersionParams{
		ID:          owner,
		VmID:        record.Resource.VMID,
		OperationID: owner,
		StepKey:     "resource",
		Body: operation.Encode(RetainedVersion{
			Resource: record.Resource,
		}),
	}))
}
