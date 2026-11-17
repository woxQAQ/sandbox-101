package machine

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func retireDirectGenerations(ctx context.Context, tx *sql.Tx, generations []ResourceStep, retirement string) error {
	if len(generations) == 0 {
		return fmt.Errorf("direct retirement lacks known generations")
	}

	ids, err := dbgen.New(tx).ListLiveDirectRetirements(ctx, generations[0].Candidate)
	if err != nil {
		return err
	}
	for _, id := range ids {
		step, err := LoadDirectRetirement(ctx, tx, id)
		if err != nil {
			return err
		}
		known := false
		for _, generation := range generations {
			if generation.OperationID == step.Input.Source.OperationID && generation.Hash == step.Input.Source.Hash && generation.State.Witness == step.Input.Source.State.Witness {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("direct retirement source generation lacks reclamation proof")
		}
		if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(ctx, dbgen.CompleteUnchangedStepParams{
			OperationID:  id,
			StepKey:      "direct_retirement",
			InputHash:    step.Hash,
			PreviousBody: operation.Encode(step.State),
			NewBody: operation.Encode(DirectRetirementState{
				Phase:     "reclaimed",
				RetiredBy: retirement,
			}),
		})); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).CompleteLiveDirectCleanup(ctx, dbgen.CompleteLiveDirectCleanupParams{
			ID:          "direct_policy:" + id,
			OperationID: id,
		})); err != nil {
			return err
		}
	}
	return nil
}
