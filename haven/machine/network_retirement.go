package machine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func RetireVMPolicy(ctx context.Context, tx *sql.Tx, generations []ResourceStep, retirement string, proofs ResourceProofs) error {
	if len(generations) == 0 {
		return fmt.Errorf("policy retirement lacks resource generations")
	}
	if err := retireDirectGenerations(ctx, tx, generations, retirement); err != nil {
		return err
	}
	vmID := generations[0].Candidate
	instance, operationID, err := policyOwner(ctx, tx, vmID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	activation, err := proofs.Activation(ctx, tx, operationID)
	if err != nil {
		return err
	}
	known := false
	for _, generation := range generations {
		if generation.Candidate == vmID && generation.Input.Resource.Instance == instance && activation.ExpectedInstance == instance && activation.ResourceOperation == generation.OperationID && activation.ResourceHash == generation.Hash {
			known = true
		}
	}
	if !known {
		return fmt.Errorf("%w: network policy retirement lacks exact historical instance", errs.ErrManualRecovery)
	}
	return store.RequireOne(dbgen.New(tx).DeleteNetworkPolicy(ctx, dbgen.DeleteNetworkPolicyParams{
		VmID:        vmID,
		Instance:    instance,
		OperationID: operationID,
	}))
}
