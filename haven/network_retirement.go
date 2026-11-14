package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func retireVMPolicy(ctx context.Context, tx *sql.Tx, generations []resourceStep, retirement string) error {
	if len(generations) == 0 {
		return fmt.Errorf("policy retirement lacks resource generations")
	}
	if err := retireDirectGenerations(ctx, tx, generations, retirement); err != nil {
		return err
	}
	vm := generations[0].Candidate
	var instance, operation string
	err := tx.QueryRowContext(ctx, "SELECT instance,operation_id FROM network_policies WHERE vm_id=?", vm).Scan(&instance, &operation)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	activation, err := loadActivation(ctx, tx, operation)
	if err != nil {
		return err
	}
	known := false
	for _, generation := range generations {
		if generation.Candidate == vm && generation.Input.Resource.Instance == instance && activation.Input.ExpectedInstance == instance && activation.Input.ResourceOperation == generation.OperationID && activation.Input.ResourceHash == generation.Hash {
			known = true
		}
	}
	if !known {
		return fmt.Errorf("%w: network policy retirement lacks exact historical instance", ErrManualRecovery)
	}
	return requireWrite(tx.ExecContext(ctx, "DELETE FROM network_policies WHERE vm_id=? AND instance=? AND operation_id=?", vm, instance, operation))
}
