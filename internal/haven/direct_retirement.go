package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func retireDirectGenerations(ctx context.Context, tx *sql.Tx, generations []resourceStep, retirement string) error {
	if len(generations) == 0 {
		return fmt.Errorf("direct retirement lacks known generations")
	}
	rows, err := tx.QueryContext(ctx, "SELECT operation_id FROM operation_steps WHERE step_key='direct_retirement' AND candidate_resource_id=? AND status IN ('pending','running')", generations[0].Candidate)
	if err != nil {
		return err
	}
	var operations []string
	for rows.Next() {
		var operation string
		if err = rows.Scan(&operation); err != nil {
			break
		}
		operations = append(operations, operation)
	}
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, operation := range operations {
		step, err := loadDirectRetirement(ctx, tx, operation)
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
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='direct_retirement' AND input_hash=? AND body=? AND status IN ('pending','running')", encode(directRetirementState{
			Phase:     "reclaimed",
			RetiredBy: retirement,
		}), operation, step.Hash, encode(step.State))); err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(ctx, "UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND resource_kind='direct_policy' AND status IN ('armed','pending')", "direct_policy:"+operation, operation)); err != nil {
			return err
		}
	}
	return nil
}
