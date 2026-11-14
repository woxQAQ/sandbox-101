package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/types"
)

// commitPublications freezes completed operation results in the workflow's own
// transaction, then commits both together. No HTTP error or bearer token is stored.
func commitPublications(tx *sql.Tx) error {
	ctx := context.Background()
	rows, err := tx.QueryContext(ctx, `SELECT o.id FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL AND (EXISTS(SELECT 1 FROM operation_credentials c WHERE c.operation_id=o.id) OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate')) ORDER BY o.rowid`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for _, id := range ids {
		view, err := projectOperation(ctx, tx, id)
		if err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_publications VALUES(?,?)", id, encode(view))); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func loadPublication(ctx context.Context, q queryer, id string) (types.Operation, error) {
	var view types.Operation
	var body []byte
	if err := q.QueryRowContext(ctx, "SELECT body FROM operation_publications WHERE operation_id=?", id).Scan(&body); err != nil {
		return view, err
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if view.ID != id || view.Result.OperationID != id || !terminal(view.Status) || view.Result.Tokens != nil || view.Error != nil {
		return view, fmt.Errorf("%w: frozen delivery identity or transport fields changed", ErrManualRecovery)
	}
	return view, nil
}
