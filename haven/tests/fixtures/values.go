package fixtures

import (
	"database/sql"
	"fmt"
)

func RequireWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("required responsibility write has no result")
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("required responsibility write affected %d rows", count)
	}
	return nil
}

func Terminal(status string) bool {
	return status == "succeeded" || status == "failed"
}
