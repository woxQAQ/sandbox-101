package haven

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"errors"
	"time"

	"github.com/woxQAQ/haven/loom"
)

// Audit commits the proxy's sanitized event before credential substitution.
func (service *Service) Audit(ctx context.Context, event any) error {
	if err := service.beginDataCall(); err != nil {
		return err
	}
	defer service.wg.Done()
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	var previous string
	if err = tx.QueryRowContext(ctx, "SELECT hash FROM journal ORDER BY seq DESC LIMIT 1").Scan(&previous); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	body := encode(struct {
		Time  time.Time `json:"time"`
		Event any       `json:"event"`
	}{time.Now().UTC(), event})
	if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO journal(body,hash) VALUES(?,?)", body, service.store.sign("journal-v1", previous, string(body)))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (state *store) validateJournal(database *sql.DB) error {
	rows, err := database.Query("SELECT body,hash FROM journal ORDER BY seq")
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()
	previous := ""
	for rows.Next() {
		var body []byte
		var hash string
		if err = rows.Scan(&body, &hash); err != nil {
			return err
		}
		if !hmac.Equal([]byte(hash), []byte(state.sign("journal-v1", previous, string(body)))) {
			return loom.Fail(loom.ManualRecovery, "audit journal identity changed")
		}
		previous = hash
	}
	return rows.Err()
}
