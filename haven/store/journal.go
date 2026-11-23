package store

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store/internal/dbgen"
)

// Sign binds length-delimited fields to this store's node key. The key remains
// owned by State and is never exposed to application or domain packages.
func (state *State) Sign(parts ...string) string {
	mac := hmac.New(sha256.New, state.key)
	for _, part := range parts {
		_, _ = fmt.Fprintf(mac, "%d:", len(part))
		_, _ = mac.Write([]byte(part))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

// AppendAudit durably appends a sanitized event before credential substitution.
// The caller holds the application admission guard and shutdown custody.
func (state *State) AppendAudit(ctx context.Context, body []byte) error {
	tx, err := state.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	queries := dbgen.New(state.db).WithTx(tx)
	previous, err := queries.GetJournalTail(ctx)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = RequireOne(queries.AppendJournal(ctx, dbgen.AppendJournalParams{
		SignedPayload: body,
		Hash:          state.Sign("journal-v1", previous, string(body)),
	})); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return state.Checkpoint()
}

func (state *State) validateJournal(database *sql.DB) error {
	rows, err := dbgen.New(database).ListJournal(context.Background())
	if err != nil {
		return err
	}
	previous := ""
	for _, row := range rows {
		if !hmac.Equal([]byte(row.Hash), []byte(state.Sign("journal-v1", previous, string(row.SignedPayload)))) {
			return fmt.Errorf("%w: audit journal identity changed", errs.ErrManualRecovery)
		}
		previous = row.Hash
	}
	return nil
}
