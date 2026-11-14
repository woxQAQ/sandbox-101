package haven

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

// statementDB reuses fixed read statements within this writer's lifetime. The
// bound excludes request-dependent SQL growth; transactions retain their own
// connection and statement lifetime. SQLite re-prepares on schema changes.
type statementDB struct {
	*sql.DB

	mu         sync.Mutex
	statements map[string]*sql.Stmt
	closed     bool
}

const maxReadStatements = 256

func (database *statementDB) readStatement(ctx context.Context, query string) *sql.Stmt {
	database.mu.Lock()
	defer database.mu.Unlock()
	if database.closed {
		return nil
	}
	if statement := database.statements[query]; statement != nil {
		return statement
	}
	if len(database.statements) >= maxReadStatements {
		return nil
	}
	statement, err := database.PrepareContext(ctx, query)
	if err != nil {
		return nil
	}
	database.statements[query] = statement
	return statement
}

func (database *statementDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if statement := database.readStatement(ctx, query); statement != nil {
		return statement.QueryRowContext(ctx, args...)
	}
	return database.DB.QueryRowContext(ctx, query, args...)
}

func (database *statementDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if statement := database.readStatement(ctx, query); statement != nil {
		return statement.QueryContext(ctx, args...)
	}
	return database.DB.QueryContext(ctx, query, args...)
}

func (database *statementDB) Close() error {
	database.mu.Lock()
	database.closed = true
	var failures []error
	for _, statement := range database.statements {
		if err := statement.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	database.statements = nil
	database.mu.Unlock()
	return errors.Join(append(failures, database.DB.Close())...)
}
