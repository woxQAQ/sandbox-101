package engine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) GetOperation(
	ctx context.Context,
	id string,
) (api.Operation, error) {
	o, err := e.loadOp(ctx, id)
	return o.Operation, err
}

func (e *Engine) FindOperation(
	ctx context.Context,
	key string,
) (api.Operation, error) {
	var id string
	err := e.db.QueryRowContext(ctx, "SELECT id FROM operations WHERE key=?", key).
		Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return api.Operation{}, api.Fail(
			api.NotFound,
			"idempotency key not found",
		)
	}
	if err != nil {
		return api.Operation{}, err
	}
	return e.GetOperation(ctx, id)
}

func (e *Engine) GetVM(ctx context.Context, id string) (api.VM, error) {
	v, err := e.loadVM(ctx, id)
	return v.VM, err
}

func (e *Engine) GetSnapshot(
	ctx context.Context,
	id string,
) (api.Snapshot, error) {
	s, err := e.loadSnapshot(ctx, id)
	return s.Snapshot, err
}

func (e *Engine) ListVMs(ctx context.Context) ([]api.VM, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT body FROM vms ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []api.VM{}
	for rows.Next() {
		var b []byte
		var v vmRecord
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &v); err != nil {
			return nil, err
		}
		out = append(out, v.VM)
	}
	return out, rows.Err()
}

func (e *Engine) ListSnapshots(ctx context.Context) ([]api.Snapshot, error) {
	rows, err := e.db.QueryContext(
		ctx,
		"SELECT body FROM snapshots ORDER BY id",
	)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []api.Snapshot{}
	for rows.Next() {
		var b []byte
		var s snapshotRecord
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &s); err != nil {
			return nil, err
		}
		out = append(out, s.Snapshot)
	}
	return out, rows.Err()
}
