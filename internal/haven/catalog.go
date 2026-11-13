package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/loom"
)

func errorsJoinRows(err error, rows *sql.Rows) error {
	return errors.Join(err, rows.Err(), rows.Close())
}

func (service *Service) List(ctx context.Context, principal Principal) ([]api.Sandbox, error) {
	if principal.SandboxID != "" {
		return nil, loom.Fail(loom.Conflict, "tenant access required")
	}
	rows, err := service.store.db.QueryContext(ctx, "SELECT id,tenant FROM sandboxes ORDER BY id")
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id, owner string
		if err = rows.Scan(&id, &owner); err != nil {
			break
		}
		if principal.Root || owner == principal.TenantID {
			ids = append(ids, id)
		}
	}
	if err = errorsJoinRows(err, rows); err != nil {
		return nil, err
	}
	out := make([]api.Sandbox, 0, len(ids))
	for _, id := range ids {
		value, err := service.Get(ctx, principal, id)
		if err != nil {
			return nil, err
		}
		out = append(out, wireSandbox(value))
	}
	return out, nil
}

func (service *Service) Templates(ctx context.Context, principal Principal) ([]api.Template, error) {
	if principal.SandboxID != "" {
		return nil, loom.Fail(loom.Conflict, "tenant access required")
	}
	rows, err := service.store.db.QueryContext(ctx, "SELECT id,tenant FROM templates ORDER BY id")
	if err != nil {
		return nil, err
	}
	type owned struct{ id, owner string }
	ids := []owned{}
	for rows.Next() {
		var item owned
		if err = rows.Scan(&item.id, &item.owner); err != nil {
			break
		}
		if principal.Root || item.owner == principal.TenantID {
			ids = append(ids, item)
		}
	}
	if err = errorsJoinRows(err, rows); err != nil {
		return nil, err
	}
	out := make([]api.Template, 0, len(ids))
	for _, item := range ids {
		value, err := loadOwnedTemplate(ctx, service.store.db, item.owner, item.id)
		if err != nil {
			return nil, err
		}
		out = append(out, wireTemplate(value))
	}
	return out, nil
}

func (service *Service) Checkpoints(ctx context.Context, principal Principal) ([]api.Checkpoint, error) {
	if principal.SandboxID != "" {
		return nil, loom.Fail(loom.Conflict, "tenant access required")
	}
	rows, err := service.store.db.QueryContext(ctx, "SELECT body FROM checkpoint_bindings ORDER BY id")
	if err != nil {
		return nil, err
	}
	out := []api.Checkpoint{}
	for rows.Next() {
		var body []byte
		var value Checkpoint
		if err = rows.Scan(&body); err != nil {
			break
		}
		if err = json.Unmarshal(body, &value); err != nil {
			break
		}
		if principal.Root || value.TenantID == principal.TenantID {
			out = append(out, wireCheckpoint(value))
		}
	}
	return out, errorsJoinRows(err, rows)
}
