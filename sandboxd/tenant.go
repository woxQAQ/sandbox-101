package sandboxd

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/woxQAQ/sandbox-engine/api"
)

// Quota has exactly one source: the durable resource table. Root bypasses the
// limit comparison only; the same admission transaction still records costs.
func (s *Service) quota(
	ctx context.Context,
	tx *sql.Tx,
	p Principal,
	count int,
	storage int64,
) error {
	if p.Root {
		return nil
	}
	var raw []byte
	if err := tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", p.TenantID).
		Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return api.Fail(api.NotFound, "not found")
		}
		return err
	}
	var t Tenant
	if err := json.Unmarshal(raw, &t); err != nil {
		return err
	}
	var slots, used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?`, p.TenantID).
		Scan(&slots, &used); err != nil {
		return err
	}
	if slots+int64(count) > int64(t.MaxSandboxes) ||
		used+storage > t.MaxStorageMiB {
		return api.Fail(api.Exhausted, "tenant quota exceeded")
	}
	return nil
}

func (s *Service) PutTenant(
	ctx context.Context,
	p Principal,
	t Tenant,
) (Tenant, error) {
	if !p.Root {
		return t, api.Fail(api.Conflict, "root access required")
	}
	if t.ID == "" || t.MaxSandboxes < 1 || t.MaxStorageMiB < 1 {
		return t, api.Fail(
			api.Invalid,
			"tenant id and positive quotas required",
		)
	}
	if t.Token == "" {
		t.Token = newID("tenant_")
	}
	disk := t
	disk.Token = ""
	_, err := s.store.db.ExecContext(
		ctx,
		"INSERT INTO tenants(id,token_hash,body) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,body=excluded.body",
		t.ID,
		hashToken(t.Token),
		encode(disk),
	)
	return t, err
}

// Tenant defines an owner's sandbox and storage budgets. Token is returned when
// configured and is omitted from the persisted tenant record.
type Tenant struct {
	ID            string `json:"id"`
	MaxSandboxes  int    `json:"max_sandboxes"`
	MaxStorageMiB int64  `json:"max_storage_mib"`
	Token         string `json:"token,omitempty"`
}
