// Package authorization resolves bearer tokens into root, tenant, or sandbox
// principals. It manages tenant credentials and admission budgets. Callers use
// the authenticated scope when authorizing access to a particular resource.
package authorization

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/woxQAQ/haven/authorization/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Principal identifies the authority established by authentication. Root grants
// node-wide scope. A tenant principal is scoped to TenantID, and a nonempty
// SandboxID further restricts that scope to a single sandbox.
type Principal struct {
	TenantID  string
	SandboxID string
	Root      bool
}

// Visible reports whether the tenant and sandbox fall within the principal's
// scope. Data access also requires lease, settings, and resource checks.
func (p Principal) Visible(tenant, sandbox string) bool {
	return (p.Root || p.TenantID == tenant) && (p.SandboxID == "" || p.SandboxID == sandbox)
}

// Repository reads and writes tenant token hashes and admission limits through
// the database or transaction supplied to New.
type Repository struct {
	queries *dbgen.Queries
}

func New(db store.DBTX) *Repository {
	return &Repository{
		queries: dbgen.New(db),
	}
}

// Limits is the persisted tenant admission budget, without bearer credentials.
type Limits struct {
	MaxSandboxes  int64 `json:"max_sandboxes"`
	MaxStorageMiB int64 `json:"max_storage_mib"`
}

func (r *Repository) Limits(ctx context.Context, id string) (Limits, error) {
	body, err := r.queries.GetTenantLimits(ctx, id)
	if err != nil {
		return Limits{}, err
	}
	var limits Limits
	err = json.Unmarshal(body, &limits)
	return limits, err
}

func (r *Repository) PutTenant(ctx context.Context, id, token string, limits Limits) error {
	body, err := json.Marshal(limits)
	if err != nil {
		return err
	}
	return store.RequireOne(r.queries.PutTenant(ctx, dbgen.PutTenantParams{
		ID:        id,
		TokenHash: tokenHash(token),
		Body:      body,
	}))
}

func tokenHash(token string) string {
	hash := sha256.Sum256([]byte(token))
	return hex.EncodeToString(hash[:])
}

func (r *Repository) TenantForToken(ctx context.Context, token string) (string, error) {
	return r.queries.TenantForToken(ctx, tokenHash(token))
}
