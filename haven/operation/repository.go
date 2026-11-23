// Package operation owns durable request identities, steps and publications.
package operation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// Result is the internal frozen workflow result. HTTP errors and bearer tokens
// are derived at delivery time and are never persisted in this value.
type Result struct {
	SandboxIDs    []string `json:"sandbox_ids,omitempty"`
	ImageDigest   string   `json:"image_digest,omitempty"`
	TemplateID    string   `json:"template_id,omitempty"`
	CheckpointID  string   `json:"checkpoint_id,omitempty"`
	SnapshotID    string   `json:"snapshot_id,omitempty"`
	SandboxID     string   `json:"sandbox_id"`
	VMID          string   `json:"vm_id"`
	ResourceReady bool     `json:"resource_ready,omitempty"`
	Error         string   `json:"error,omitempty"`
}

const StatusPending = "pending"

// State is the durable status and workflow result of an accepted operation.
type State struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Status string `json:"status"`
	Result Result `json:"result"`
}

// Repository uses either the shared read connection or the caller's transaction.
// It does not decide admission, project business results, or commit transactions.
type Repository struct {
	database store.DBTX
	queries  *dbgen.Queries
}

func New(db store.DBTX) *Repository {
	return &Repository{
		database: db,
		queries:  dbgen.New(db),
	}
}

func (r *Repository) GetScopedState(ctx context.Context, kind, scope, id string) (State, error) {
	row, err := r.readOperation(ctx, func(q *dbgen.Queries) (dbgen.Operation, error) {
		return q.GetScopedOperation(ctx, dbgen.GetScopedOperationParams{
			ID:        id,
			ScopeKind: kind,
			ScopeID:   scope,
		})
	})
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	if err != nil {
		return State{}, err
	}
	out := State{
		ID:     row.ID,
		Key:    row.Key,
		Status: row.Status,
		Result: row.Result,
	}
	return out, nil
}

func (r *Repository) OperationIDByTenantKey(ctx context.Context, tenant, key string) (string, error) {
	return r.queries.GetOperationIDByTenantKey(ctx, dbgen.GetOperationIDByTenantKeyParams{
		ScopeID: tenant,
		IdemKey: key,
	})
}

// LookupForReplay returns the operation for a scoped key. A parameter mismatch
// returns the existing operation, found=true and ErrConflict.
func (r *Repository) LookupForReplay(ctx context.Context, scope Scope, key string, request Request) (State, bool, error) {
	row, err := r.readOperation(ctx, func(q *dbgen.Queries) (dbgen.Operation, error) {
		return q.GetScopedRequest(ctx, dbgen.GetScopedRequestParams{
			ScopeKind: scope.Kind,
			ScopeID:   scope.ID,
			IdemKey:   key,
		})
	})
	record := State{
		ID:     row.ID,
		Key:    row.Key,
		Status: row.Status,
		Result: row.Result,
	}
	if errors.Is(err, sql.ErrNoRows) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	if !row.Request.Equal(request) {
		return record, true, fmt.Errorf("%w: key belongs to a different request", errs.ErrConflict)
	}
	return record, true, nil
}

func (r *Repository) TenantScope(ctx context.Context, id string) (string, error) {
	return r.queries.GetTenantScope(ctx, id)
}

func (r *Repository) CommandMACByTenantKey(ctx context.Context, tenant, key string) (string, error) {
	return r.queries.GetCommandMACByTenantKey(ctx, dbgen.GetCommandMACByTenantKeyParams{
		ScopeID: tenant,
		IdemKey: key,
	})
}

func (r *Repository) CommandMAC(ctx context.Context, id string) (string, error) {
	return r.queries.GetCommandMAC(ctx, id)
}

func (r *Repository) ListIDsWithUnfinishedSteps(ctx context.Context) ([]string, error) {
	return r.queries.ListIDsWithUnfinishedSteps(ctx)
}

func (r *Repository) Times(ctx context.Context, id string) (time.Time, time.Time, error) {
	row, err := r.queries.GetOperationTimes(ctx, id)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.Parse(time.RFC3339Nano, row.CreatedAt)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	var end time.Time
	if row.FinishedAt != "" {
		end, err = time.Parse(time.RFC3339Nano, row.FinishedAt)
	}
	return start, end, err
}

// ProjectionSource is the original request and result read in the caller's
// snapshot. Application services interpret it to build a business publication.
type ProjectionSource struct {
	ID        string
	Key       string
	ScopeKind string
	ScopeID   string
	Status    string
	Request   Request
	Result    Result
}

func (r *Repository) LoadProjectionSource(ctx context.Context, id string) (ProjectionSource, error) {
	return r.readOperation(ctx, func(q *dbgen.Queries) (dbgen.Operation, error) {
		return q.GetOperationProjection(ctx, id)
	})
}

func (r *Repository) PublicStatus(ctx context.Context, id string) (string, error) {
	return r.queries.GetPublicStatus(ctx, id)
}

// Step retains the exact durable payload. Its owner decodes and verifies the
// workflow-specific proof; current resource observations cannot replace it.
type Step struct {
	OperationID      string
	Key              string
	Action           string
	Status           string
	Hash             string
	Candidate        string
	ExpectedInstance string
	Input            []byte
}
