// Package operation owns durable request identities, steps and publications.
package operation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
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

type Record struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Status string `json:"status"`
	Result Result `json:"result"`
}

// Repository uses either the shared read connection or the caller's transaction.
// It does not decide admission, project business results, or commit transactions.
type Repository struct {
	queries *dbgen.Queries
}

func New(db store.DBTX) *Repository {
	return &Repository{
		queries: dbgen.New(db),
	}
}

func (r *Repository) Get(ctx context.Context, kind, scope, id string) (Record, error) {
	row, err := r.queries.GetScopedOperation(ctx, dbgen.GetScopedOperationParams{
		ID:        id,
		ScopeKind: kind,
		ScopeID:   scope,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	if err != nil {
		return Record{}, err
	}
	out := Record{
		ID:     row.ID,
		Key:    row.IdemKey,
		Status: row.Status,
	}
	hash := sha256.Sum256(row.Request)
	if hex.EncodeToString(hash[:]) != row.RequestHash {
		return out, fmt.Errorf("operation request identity corrupted")
	}
	err = json.Unmarshal(row.Body, &out.Result)
	return out, err
}

func (r *Repository) RequestID(ctx context.Context, tenant, key string) (string, error) {
	return r.queries.GetRequestID(ctx, dbgen.GetRequestIDParams{
		ScopeID: tenant,
		IdemKey: key,
	})
}

// Cached resolves the original scoped request. A matching key alone cannot
// authorize replay: both the stored checksum and the requested hash must match.
func (r *Repository) Cached(ctx context.Context, scope Scope, key, hash string) (Record, bool, error) {
	row, err := r.queries.GetScopedRequest(ctx, dbgen.GetScopedRequestParams{
		ScopeKind: scope.Kind,
		ScopeID:   scope.ID,
		IdemKey:   key,
	})
	record := Record{
		ID:     row.ID,
		Key:    row.IdemKey,
		Status: row.Status,
	}
	if errors.Is(err, sql.ErrNoRows) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	checksum := sha256.Sum256(row.Request)
	if hex.EncodeToString(checksum[:]) != row.RequestHash {
		return record, true, fmt.Errorf("operation request identity corrupted")
	}
	if err = json.Unmarshal(row.Body, &record.Result); err != nil {
		return record, true, err
	}
	if hash != row.RequestHash {
		return record, true, fmt.Errorf("%w: key belongs to a different request", errs.ErrConflict)
	}
	return record, true, nil
}

func (r *Repository) TenantScope(ctx context.Context, id string) (string, error) {
	return r.queries.GetTenantScope(ctx, id)
}

func (r *Repository) TenantCredentialMAC(ctx context.Context, tenant, key string) (string, error) {
	return r.queries.GetTenantCredentialMAC(ctx, dbgen.GetTenantCredentialMACParams{
		ScopeID: tenant,
		IdemKey: key,
	})
}

func (r *Repository) CredentialMAC(ctx context.Context, id string) (string, error) {
	return r.queries.GetCredentialMAC(ctx, id)
}

func (r *Repository) Unfinished(ctx context.Context) ([]string, error) {
	return r.queries.ListUnfinishedOperations(ctx)
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
	ID          string
	Key         string
	ScopeKind   string
	ScopeID     string
	Status      string
	RequestHash string
	Request     []byte
	Body        []byte
}

func (r *Repository) Projection(ctx context.Context, id string) (ProjectionSource, error) {
	row, err := r.queries.GetOperationProjection(ctx, id)
	if err != nil {
		return ProjectionSource{}, err
	}
	return ProjectionSource{
		ID:          row.ID,
		Key:         row.IdemKey,
		ScopeKind:   row.ScopeKind,
		ScopeID:     row.ScopeID,
		Status:      row.Status,
		RequestHash: row.RequestHash,
		Request:     row.Request,
		Body:        row.Body,
	}, nil
}

func (r *Repository) PublicStatus(ctx context.Context, id string) (string, error) {
	return r.queries.GetPublicStatus(ctx, id)
}

func (r *Repository) Publication(ctx context.Context, id string) ([]byte, error) {
	return r.queries.GetPublication(ctx, id)
}

func (r *Repository) UnpublishedTerminals(ctx context.Context) ([]string, error) {
	return r.queries.ListUnpublishedTerminals(ctx)
}

func (r *Repository) Publish(ctx context.Context, id string, body []byte) error {
	return store.RequireOne(r.queries.InsertPublication(ctx, dbgen.InsertPublicationParams{
		OperationID: id,
		Body:        body,
	}))
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
	Body             []byte
}
