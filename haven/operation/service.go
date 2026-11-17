package operation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
)

// WorkerReader observes process-local completion without granting dispatch or
// shutdown authority. Durable status remains owned by the operation repository.
type WorkerReader interface {
	Snapshot(string) execution.WorkerState
}

// Operations authorizes operation reads and waits for accepted work to settle.
// It does not submit effects or own worker lifetime. Terminal business views are
// read from immutable publications, with capabilities derived only on delivery.
type Operations struct {
	database   *store.Database
	repository *Repository
	workers    WorkerReader
	nodeID     string
}

func NewOperations(database *store.Database, workers WorkerReader, nodeID string) *Operations {
	return &Operations{
		database:   database,
		repository: New(database),
		workers:    workers,
		nodeID:     nodeID,
	}
}

func (s *Operations) GetOperation(ctx context.Context, tenant, id string) (Record, error) {
	return s.repository.Get(ctx, "tenant", tenant, id)
}

func (s *Operations) GetManagementOperation(ctx context.Context, principal authorization.Principal, id string) (Record, error) {
	if !principal.Root || principal.SandboxID != "" {
		return Record{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return s.repository.Get(ctx, "node", s.nodeID, id)
}

func (s *Operations) Operation(ctx context.Context, principal authorization.Principal, id string) (Record, error) {
	record, _, err := s.authorized(ctx, principal, id)
	return record, err
}

// authorized resolves root reads to the immutable tenant scope and preserves
// root-only command credentials and sandbox capability confinement.
func (s *Operations) authorized(ctx context.Context, principal authorization.Principal, id string) (Record, string, error) {
	tenant := principal.TenantID
	if principal.Root {
		var err error
		tenant, err = s.repository.TenantScope(ctx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
		}
		if err != nil {
			return Record{}, "", err
		}
	}
	record, err := s.GetOperation(ctx, tenant, id)
	if err != nil {
		return record, "", err
	}
	mac, lookup := s.repository.CredentialMAC(ctx, id)
	if lookup == nil && strings.HasPrefix(mac, "root:") && !principal.Root {
		return Record{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return Record{}, "", lookup
	}
	if principal.SandboxID != "" && record.Result.SandboxID != principal.SandboxID {
		return Record{}, "", fmt.Errorf("%w: operation not found", errs.ErrNotFound)
	}
	return record, tenant, nil
}

func (s *Operations) Wait(ctx context.Context, principal authorization.Principal, id string) (Record, error) {
	_, tenant, err := s.authorized(ctx, principal, id)
	if err != nil {
		return Record{}, err
	}
	return s.wait(ctx, Scope{
		Kind: "tenant",
		ID:   tenant,
	}, id, principal.SandboxID)
}

// WaitAccepted waits for an already accepted tenant operation during workflow
// orchestration. Its caller must separately verify frozen parent/child authority;
// observing completion here does not grant permission to hand off responsibility.
func (s *Operations) WaitAccepted(ctx context.Context, tenant, id string) (Record, error) {
	return s.wait(ctx, Scope{
		Kind: "tenant",
		ID:   tenant,
	}, id, "")
}

func (s *Operations) WaitManagement(ctx context.Context, principal authorization.Principal, id string) (Record, error) {
	if !principal.Root || principal.SandboxID != "" {
		return Record{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	return s.wait(ctx, Scope{
		Kind: "node",
		ID:   s.nodeID,
	}, id, "")
}

func (s *Operations) wait(ctx context.Context, scope Scope, id, sandbox string) (Record, error) {
	for {
		record, err := s.repository.Get(ctx, scope.Kind, scope.ID, id)
		if err != nil {
			return record, err
		}
		if sandbox != "" && record.Result.SandboxID != sandbox {
			return Record{}, fmt.Errorf("%w: operation not found", errs.ErrNotFound)
		}
		worker := s.workers.Snapshot(id)
		terminal := record.Status == "succeeded" || record.Status == "failed"
		// Synchronous delivery waits for the worker to release its workflow
		// guard and finish the durable HEAD update before follow-up admission.
		if terminal && !worker.Active {
			if record.Status == "failed" {
				return record, fmt.Errorf("%w: sandbox operation failed", errs.ErrConflict)
			}
			return record, nil
		}
		if !terminal && !worker.Active && worker.Err != nil {
			return record, worker.Err
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return record, ctx.Err()
		case <-timer.C:
		}
	}
}
