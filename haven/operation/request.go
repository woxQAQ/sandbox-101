package operation

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// ResourceRequest identifies the original allocation, predecessor or cleanup target.
// TenantID and Key come from the operation's immutable scope and idempotency key.
type ResourceRequest struct {
	TenantID          string
	Key               string
	TemplateID        string
	SandboxID         string
	SnapshotID        string
	ExpectedInstance  string
	StopOperationID   string
	SourceOperationID string
	CacheBuilder      string
	CacheDiffID       string
}

// Request is the admitted intent. A resource admission can also carry the tenant
// command that caused it, for example a claim served by cloning a golden snapshot.
type Request struct {
	Action   string
	Resource *ResourceRequest
	Command  *Intent
	Import   *imageimport.ImageImportOptions
	GC       *types.GCOptions
}

func (r Request) kind() (string, error) {
	switch {
	case r.Import != nil && r.Action == "image_import" && r.GC == nil && r.Resource == nil && r.Command == nil:
		return "image_import", nil
	case r.GC != nil && r.Action == "gc" && r.Import == nil && r.Resource == nil && r.Command == nil:
		return "gc", nil
	case r.Import == nil && r.GC == nil && r.Action != "":
		if r.Resource != nil {
			if r.Command != nil {
				return "resource_command", nil
			}
			return "resource", nil
		}
		if r.Command != nil && r.Command.Action == r.Action {
			return "command", nil
		}
	}
	return "", fmt.Errorf("inconsistent request for action %q", r.Action)
}

func (r Request) Equal(other Request) bool {
	if r.Command != nil && other.Command != nil {
		if !r.Command.equal(*other.Command) {
			return false
		}
		r.Command = nil
		other.Command = nil
	}
	return reflect.DeepEqual(r, other)
}

func (r Request) ResourceFor(action string) (ResourceRequest, error) {
	if r.Action != action || r.Resource == nil {
		return ResourceRequest{}, fmt.Errorf("expected %s resource request, got %s", action, r.Action)
	}
	return *r.Resource, nil
}

func insertRequest(ctx context.Context, q *dbgen.Queries, a Admission) error {
	r := a.Request
	if s := r.Resource; s != nil {
		if a.ScopeKind == ScopeTenant && (s.TenantID != a.ScopeID || s.Key != a.Key) {
			return fmt.Errorf("resource request scope or key changed")
		}
		if a.ScopeKind != ScopeTenant && (s.TenantID != "" || s.Key != "") {
			return fmt.Errorf("node request has tenant identity")
		}
		if err := store.RequireOne(q.InsertRequestResource(ctx, dbgen.InsertRequestResourceParams{
			OperationID:       a.ID,
			TemplateID:        s.TemplateID,
			SandboxID:         s.SandboxID,
			SnapshotID:        s.SnapshotID,
			ExpectedInstance:  s.ExpectedInstance,
			StopOperationID:   s.StopOperationID,
			SourceOperationID: s.SourceOperationID,
			CacheBuilder:      s.CacheBuilder,
			CacheDiffID:       s.CacheDiffID,
		})); err != nil {
			return err
		}
	}
	if c := r.Command; c != nil {
		return insertCommand(ctx, q, a.ID, *c, r.Resource != nil)
	}
	if o := r.Import; o != nil {
		return store.RequireOne(q.InsertRequestImport(ctx, dbgen.InsertRequestImportParams{
			OperationID:     a.ID,
			Layout:          o.Layout,
			Digest:          o.Digest,
			Materialization: o.Materialization,
			MaxBytes:        o.MaxBytes,
			MaxFiles:        o.MaxFiles,
		}))
	}
	if o := r.GC; o != nil {
		return store.RequireOne(q.InsertRequestGC(ctx, dbgen.InsertRequestGCParams{
			OperationID: a.ID,
			DryRun:      o.DryRun,
			Batch:       int64(o.Batch),
			BudgetNs:    int64(o.Budget),
		}))
	}
	return nil
}

func readRequest(ctx context.Context, q *dbgen.Queries, row dbgen.Operation) (Request, error) {
	r := Request{
		Action: row.Action,
	}
	switch row.RequestKind {
	case "resource", "resource_command":
		s, err := q.GetRequestResource(ctx, row.ID)
		if err != nil {
			return r, err
		}
		r.Resource = &ResourceRequest{
			TemplateID:        s.TemplateID,
			SandboxID:         s.SandboxID,
			SnapshotID:        s.SnapshotID,
			ExpectedInstance:  s.ExpectedInstance,
			StopOperationID:   s.StopOperationID,
			SourceOperationID: s.SourceOperationID,
			CacheBuilder:      s.CacheBuilder,
			CacheDiffID:       s.CacheDiffID,
		}
		if row.ScopeKind == ScopeTenant {
			r.Resource.TenantID = row.ScopeID
			r.Resource.Key = row.IdemKey
		}
	case "image_import":
		o, err := q.GetRequestImport(ctx, row.ID)
		if err != nil {
			return r, err
		}
		r.Import = &imageimport.ImageImportOptions{
			Layout:          o.Layout,
			Digest:          o.Digest,
			Materialization: o.Materialization,
			MaxBytes:        o.MaxBytes,
			MaxFiles:        o.MaxFiles,
		}
	case "gc":
		o, err := q.GetRequestGC(ctx, row.ID)
		if err != nil {
			return r, err
		}
		r.GC = &types.GCOptions{
			DryRun: o.DryRun,
			Batch:  int(o.Batch),
			Budget: time.Duration(o.BudgetNs),
		}
	case "command":
	default:
		return r, fmt.Errorf("unknown request kind %q", row.RequestKind)
	}
	if row.RequestKind == "command" || row.RequestKind == "resource_command" {
		c, err := readCommand(ctx, q, row)
		if err != nil {
			return r, err
		}
		r.Command = &c
	}
	_, err := r.kind()
	return r, err
}
