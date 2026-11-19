package node

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/gc"
	image "github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// NodeMetadata exposes only immutable origin and the current metadata epoch.
type NodeMetadata interface {
	Identity() store.Identity
	Reviewed() bool
	Epoch(context.Context) (int64, error)
}

type ManagementDependencies struct {
	Availability  *execution.Availability
	Operations    *operation.Operations
	Images        *image.Images
	GC            *gc.Collector
	SupplyCleanup *supply.SupplyCleanup
}

// Management admits host requests through the same durable business workflows.
type Management struct {
	metadata      NodeMetadata
	availability  *execution.Availability
	operations    *operation.Operations
	images        *image.Images
	collector     *gc.Collector
	supplyCleanup *supply.SupplyCleanup
}

func NewManagement(metadata NodeMetadata, dependencies ManagementDependencies) *Management {
	return &Management{
		metadata:      metadata,
		availability:  dependencies.Availability,
		operations:    dependencies.Operations,
		images:        dependencies.Images,
		collector:     dependencies.GC,
		supplyCleanup: dependencies.SupplyCleanup,
	}
}

// ReadNodeStatus reads metadata under the caller's existing store custody.
func ReadNodeStatus(ctx context.Context, metadata NodeMetadata) (types.NodeStatus, error) {
	status := types.NodeStatus{
		Format:           store.Format,
		Version:          store.Version,
		StoreID:          metadata.Identity().Identity,
		RecoveryReviewed: metadata.Reviewed(),
	}
	var err error
	status.Epoch, err = metadata.Epoch(ctx)
	return status, err
}

func (m *Management) Status(ctx context.Context, principal authorization.Principal) (types.NodeStatus, error) {
	if !principal.Root || principal.SandboxID != "" {
		return types.NodeStatus{}, errs.ErrForbidden
	}
	status, err := ReadNodeStatus(ctx, m.metadata)
	if err != nil {
		return status, err
	}
	status.Draining = m.availability.IsDraining()
	return status, nil
}

// Drain stops new admission under the shared registration guard.
func (m *Management) Drain() {
	m.availability.Drain()
}

func managementOperation(value operation.Record) types.ManagementOperation {
	return types.ManagementOperation{
		ID:     value.ID,
		Key:    value.Key,
		Status: value.Status,
		Result: types.ManagementResult(value.Result),
	}
}

func (m *Management) ManagementOperation(ctx context.Context, principal authorization.Principal, id string) (types.ManagementOperation, error) {
	value, err := m.operations.GetManagementOperation(ctx, principal, id)
	return managementOperation(value), err
}

func (m *Management) Manage(ctx context.Context, principal authorization.Principal, request types.ManagementRequest) (types.ManagementResponse, error) {
	var response types.ManagementResponse
	if !principal.Root || principal.SandboxID != "" {
		return response, errs.ErrForbidden
	}
	var accepted operation.Record
	var err error
	switch request.Action {
	case types.ManagementActionImport:
		if request.Import == nil || request.GC != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, errs.InvalidInput("import requires only import options")
		}
		accepted, err = m.images.AdmitImport(ctx, principal, request.Key, imageimport.ImageImportOptions(*request.Import))
	case types.ManagementActionGC:
		if request.GC == nil || request.Import != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, errs.InvalidInput("gc requires only gc options")
		}
		report, err := m.collector.Collect(ctx, principal, request.Key, *request.GC)
		response.GC = &report
		return response, err
	case types.ManagementActionSupplyCleanup:
		if request.Import != nil || request.GC != nil || request.OperationID == "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, errs.InvalidInput("supply cleanup requires only original operation id")
		}
		accepted, err = m.supplyCleanup.AdmitSupplyCleanup(ctx, principal, request.Key, supply.SupplyCleanupRequest{
			OperationID: request.OperationID,
		})
	case types.ManagementActionImageCleanup:
		if request.Import != nil || request.GC != nil {
			return response, errs.InvalidInput("image cleanup accepts only original import or cache identity")
		}
		accepted, err = m.images.AdmitCleanup(ctx, principal, request.Key, image.ImageCleanupRequest{
			ImportOperationID: request.OperationID,
			CacheBuilder:      request.CacheBuilder,
			CacheDiffID:       request.CacheDiffID,
		})
	default:
		return response, errs.InvalidInput("unsupported management action")
	}
	if accepted.ID != "" {
		view := managementOperation(accepted)
		response.Operation = &view
	}
	if err != nil {
		return response, err
	}
	accepted, err = m.operations.WaitManagement(ctx, principal, accepted.ID)
	view := managementOperation(accepted)
	response.Operation = &view
	if err != nil && accepted.Status == "failed" {
		return response, fmt.Errorf("management operation %s failed", accepted.ID)
	}
	return response, err
}
