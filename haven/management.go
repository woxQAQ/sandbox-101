package haven

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/types"
)

func (service *Service) Status(ctx context.Context, principal Principal) (types.NodeStatus, error) {
	if !principal.Root || principal.SandboxID != "" {
		return types.NodeStatus{}, ErrForbidden
	}
	status := types.NodeStatus{
		Format:           storeFormat,
		Version:          storeVersion,
		StoreID:          service.store.identity.Identity,
		RecoveryReviewed: service.store.reviewed,
	}
	if err := service.store.db.QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&status.Epoch); err != nil {
		return status, err
	}
	service.mu.Lock()
	status.Draining = service.draining
	service.mu.Unlock()
	return status, nil
}

// InspectState validates offline metadata under the normal exclusive writer lock.
// It never constructs a runtime or dispatches retained workflows.
func InspectState(ctx context.Context, directory string) (types.NodeStatus, error) {
	state, err := openStore(directory, false)
	if err != nil {
		return types.NodeStatus{}, err
	}
	status := types.NodeStatus{
		Format:           storeFormat,
		Version:          storeVersion,
		StoreID:          state.identity.Identity,
		RecoveryReviewed: state.reviewed,
	}
	err = state.db.QueryRowContext(ctx, "SELECT epoch FROM store_metadata WHERE singleton=1").Scan(&status.Epoch)
	closeErr := state.close()
	if err != nil {
		return status, err
	}
	return status, closeErr
}

func managementOperation(value Operation) types.ManagementOperation {
	return types.ManagementOperation{
		ID:     value.ID,
		Key:    value.Key,
		Status: value.Status,
		Result: types.ManagementResult(value.Result),
	}
}

func (service *Service) ManagementOperation(ctx context.Context, principal Principal, id string) (types.ManagementOperation, error) {
	value, err := service.GetManagementOperation(ctx, principal, id)
	return managementOperation(value), err
}

// Manage accepts host administration at the same workflow boundary as business
// admission. Adapters have no catalog access or host effects implementation.
func (service *Service) Manage(ctx context.Context, principal Principal, request types.ManagementRequest) (types.ManagementResponse, error) {
	var response types.ManagementResponse
	if !principal.Root || principal.SandboxID != "" {
		return response, ErrForbidden
	}
	var operation Operation
	var err error
	switch request.Action {
	case "import":
		if request.Import == nil || request.GC != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, fmt.Errorf("%w: import requires only import options", ErrInvalid)
		}
		operation, err = service.AdmitImageImport(ctx, principal, request.Key, imageimport.ImageImportOptions(*request.Import))
	case "gc":
		if request.GC == nil || request.Import != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, fmt.Errorf("%w: gc requires only gc options", ErrInvalid)
		}
		report, err := service.CollectGarbage(ctx, principal, request.Key, *request.GC)
		response.GC = &report
		return response, err
	case "supply_cleanup":
		if request.Import != nil || request.GC != nil || request.OperationID == "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, fmt.Errorf("%w: supply cleanup requires only original operation id", ErrInvalid)
		}
		operation, err = service.AdmitSupplyCleanup(ctx, principal, request.Key, SupplyCleanupRequest{
			OperationID: request.OperationID,
		})
	case "image_cleanup":
		if request.Import != nil || request.GC != nil {
			return response, fmt.Errorf("%w: image cleanup accepts only original import or cache identity", ErrInvalid)
		}
		operation, err = service.AdmitImageCleanup(ctx, principal, request.Key, ImageCleanupRequest{
			ImportOperationID: request.OperationID,
			CacheBuilder:      request.CacheBuilder,
			CacheDiffID:       request.CacheDiffID,
		})
	default:
		return response, fmt.Errorf("%w: unsupported management action", ErrInvalid)
	}
	if operation.ID != "" {
		view := managementOperation(operation)
		response.Operation = &view
	}
	if err != nil {
		return response, err
	}
	operation, err = service.WaitManagement(ctx, principal, operation.ID)
	view := managementOperation(operation)
	response.Operation = &view
	if err != nil && operation.Status == "failed" {
		return response, fmt.Errorf("management operation %s failed", operation.ID)
	}
	return response, err
}
