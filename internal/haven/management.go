package haven

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func (service *Service) Status(ctx context.Context, principal Principal) (api.NodeStatus, error) {
	if !principal.Root || principal.SandboxID != "" {
		return api.NodeStatus{}, api.Fail(api.ErrForbidden, "root access required")
	}
	status := api.NodeStatus{
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
func InspectState(ctx context.Context, directory string) (api.NodeStatus, error) {
	state, err := openStore(directory, false)
	if err != nil {
		return api.NodeStatus{}, err
	}
	status := api.NodeStatus{
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

func managementOperation(value Operation) api.ManagementOperation {
	return api.ManagementOperation{
		ID:     value.ID,
		Key:    value.Key,
		Status: value.Status,
		Result: api.ManagementResult(value.Result),
	}
}

func (service *Service) ManagementOperation(ctx context.Context, principal Principal, id string) (api.ManagementOperation, error) {
	value, err := service.GetManagementOperation(ctx, principal, id)
	return managementOperation(value), err
}

// Manage accepts host administration at the same workflow boundary as business
// admission. Adapters have no catalog access or host effects implementation.
func (service *Service) Manage(ctx context.Context, principal Principal, request api.ManagementRequest) (api.ManagementResponse, error) {
	var response api.ManagementResponse
	if !principal.Root || principal.SandboxID != "" {
		return response, api.Fail(api.ErrForbidden, "root access required")
	}
	var operation Operation
	var err error
	switch request.Action {
	case "import":
		if request.Import == nil || request.GC != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, loom.Fail(loom.Invalid, "import requires only import options")
		}
		operation, err = service.AdmitImageImport(ctx, principal, request.Key, imageimport.ImageImportOptions(*request.Import))
	case "gc":
		if request.GC == nil || request.Import != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, loom.Fail(loom.Invalid, "gc requires only gc options")
		}
		report, err := service.CollectGarbage(ctx, principal, request.Key, *request.GC)
		response.GC = &report
		return response, err
	case "supply_cleanup":
		if request.Import != nil || request.GC != nil || request.OperationID == "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			return response, loom.Fail(loom.Invalid, "supply cleanup requires only original operation id")
		}
		operation, err = service.AdmitSupplyCleanup(ctx, principal, request.Key, SupplyCleanupRequest{
			OperationID: request.OperationID,
		})
	case "image_cleanup":
		if request.Import != nil || request.GC != nil {
			return response, loom.Fail(loom.Invalid, "image cleanup accepts only original import or cache identity")
		}
		operation, err = service.AdmitImageCleanup(ctx, principal, request.Key, ImageCleanupRequest{
			ImportOperationID: request.OperationID,
			CacheBuilder:      request.CacheBuilder,
			CacheDiffID:       request.CacheDiffID,
		})
	default:
		return response, loom.Fail(loom.Invalid, "unsupported management action")
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
