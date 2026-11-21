// Package management adapts authenticated local administration to Haven.
package management

import (
	"net/http"
	"strings"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/httpwire"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/localuds"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

type Services struct {
	Node          *node.Service
	Operations    *operation.Monitor
	Images        *image.Images
	GC            *gc.Collector
	SupplyCleanup *supply.SupplyCleanup
}

func New(services Services) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !localuds.Authorized(r.Context()) {
			httpwire.NewFailure(http.StatusForbidden, types.CodeForbidden, "local administrator peer required").Write(w)
			return
		}
		principal := authorization.Principal{
			Root:     true,
			TenantID: "node-admin",
		}
		switch {
		case r.URL.Path == "/v1/status" && r.Method == http.MethodGet:
			value, err := services.Node.Status(r.Context(), principal)
			httpwire.Respond(w, value, err)
		case r.URL.Path == "/v1/manage" && r.Method == http.MethodPost:
			services.manage(w, r, principal)
		case strings.HasPrefix(r.URL.Path, "/v1/operations/") && r.Method == http.MethodGet:
			id := strings.TrimPrefix(r.URL.Path, "/v1/operations/")
			if id == "" || strings.Contains(id, "/") {
				httpwire.NotFound().Write(w)
				return
			}
			value, err := services.Operations.GetManagementOperation(r.Context(), principal, id)
			httpwire.Respond(w, operationView(value), err)
		default:
			httpwire.NotFound().Write(w)
		}
	})
}

func (s Services) manage(w http.ResponseWriter, r *http.Request, principal authorization.Principal) {
	var request types.ManagementRequest
	if err := httpwire.ReadJSON(w, r, &request); err != nil {
		httpwire.Error(w, err)
		return
	}
	ctx := r.Context()
	var accepted operation.State
	var err error
	switch request.Action {
	case types.ManagementActionImport:
		if request.Import == nil || request.GC != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			httpwire.InvalidRequest("import requires only import options").Write(w)
			return
		}
		accepted, err = s.Images.AdmitImport(ctx, principal, request.Key, imageimport.ImageImportOptions(*request.Import))
	case types.ManagementActionGC:
		if request.GC == nil || request.Import != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			httpwire.InvalidRequest("gc requires only gc options").Write(w)
			return
		}
		report, err := s.GC.Collect(ctx, principal, request.Key, *request.GC)
		httpwire.Respond(w, types.ManagementResponse{GC: &report}, err)
		return
	case types.ManagementActionSupplyCleanup:
		if request.Import != nil || request.GC != nil || request.OperationID == "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			httpwire.InvalidRequest("supply cleanup requires only original operation id").Write(w)
			return
		}
		accepted, err = s.SupplyCleanup.AdmitSupplyCleanup(ctx, principal, request.Key, supply.SupplyCleanupRequest{OperationID: request.OperationID})
	case types.ManagementActionImageCleanup:
		if request.Import != nil || request.GC != nil {
			httpwire.InvalidRequest("image cleanup accepts only original import or cache identity").Write(w)
			return
		}
		accepted, err = s.Images.AdmitCleanup(ctx, principal, request.Key, image.ImageCleanupRequest{
			ImportOperationID: request.OperationID,
			CacheBuilder:      request.CacheBuilder,
			CacheDiffID:       request.CacheDiffID,
		})
	default:
		httpwire.InvalidRequest("unsupported management action").Write(w)
		return
	}
	if err != nil {
		httpwire.Error(w, err)
		return
	}
	result, err := s.Operations.WaitManagement(ctx, principal, accepted.ID)
	if result.IsTerminalFailure(err) {
		httpwire.InternalFailure().Write(w)
		return
	}
	view := operationView(result)
	httpwire.Respond(w, types.ManagementResponse{Operation: &view}, err)
}

func operationView(value operation.State) types.ManagementOperation {
	return types.ManagementOperation{
		ID:     value.ID,
		Key:    value.Key,
		Status: value.Status,
		Result: types.ManagementResult(value.Result),
	}
}
