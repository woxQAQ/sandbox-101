package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc"
	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/localuds"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// ManagementServices names the node capabilities exposed to the local
// administrator socket. Authentication comes from the unix peer, not tokens.
type ManagementServices struct {
	Node             *node.Service
	OperationMonitor *operation.Monitor
	Images           *image.Images
	GC               *gc.Collector
	SupplyCleanup    *supply.SupplyCleanup
}

// NewManagement adapts authenticated local administration to Haven. The unix
// peer check gates every path, routed or not; passed requests run as the
// synthetic node administrator.
func NewManagement(services ManagementServices) http.Handler {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.Use(noStore, func(c *gin.Context) {
		if !localuds.Authorized(c.Request.Context()) {
			c.AbortWithStatusJSON(http.StatusForbidden, types.ApiError{
				Code:    types.CodeForbidden,
				Message: "local administrator peer required",
			})
			return
		}
		c.Set("principal", authorization.Principal{
			Root:     true,
			TenantID: "node-admin",
		})
	})
	router.NoRoute(func(c *gin.Context) {
		notFound(c)
	})
	router.GET("/v1/status", services.status)
	router.POST("/v1/manage", services.manage)
	router.GET("/v1/operations/:id", services.getOperation)
	return router
}

func (s ManagementServices) status(c *gin.Context) {
	value, err := s.Node.Status(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (s ManagementServices) getOperation(c *gin.Context) {
	value, err := s.OperationMonitor.GetManagementOperation(c.Request.Context(), principal(c), c.Param("id"))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, managementView(value))
}

func (s ManagementServices) manage(c *gin.Context) {
	var request types.ManagementRequest
	if err := decodeJSON(c, &request); err != nil {
		writeError(c, err)
		return
	}
	ctx := c.Request.Context()
	principal := principal(c)
	var accepted operation.State
	var err error
	switch request.Action {
	case types.ManagementActionImport:
		if request.Import == nil || request.GC != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			invalidRequest(c, "import requires only import options")
			return
		}
		accepted, err = s.Images.AdmitImport(ctx, principal, request.Key, imageimport.ImageImportOptions(*request.Import))
	case types.ManagementActionGC:
		if request.GC == nil || request.Import != nil || request.OperationID != "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			invalidRequest(c, "gc requires only gc options")
			return
		}
		report, err := s.GC.Collect(ctx, principal, request.Key, *request.GC)
		if err != nil {
			writeError(c, err)
			return
		}
		c.JSON(http.StatusOK, types.ManagementResponse{
			GC: &report,
		})
		return
	case types.ManagementActionSupplyCleanup:
		if request.Import != nil || request.GC != nil || request.OperationID == "" || request.CacheBuilder != "" || request.CacheDiffID != "" {
			invalidRequest(c, "supply cleanup requires only original operation id")
			return
		}
		accepted, err = s.SupplyCleanup.AdmitSupplyCleanup(ctx, principal, request.Key, supply.SupplyCleanupRequest{
			OperationID: request.OperationID,
		})
	case types.ManagementActionImageCleanup:
		if request.Import != nil || request.GC != nil {
			invalidRequest(c, "image cleanup accepts only original import or cache identity")
			return
		}
		accepted, err = s.Images.AdmitCleanup(ctx, principal, request.Key, image.ImageCleanupRequest{
			ImportOperationID: request.OperationID,
			CacheBuilder:      request.CacheBuilder,
			CacheDiffID:       request.CacheDiffID,
		})
	default:
		invalidRequest(c, "unsupported management action")
		return
	}
	if err != nil {
		writeError(c, err)
		return
	}
	result, err := s.OperationMonitor.WaitManagement(ctx, principal, accepted.ID)
	if result.IsTerminalFailure(err) {
		internalFailure(c)
		return
	}
	view := managementView(result)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, types.ManagementResponse{
		Operation: &view,
	})
}

// managementView projects the durable operation state onto the local
// administration shape; no transport error data exists on this socket.
func managementView(value operation.State) types.ManagementOperation {
	return types.ManagementOperation{
		ID:     value.ID,
		Key:    value.Key,
		Status: value.Status,
		Result: types.ManagementResult(value.Result),
	}
}
