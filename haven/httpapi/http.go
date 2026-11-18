// Package httpapi adapts the public business protocol to Haven workflows.
package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/httpwire"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// Services names the business capabilities exposed by the public protocol.
// Controllers receive no application lifecycle, runtime or database authority.
type Services struct {
	Authentication *authorization.Authentication
	Catalog        *sandbox.Catalog
	Templates      *supply.TemplateCatalog
	Operations     *operation.Operations
	Delivery       *sandbox.OperationDelivery
	Tenants        *authorization.Tenants
	Requests       *sandbox.SandboxRequests
	Dataplane      *dataplane.Plane
	Management     *node.Management
}

type Adapter struct {
	authentication *authorization.Authentication
	catalog        *sandbox.Catalog
	templates      *supply.TemplateCatalog
	operations     *operation.Operations
	delivery       *sandbox.OperationDelivery
	tenants        *authorization.Tenants
	requests       *sandbox.SandboxRequests
	plane          *dataplane.Plane
	management     *node.Management
}

func New(dependencies Services) http.Handler {
	adapter := &Adapter{
		authentication: dependencies.Authentication,
		catalog:        dependencies.Catalog,
		templates:      dependencies.Templates,
		operations:     dependencies.Operations,
		delivery:       dependencies.Delivery,
		tenants:        dependencies.Tenants,
		requests:       dependencies.Requests,
		plane:          dependencies.Dataplane,
		management:     dependencies.Management,
	}
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			adapter.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrNotFound,
			Message: "not found",
		})
	})
	router.NoMethod(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			adapter.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		writeJSON(c.Writer, 405, &types.ApiError{
			Code:    types.ErrInvalid,
			Message: "method not allowed",
		})
	})
	router.Match([]string{
		http.MethodGet,
		http.MethodHead,
	}, "/healthz", func(c *gin.Context) {
		writeJSON(c.Writer, 200, map[string]string{
			"status": "ok",
		})
	})
	router.Match([]string{
		http.MethodGet,
		http.MethodHead,
	}, "/metrics", adapter.authenticate, adapter.metrics)
	router.Any("/preview/*path", gin.WrapF(adapter.preview))
	v1 := router.Group("/v1", adapter.authenticate)
	v1.POST("/tenants", adapter.putTenant)
	v1.GET("/status", adapter.status)
	v1.POST("/drain", adapter.drain)
	v1.GET("/sandboxes", func(c *gin.Context) {
		value, err := adapter.catalog.List(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/templates", func(c *gin.Context) {
		value, err := adapter.templates.Templates(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/checkpoints", func(c *gin.Context) {
		value, err := adapter.catalog.Checkpoints(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/sandboxes/:id", func(c *gin.Context) {
		value, err := adapter.catalog.Get(c.Request.Context(), principal(c), c.Param("id"))
		respond(c.Writer, value, err)
	})
	v1.GET("/operations/:id", func(c *gin.Context) {
		value, err := adapter.delivery.PublicOperation(c.Request.Context(), principal(c), c.Param("id"))
		respond(c.Writer, operationResponse(value), err)
	})
	v1.GET("/sandboxes/:id/connect", func(c *gin.Context) {
		adapter.relay(c.Writer, c.Request, principal(c), c.Param("id"))
	})
	v1.POST("/claim", func(c *gin.Context) {
		adapter.mutate(c, "claim", "", "")
	})
	v1.POST("/templates", func(c *gin.Context) {
		adapter.mutate(c, "template_put", "", "")
	})
	v1.DELETE("/sandboxes/:id", func(c *gin.Context) {
		adapter.mutate(c, "release", c.Param("id"), "")
	})
	v1.POST("/sandboxes/:id/:action", func(c *gin.Context) {
		adapter.mutate(c, c.Param("action"), c.Param("id"), "")
	})
	v1.DELETE("/checkpoints/:id", func(c *gin.Context) {
		adapter.mutate(c, "delete_checkpoint", "", c.Param("id"))
	})
	v1.POST("/checkpoints/:id/fork", func(c *gin.Context) {
		adapter.mutate(c, "fork", "", c.Param("id"))
	})
	return router
}

func principal(c *gin.Context) authorization.Principal {
	return c.MustGet("principal").(authorization.Principal)
}

func (adapter *Adapter) authenticate(c *gin.Context) {
	header := c.GetHeader("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrNotFound,
			Message: "not found",
		})
		c.Abort()
		return
	}
	token := strings.TrimPrefix(header, "Bearer ")
	value, err := adapter.authentication.Authenticate(c.Request.Context(), token)
	if err != nil && c.Request.Method == http.MethodGet && c.FullPath() == "/v1/operations/:id" {
		value, err = adapter.authentication.AuthenticateOperation(c.Request.Context(), token)
	}
	if err != nil {
		writeError(c.Writer, err)
		c.Abort()
		return
	}
	if value.Root && c.GetHeader("X-Tenant-ID") != "" {
		value.TenantID = c.GetHeader("X-Tenant-ID")
	}
	c.Set("principal", value)
}

func (adapter *Adapter) status(c *gin.Context) {
	value, err := adapter.management.Status(c.Request.Context(), principal(c))
	respond(c.Writer, value, err)
}

func (adapter *Adapter) drain(c *gin.Context) {
	if !principal(c).Root {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrForbidden,
			Message: "root access required",
		})
		return
	}
	adapter.management.Drain()
	writeJSON(c.Writer, 200, map[string]bool{
		"draining": true,
	})
}

func (adapter *Adapter) putTenant(c *gin.Context) {
	owner := principal(c)
	if !owner.Root {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrForbidden,
			Message: "root access required",
		})
		return
	}
	var value types.Tenant
	if err := readJSON(c.Writer, c.Request, &value); err != nil {
		writeError(c.Writer, err)
		return
	}
	if value.ID == "" || value.MaxSandboxes < 1 || value.MaxStorageMiB < 1 {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrInvalid,
			Message: "tenant id and positive quotas required",
		})
		return
	}
	if value.Token == "" {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			writeError(c.Writer, err)
			return
		}
		value.Token = "tenant_" + hex.EncodeToString(entropy[:])
	}
	_, err := adapter.tenants.PutTenant(c.Request.Context(), owner, value)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	writeJSON(c.Writer, 201, value)
}

func (adapter *Adapter) metrics(c *gin.Context) {
	if !principal(c).Root {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrForbidden,
			Message: "root access required",
		})
		return
	}
	sandboxes, err := adapter.catalog.List(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	counts := map[string]int{}
	for _, sandbox := range sandboxes {
		counts[sandbox.State]++
	}
	c.Header("Content-Type", "text/plain; version=0.0.4")
	for _, state := range []string{
		"provisioning",
		"running",
		"pausing",
		"paused",
		"resuming",
		"failed",
		"terminated",
	} {
		if _, err = fmt.Fprintf(c.Writer, "sandboxd_sandboxes{state=%q} %d\n", state, counts[state]); err != nil {
			return
		}
	}
}

func (adapter *Adapter) mutate(c *gin.Context, action, sandboxID, checkpointID string) {
	request := c.Request
	var command types.Command
	if request.Method == http.MethodPost {
		if err := readJSON(c.Writer, request, &command); err != nil {
			writeError(c.Writer, err)
			return
		}
	}
	if c.FullPath() == "/v1/templates" && command.Action == "template_build" {
		action = "template_build"
	}
	command.Action = action
	if sandboxID != "" {
		command.SandboxID = sandboxID
	}
	if checkpointID != "" {
		command.CheckpointID = checkpointID
	}
	if sandboxID != "" && action == "preview" {
		adapter.createPreview(c.Writer, request, principal(c), sandboxID)
		return
	}
	key := request.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 256 {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrInvalid,
			Message: "Idempotency-Key required (max 256 bytes)",
		})
		return
	}
	actor := principal(c)
	if actor.SandboxID != "" && (command.SandboxID != actor.SandboxID || (action != "renew" && action != "release")) {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrNotFound,
			Message: "not found",
		})
		return
	}
	if action == "checkpoint_delete" {
		writeFailure(c.Writer, types.ApiError{
			Code:    types.ErrInvalid,
			Message: "unsupported sandbox action",
		})
		return
	}
	if actor.Root && action == "fork" {
		owner := ""
		if command.CheckpointID != "" {
			value, err := adapter.catalog.GetCheckpoint(request.Context(), actor, command.CheckpointID)
			if err != nil {
				writeError(c.Writer, err)
				return
			}
			owner = value.TenantID
		} else {
			value, err := adapter.catalog.Get(request.Context(), actor, command.SandboxID)
			if err != nil {
				writeError(c.Writer, err)
				return
			}
			owner = value.TenantID
		}
		if owner != actor.TenantID {
			writeFailure(c.Writer, types.ApiError{
				Code:    types.ErrNotFound,
				Message: "not found",
			})
			return
		}
	}
	payload, err := json.Marshal(command)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	var intent operation.Command
	if err = json.Unmarshal(payload, &intent); err != nil {
		writeError(c.Writer, err)
		return
	}
	// Clients use the public "delete_checkpoint" spelling; the stored action is
	// "checkpoint_delete". The reverse translation lives in operation decode.
	if action == "delete_checkpoint" {
		intent.Action = "checkpoint_delete"
	}
	operation, err := adapter.requests.Submit(request.Context(), principal(c), request.Header.Get("Idempotency-Key"), intent)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	if strings.Contains(request.Header.Get("Prefer"), "respond-async") {
		view, err := adapter.delivery.PublicOperation(request.Context(), principal(c), operation.ID)
		if err != nil {
			writeError(c.Writer, err)
			return
		}
		c.Header("Location", "/v1/operations/"+operation.ID)
		writeJSON(c.Writer, 202, operationResponse(view))
		return
	}
	operation, err = adapter.operations.Wait(request.Context(), principal(c), operation.ID)
	if err != nil && operation.Status != "failed" {
		writeError(c.Writer, err)
		return
	}
	view, err := adapter.delivery.PublicOperation(request.Context(), principal(c), operation.ID)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	view = operationResponse(view)
	if view.Error != nil {
		writeFailure(c.Writer, *view.Error)
		return
	}
	respond(c.Writer, view.Result, nil)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	httpwire.JSON(w, status, value)
}

func writeFailure(w http.ResponseWriter, failure types.ApiError) {
	httpwire.Failure(w, failure)
}

func writeError(w http.ResponseWriter, err error) {
	httpwire.Error(w, err)
}

func readJSON(w http.ResponseWriter, r *http.Request, value any) error {
	return httpwire.ReadJSON(w, r, value)
}

func respond(w http.ResponseWriter, value any, err error) {
	httpwire.Respond(w, value, err)
}
