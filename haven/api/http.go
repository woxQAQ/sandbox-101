// Package api adapts the public business protocol to Haven workflows.
package api

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
// The handler receives no application lifecycle, runtime or database authority.
type Services struct {
	Authentication *authorization.Authentication
	Catalog        *sandbox.Catalog
	Templates      *supply.TemplateCatalog
	Operations     *operation.Operations
	Delivery       *sandbox.OperationDelivery
	Tenants        *authorization.Tenants
	Requests       *sandbox.SandboxRequests
	Dataplane      *dataplane.Plane
	Node           *node.Service
}

// handler owns exactly the assembled business capabilities. The application
// keeps the writer lock, egress callback binding, startup recovery and final
// shutdown outside this package.
type handler struct {
	services Services
}

// Lifecycle actions on an admitted sandbox. Each gets an explicit route so the
// URL decides command and target; the request body only supplies parameters.
var sandboxActions = []string{
	"configure",
	"renew",
	"checkpoint",
	"pause",
	"resume",
	"rollback",
	"fork",
}

func New(services Services) http.Handler {
	h := &handler{
		services: services,
	}
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			h.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		httpwire.NotFound().Write(c.Writer)
	})
	router.NoMethod(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			h.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		httpwire.NewFailure(http.StatusMethodNotAllowed, types.CodeInvalid, "method not allowed").Write(c.Writer)
	})
	router.Match([]string{
		http.MethodGet,
		http.MethodHead,
	}, "/healthz", func(c *gin.Context) {
		httpwire.JSON(c.Writer, 200, map[string]string{
			"status": "ok",
		})
	})
	router.Match([]string{
		http.MethodGet,
		http.MethodHead,
	}, "/metrics", h.authenticate, h.metrics)
	router.Any("/preview/*path", gin.WrapF(h.preview))
	v1 := router.Group("/v1", h.authenticate)
	v1.POST("/tenants", h.putTenant)
	v1.GET("/status", h.status)
	v1.POST("/node/drain", h.drainNode)
	v1.GET("/sandboxes", h.listSandboxes)
	v1.GET("/templates", h.listTemplates)
	v1.GET("/checkpoints", h.listCheckpoints)
	v1.GET("/sandboxes/:id", h.getSandbox)
	v1.GET("/operations/:id", h.getOperation)
	v1.GET("/sandboxes/:id/connect", h.connectSandbox)
	v1.POST("/sandboxes", h.claimSandbox)
	v1.DELETE("/sandboxes/:id", h.sandboxAction("release"))
	for _, action := range sandboxActions {
		v1.POST("/sandboxes/:id/"+action, h.sandboxAction(action))
	}
	v1.POST("/sandboxes/:id/preview", h.previewSandbox)
	v1.POST("/templates", h.registerTemplate)
	v1.POST("/templates/:id/build", h.buildTemplate)
	v1.DELETE("/checkpoints/:id", h.deleteCheckpoint)
	v1.POST("/checkpoints/:id/fork", h.forkCheckpoint)
	return router
}

func principal(c *gin.Context) authorization.Principal {
	return c.MustGet("principal").(authorization.Principal)
}

func (h *handler) authenticate(c *gin.Context) {
	header := c.GetHeader("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		httpwire.NotFound().Write(c.Writer)
		c.Abort()
		return
	}
	token := strings.TrimPrefix(header, "Bearer ")
	value, err := h.services.Authentication.Authenticate(c.Request.Context(), token)
	if err != nil && c.Request.Method == http.MethodGet && c.FullPath() == "/v1/operations/:id" {
		value, err = h.services.Authentication.AuthenticateOperation(c.Request.Context(), token)
	}
	if err != nil {
		httpwire.Error(c.Writer, err)
		c.Abort()
		return
	}
	if value.Root && c.GetHeader("X-Tenant-ID") != "" {
		value.TenantID = c.GetHeader("X-Tenant-ID")
	}
	c.Set("principal", value)
}

// decode strictly reads one public command. The path decides action and target;
// repeated action or ID values in the body must agree with it.
func decode(c *gin.Context, action, sandboxID, checkpointID string) (types.Command, bool) {
	var command types.Command
	if action != "release" || c.Request.ContentLength != 0 {
		if err := httpwire.ReadJSON(c.Writer, c.Request, &command); err != nil {
			httpwire.Error(c.Writer, err)
			return command, false
		}
	}
	if command.Action != "" && command.Action != action {
		httpwire.InvalidRequest("request action disagrees with the route").Write(c.Writer)
		return command, false
	}
	if sandboxID != "" && command.SandboxID != "" && command.SandboxID != sandboxID {
		httpwire.InvalidRequest("request sandbox id disagrees with the route").Write(c.Writer)
		return command, false
	}
	if checkpointID != "" && command.CheckpointID != "" && command.CheckpointID != checkpointID {
		httpwire.InvalidRequest("request checkpoint id disagrees with the route").Write(c.Writer)
		return command, false
	}
	command.Action = action
	if sandboxID != "" {
		command.SandboxID = sandboxID
	}
	if checkpointID != "" {
		command.CheckpointID = checkpointID
	}
	return command, true
}

func (h *handler) claimSandbox(c *gin.Context) {
	command, ok := decode(c, "claim", "", "")
	if !ok {
		return
	}
	h.submit(c, command)
}

// sandboxAction registers one lifecycle command explicitly. The route fixes
// action and sandbox; the body only carries parameters.
func (h *handler) sandboxAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		command, ok := decode(c, action, c.Param("id"), "")
		if !ok {
			return
		}
		if action == "fork" && command.CheckpointID != "" {
			httpwire.InvalidRequest("sandbox fork source is the route sandbox").Write(c.Writer)
			return
		}
		h.submit(c, command)
	}
}

func (h *handler) registerTemplate(c *gin.Context) {
	command, ok := decode(c, "template_put", "", "")
	if !ok {
		return
	}
	h.submit(c, command)
}

func (h *handler) buildTemplate(c *gin.Context) {
	command, ok := decode(c, "template_build", "", "")
	if !ok {
		return
	}
	if command.Template == nil {
		httpwire.InvalidRequest("template field is required").Write(c.Writer)
		return
	}
	if command.Template.ID == "" {
		command.Template.ID = c.Param("id")
	}
	if command.Template.ID != c.Param("id") {
		httpwire.InvalidRequest("request template id disagrees with the route").Write(c.Writer)
		return
	}
	h.submit(c, command)
}

func (h *handler) deleteCheckpoint(c *gin.Context) {
	h.submit(c, types.Command{
		// Clients use the public "delete_checkpoint" spelling; the stored
		// action is "checkpoint_delete". The reverse translation lives in
		// operation decode.
		Action:       "checkpoint_delete",
		CheckpointID: c.Param("id"),
	})
}

func (h *handler) forkCheckpoint(c *gin.Context) {
	command, ok := decode(c, "fork", "", c.Param("id"))
	if !ok {
		return
	}
	if command.SandboxID != "" {
		httpwire.InvalidRequest("checkpoint fork source is the route checkpoint").Write(c.Writer)
		return
	}
	h.submit(c, command)
}

// submit accepts one business command and delivers the accepted operation
// synchronously or as an asynchronous pointer. All routes share this single
// submit, wait and response behavior.
func (h *handler) submit(c *gin.Context, command types.Command) {
	request := c.Request
	key := request.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 256 {
		httpwire.InvalidRequest("Idempotency-Key required (max 256 bytes)").Write(c.Writer)
		return
	}
	payload, err := json.Marshal(command)
	if err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	var intent operation.Command
	if err = json.Unmarshal(payload, &intent); err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	accepted, err := h.services.Requests.Submit(request.Context(), principal(c), key, intent)
	if err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	if strings.Contains(request.Header.Get("Prefer"), "respond-async") {
		view, err := h.services.Delivery.PublicOperation(request.Context(), principal(c), accepted.ID)
		if err != nil {
			httpwire.Error(c.Writer, err)
			return
		}
		c.Header("Location", "/v1/operations/"+accepted.ID)
		httpwire.JSON(c.Writer, 202, operationView(view))
		return
	}
	accepted, err = h.services.Operations.Wait(request.Context(), principal(c), accepted.ID)
	if err != nil && !accepted.SettledFailure(err) {
		httpwire.Error(c.Writer, err)
		return
	}
	view, err := h.services.Delivery.PublicOperation(request.Context(), principal(c), accepted.ID)
	if err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	if view.Status == "failed" {
		httpwire.InternalFailure().Write(c.Writer)
		return
	}
	httpwire.Respond(c.Writer, view.Result, nil)
}

// operationView adds transport error data to a copy of the frozen business
// view. Internal diagnostics never determine HTTP categories or enter this
// view. Both asynchronous answers and operation GET use this representation.
func operationView(view types.Operation) types.Operation {
	view.Error = nil
	if view.Status == "failed" {
		failure := httpwire.InternalFailure()
		view.Error = &failure.Payload
	}
	return view
}

func (h *handler) getOperation(c *gin.Context) {
	view, err := h.services.Delivery.PublicOperation(c.Request.Context(), principal(c), c.Param("id"))
	httpwire.Respond(c.Writer, operationView(view), err)
}

func (h *handler) listSandboxes(c *gin.Context) {
	value, err := h.services.Catalog.List(c.Request.Context(), principal(c))
	httpwire.Respond(c.Writer, value, err)
}

func (h *handler) listTemplates(c *gin.Context) {
	value, err := h.services.Templates.Templates(c.Request.Context(), principal(c))
	httpwire.Respond(c.Writer, value, err)
}

func (h *handler) listCheckpoints(c *gin.Context) {
	value, err := h.services.Catalog.Checkpoints(c.Request.Context(), principal(c))
	httpwire.Respond(c.Writer, value, err)
}

func (h *handler) getSandbox(c *gin.Context) {
	value, err := h.services.Catalog.Get(c.Request.Context(), principal(c), c.Param("id"))
	httpwire.Respond(c.Writer, value, err)
}

func (h *handler) status(c *gin.Context) {
	value, err := h.services.Node.Status(c.Request.Context(), principal(c))
	httpwire.Respond(c.Writer, value, err)
}

func (h *handler) drainNode(c *gin.Context) {
	if err := h.services.Node.Drain(principal(c)); err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	httpwire.JSON(c.Writer, 200, map[string]bool{
		"draining": true,
	})
}

func (h *handler) putTenant(c *gin.Context) {
	owner := principal(c)
	if !owner.Root {
		httpwire.AdminRequired().Write(c.Writer)
		return
	}
	var value types.Tenant
	if err := httpwire.ReadJSON(c.Writer, c.Request, &value); err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	if value.ID == "" || value.MaxSandboxes < 1 || value.MaxStorageMiB < 1 {
		httpwire.InvalidRequest("tenant id and positive quotas required").Write(c.Writer)
		return
	}
	if value.Token == "" {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			httpwire.Error(c.Writer, err)
			return
		}
		value.Token = "tenant_" + hex.EncodeToString(entropy[:])
	}
	_, err := h.services.Tenants.PutTenant(c.Request.Context(), owner, value)
	if err != nil {
		httpwire.Error(c.Writer, err)
		return
	}
	httpwire.JSON(c.Writer, 201, value)
}

func (h *handler) metrics(c *gin.Context) {
	if !principal(c).Root {
		httpwire.AdminRequired().Write(c.Writer)
		return
	}
	sandboxes, err := h.services.Catalog.List(c.Request.Context(), principal(c))
	if err != nil {
		httpwire.Error(c.Writer, err)
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
