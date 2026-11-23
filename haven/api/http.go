// Package api adapts the public business protocol to Haven workflows.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/node"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

// Services names the business capabilities exposed by the public protocol.
// The handler receives no application lifecycle, runtime or database authority.
type Services struct {
	Authentication   *authorization.Authentication
	Catalog          *sandbox.Catalog
	Templates        *supply.TemplateCatalog
	OperationMonitor *operation.Monitor
	OperationViews   *sandbox.OperationViews
	Tenants          *authorization.Tenants
	CommandAdmission *sandbox.CommandAdmission
	Dataplane        *dataplane.Plane
	Node             *node.Service
}

func New(h Services) http.Handler {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.Use(noStore)
	router.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			h.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		notFound(c)
	})
	router.NoMethod(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			h.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		c.AbortWithStatusJSON(http.StatusMethodNotAllowed, types.ApiError{
			Code:    types.CodeInvalid,
			Message: "method not allowed",
		})
	})
	router.Match([]string{
		http.MethodGet,
		http.MethodHead,
	}, "/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, map[string]string{
			"status": "ok",
		})
	})

	v1 := router.Group("/v1", h.authenticate)
	{
		v1.POST("/tenants", h.putTenant)
		v1.GET("/status", h.status)
		v1.POST("/node/drain", h.drainNode)
		v1.GET("/sandboxes", h.listSandboxes)
		v1.GET("/sandboxes/:id", h.getSandbox)
		v1.POST("/sandboxes", h.claimSandbox)
		v1.DELETE("/sandboxes/:id", h.sandboxAction("release"))
		// Lifecycle actions on an admitted sandbox. Each gets an explicit route so the
		// URL decides command and target; the request body only supplies parameters.
		sandboxActions := []string{
			"configure",
			"renew",
			"checkpoint",
			"pause",
			"resume",
			"rollback",
			"fork",
		}
		for _, action := range sandboxActions {
			v1.POST("/sandboxes/:id/"+action, h.sandboxAction(action))
		}

		v1.GET("/operations/:id", h.getOperation)
		v1.GET("/templates", h.listTemplates)
		v1.POST("/templates", h.registerTemplate)
		v1.POST("/templates/:id/build", h.buildTemplate)

		v1.GET("/checkpoints", h.listCheckpoints)
		v1.DELETE("/checkpoints/:id", h.deleteCheckpoint)
		v1.POST("/checkpoints/:id/fork", h.forkCheckpoint)
	}
	// see data.go
	previewTransport := newPreviewTransport()
	router.Any("/preview/*path", func(c *gin.Context) {
		h.preview(c, previewTransport)
	})
	v1.GET("/sandboxes/:id/connect", h.connectSandbox)
	v1.POST("/sandboxes/:id/preview", h.previewSandbox)
	return router
}

func principal(c *gin.Context) authorization.Principal {
	return c.MustGet("principal").(authorization.Principal)
}

func (h *Services) authenticate(c *gin.Context) {
	header := c.GetHeader("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		notFound(c)
		return
	}
	token := strings.TrimPrefix(header, "Bearer ")
	value, err := h.Authentication.Authenticate(c.Request.Context(), token)
	if err != nil && c.Request.Method == http.MethodGet && c.FullPath() == "/v1/operations/:id" {
		value, err = h.Authentication.AuthenticateOperation(c.Request.Context(), token)
	}
	if err != nil {
		writeError(c, err)
		return
	}
	if value.Root && c.GetHeader("X-Tenant-ID") != "" {
		value.TenantID = c.GetHeader("X-Tenant-ID")
	}
	c.Set("principal", value)
}

// handlers
func (h *Services) putTenant(c *gin.Context) {
	owner := principal(c)
	if !owner.Root {
		c.AbortWithStatusJSON(http.StatusForbidden, types.ApiError{
			Code:    types.CodeForbidden,
			Message: "root access required",
		})
		return
	}
	var value types.Tenant
	if err := decodeJSON(c, &value); err != nil {
		writeError(c, err)
		return
	}
	if value.ID == "" || value.MaxSandboxes < 1 || value.MaxStorageMiB < 1 {
		invalidRequest(c, "tenant id and positive quotas required")
		return
	}
	if value.Token == "" {
		var entropy [32]byte
		if _, err := rand.Read(entropy[:]); err != nil {
			writeError(c, err)
			return
		}
		value.Token = "tenant_" + hex.EncodeToString(entropy[:])
	}
	_, err := h.Tenants.PutTenant(c.Request.Context(), owner, value)
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, value)
}

func (h *Services) status(c *gin.Context) {
	value, err := h.Node.Status(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (h *Services) drainNode(c *gin.Context) {
	if err := h.Node.Drain(principal(c)); err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]bool{
		"draining": true,
	})
}

func (h *Services) listSandboxes(c *gin.Context) {
	value, err := h.Catalog.List(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (h *Services) getSandbox(c *gin.Context) {
	value, err := h.Catalog.Get(c.Request.Context(), principal(c), c.Param("id"))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (h *Services) claimSandbox(c *gin.Context) {
	command, ok := decode(c, "claim", "", "")
	if !ok {
		return
	}
	h.submit(c, command)
}

// sandboxAction registers one lifecycle command explicitly. The route fixes
// action and sandbox; the body only carries parameters.
func (h *Services) sandboxAction(action string) gin.HandlerFunc {
	return func(c *gin.Context) {
		command, ok := decode(c, action, c.Param("id"), "")
		if !ok {
			return
		}
		if action == "fork" && command.CheckpointID != "" {
			invalidRequest(c, "sandbox fork source is the route sandbox")
			return
		}
		h.submit(c, command)
	}
}

func (h *Services) getOperation(c *gin.Context) {
	view, err := h.OperationViews.Get(c.Request.Context(), principal(c), c.Param("id"))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, operationView(view))
}

func (h *Services) listTemplates(c *gin.Context) {
	value, err := h.Templates.Templates(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (h *Services) registerTemplate(c *gin.Context) {
	command, ok := decode(c, "template_put", "", "")
	if !ok {
		return
	}
	h.submit(c, command)
}

func (h *Services) buildTemplate(c *gin.Context) {
	command, ok := decode(c, "template_build", "", "")
	if !ok {
		return
	}
	if command.Template == nil {
		invalidRequest(c, "template field is required")
		return
	}
	if command.Template.ID == "" {
		command.Template.ID = c.Param("id")
	}
	if command.Template.ID != c.Param("id") {
		invalidRequest(c, "request template id disagrees with the route")
		return
	}
	h.submit(c, command)
}

func (h *Services) listCheckpoints(c *gin.Context) {
	value, err := h.Catalog.Checkpoints(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, value)
}

func (h *Services) deleteCheckpoint(c *gin.Context) {
	h.submit(c, types.Command{
		// Clients use the public "delete_checkpoint" spelling; the stored
		// action is "checkpoint_delete". The reverse translation lives in
		// operation decode.
		Action:       "checkpoint_delete",
		CheckpointID: c.Param("id"),
	})
}

func (h *Services) forkCheckpoint(c *gin.Context) {
	command, ok := decode(c, "fork", "", c.Param("id"))
	if !ok {
		return
	}
	if command.SandboxID != "" {
		invalidRequest(c, "checkpoint fork source is the route checkpoint")
		return
	}
	h.submit(c, command)
}

// decode strictly reads one public command. The path decides action and target;
// repeated action or ID values in the body must agree with it.
func decode(c *gin.Context, action, sandboxID, checkpointID string) (types.Command, bool) {
	var command types.Command
	if action != "release" || c.Request.ContentLength != 0 {
		if err := decodeJSON(c, &command); err != nil {
			writeError(c, err)
			return command, false
		}
	}
	if command.Action != "" && command.Action != action {
		invalidRequest(c, "request action disagrees with the route")
		return command, false
	}
	if sandboxID != "" && command.SandboxID != "" && command.SandboxID != sandboxID {
		invalidRequest(c, "request sandbox id disagrees with the route")
		return command, false
	}
	if checkpointID != "" && command.CheckpointID != "" && command.CheckpointID != checkpointID {
		invalidRequest(c, "request checkpoint id disagrees with the route")
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

// submit accepts one business command and delivers the accepted operation
// synchronously or as an asynchronous pointer. All routes share this single
// submit, wait and response behavior.
func (h *Services) submit(c *gin.Context, command types.Command) {
	request := c.Request
	key := request.Header.Get("Idempotency-Key")
	if len(key) < 1 || len(key) > 256 {
		invalidRequest(c, "Idempotency-Key required (max 256 bytes)")
		return
	}
	accepted, err := h.CommandAdmission.Submit(request.Context(), principal(c), key, operation.NewIntent(command))
	if err != nil {
		writeError(c, err)
		return
	}
	if prefersAsync(request.Header.Values("Prefer")) {
		view, err := h.OperationViews.Get(request.Context(), principal(c), accepted.ID)
		if err != nil {
			writeError(c, err)
			return
		}
		c.Header("Location", "/v1/operations/"+accepted.ID)
		c.JSON(http.StatusAccepted, operationView(view))
		return
	}
	accepted, err = h.OperationMonitor.Wait(request.Context(), principal(c), accepted.ID)
	if err != nil && !accepted.IsTerminalFailure(err) {
		writeError(c, err)
		return
	}
	view, err := h.OperationViews.Get(request.Context(), principal(c), accepted.ID)
	if err != nil {
		writeError(c, err)
		return
	}
	if view.Status == "failed" {
		internalFailure(c)
		return
	}
	c.JSON(http.StatusOK, view.Result)
}

// operationView adds transport error data to a copy of the frozen business
// view. Internal diagnostics never determine HTTP categories or enter this
// view. Both asynchronous answers and operation GET use this representation.
func operationView(view types.Operation) types.Operation {
	view.Error = nil
	if view.Status == "failed" {
		view.Error = &types.ApiError{
			Code:    types.CodeInternal,
			Message: "internal service error",
		}
	}
	return view
}

// prefersAsync checks the first respond-async preference across all Prefer fields.
// RFC 7240 defines it without a value and treats an empty value as absent.
func prefersAsync(fields []string) bool {
	const token = "respond-async"
	for _, field := range fields {
		for field != "" {
			var preference string
			preference, field = cutPreference(field)
			head, _, _ := strings.Cut(preference, ";")
			name, value, _ := strings.Cut(head, "=")
			name = strings.Trim(name, " \t")
			if len(name) == len(token) && strings.EqualFold(name, token) {
				value = strings.Trim(value, " \t")
				return value == "" || value == `""`
			}
		}
	}
	return false
}

// cutPreference splits at the first comma outside an HTTP quoted-string.
func cutPreference(field string) (preference, rest string) {
	quoted := false
	for i := 0; i < len(field); i++ {
		switch field[i] {
		case '"':
			quoted = !quoted
		case '\\':
			if quoted {
				i++ // Skip the escaped byte inside a quoted-string.
			}
		case ',':
			if !quoted {
				return field[:i], field[i+1:]
			}
		}
	}
	return field, ""
}
