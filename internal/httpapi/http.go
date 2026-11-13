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

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/internal/haven"
	"github.com/woxQAQ/haven/internal/httpwire"
)

type Adapter struct{ service *haven.Service }

func New(service *haven.Service) http.Handler {
	adapter := &Adapter{service: service}
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
		writeError(c.Writer, api.Fail(api.ErrNotFound, "not found"))
	})
	router.NoMethod(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			adapter.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		writeJSON(c.Writer, 405, &api.Error{Code: api.ErrInvalid, Message: "method not allowed"})
	})
	router.Match([]string{http.MethodGet, http.MethodHead}, "/healthz", func(c *gin.Context) { writeJSON(c.Writer, 200, map[string]string{"status": "ok"}) })
	router.Match([]string{http.MethodGet, http.MethodHead}, "/metrics", adapter.authenticate, adapter.metrics)
	router.Any("/preview/*path", gin.WrapF(adapter.preview))
	v1 := router.Group("/v1", adapter.authenticate)
	v1.POST("/tenants", adapter.putTenant)
	v1.GET("/status", func(c *gin.Context) {
		value, err := service.Status(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.POST("/drain", func(c *gin.Context) {
		if !principal(c).Root {
			writeError(c.Writer, api.Fail(api.ErrForbidden, "root access required"))
			return
		}
		service.Drain()
		writeJSON(c.Writer, 200, map[string]bool{"draining": true})
	})
	v1.GET("/sandboxes", func(c *gin.Context) {
		value, err := service.List(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/templates", func(c *gin.Context) {
		value, err := service.Templates(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/checkpoints", func(c *gin.Context) {
		value, err := service.Checkpoints(c.Request.Context(), principal(c))
		respond(c.Writer, value, err)
	})
	v1.GET("/sandboxes/:id", func(c *gin.Context) {
		value, err := service.Get(c.Request.Context(), principal(c), c.Param("id"))
		respond(c.Writer, value, err)
	})
	v1.GET("/operations/:id", func(c *gin.Context) {
		value, err := service.PublicOperation(c.Request.Context(), principal(c), c.Param("id"))
		respond(c.Writer, value, err)
	})
	v1.GET("/sandboxes/:id/connect", func(c *gin.Context) { adapter.relay(c.Writer, c.Request, principal(c), c.Param("id")) })
	v1.POST("/claim", func(c *gin.Context) { adapter.mutate(c, "claim", "", "") })
	v1.POST("/templates", func(c *gin.Context) { adapter.mutate(c, "template_put", "", "") })
	v1.DELETE("/sandboxes/:id", func(c *gin.Context) { adapter.mutate(c, "release", c.Param("id"), "") })
	v1.POST("/sandboxes/:id/:action", func(c *gin.Context) { adapter.mutate(c, c.Param("action"), c.Param("id"), "") })
	v1.DELETE("/checkpoints/:id", func(c *gin.Context) { adapter.mutate(c, "delete_checkpoint", "", c.Param("id")) })
	v1.POST("/checkpoints/:id/fork", func(c *gin.Context) { adapter.mutate(c, "fork", "", c.Param("id")) })
	return router
}
func principal(c *gin.Context) haven.Principal { return c.MustGet("principal").(haven.Principal) }
func (adapter *Adapter) authenticate(c *gin.Context) {
	header := c.GetHeader("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		writeError(c.Writer, api.Fail(api.ErrNotFound, "not found"))
		c.Abort()
		return
	}
	token := strings.TrimPrefix(header, "Bearer ")
	value, err := adapter.service.Authenticate(c.Request.Context(), token)
	if err != nil && c.Request.Method == http.MethodGet && c.FullPath() == "/v1/operations/:id" {
		value, err = adapter.service.AuthenticateOperation(c.Request.Context(), token)
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

func (adapter *Adapter) putTenant(c *gin.Context) {
	owner := principal(c)
	if !owner.Root {
		writeError(c.Writer, api.Fail(api.ErrForbidden, "root access required"))
		return
	}
	var value api.Tenant
	if err := readJSON(c.Writer, c.Request, &value); err != nil {
		writeError(c.Writer, err)
		return
	}
	if value.ID == "" || value.MaxSandboxes < 1 || value.MaxStorageMiB < 1 {
		writeError(c.Writer, api.Fail(api.ErrInvalid, "tenant id and positive quotas required"))
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
	_, err := adapter.service.PutTenant(c.Request.Context(), owner, haven.Tenant{ID: value.ID, Token: value.Token, MaxSandboxes: value.MaxSandboxes, MaxStorageMiB: value.MaxStorageMiB})
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	writeJSON(c.Writer, 201, value)
}

func (adapter *Adapter) metrics(c *gin.Context) {
	if !principal(c).Root {
		writeError(c.Writer, api.Fail(api.ErrForbidden, "root access required"))
		return
	}
	sandboxes, err := adapter.service.List(c.Request.Context(), principal(c))
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	counts := map[string]int{}
	for _, sandbox := range sandboxes {
		counts[sandbox.State]++
	}
	c.Header("Content-Type", "text/plain; version=0.0.4")
	for _, state := range []string{"provisioning", "running", "pausing", "paused", "resuming", "failed", "terminated"} {
		if _, err = fmt.Fprintf(c.Writer, "sandboxd_sandboxes{state=%q} %d\n", state, counts[state]); err != nil {
			return
		}
	}
}

func (adapter *Adapter) mutate(c *gin.Context, action, sandboxID, checkpointID string) {
	request := c.Request
	var command api.Command
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
		writeError(c.Writer, api.Fail(api.ErrInvalid, "Idempotency-Key required (max 256 bytes)"))
		return
	}
	actor := principal(c)
	if actor.SandboxID != "" && (command.SandboxID != actor.SandboxID || (action != "renew" && action != "release")) {
		writeError(c.Writer, api.Fail(api.ErrNotFound, "not found"))
		return
	}
	if action == "checkpoint_delete" {
		writeError(c.Writer, api.Fail(api.ErrInvalid, "unsupported sandbox action"))
		return
	}
	if actor.Root && action == "fork" {
		owner := ""
		if command.CheckpointID != "" {
			value, err := adapter.service.GetCheckpoint(request.Context(), actor, command.CheckpointID)
			if err != nil {
				writeError(c.Writer, err)
				return
			}
			owner = value.TenantID
		} else {
			value, err := adapter.service.Get(request.Context(), actor, command.SandboxID)
			if err != nil {
				writeError(c.Writer, err)
				return
			}
			owner = value.TenantID
		}
		if owner != actor.TenantID {
			writeError(c.Writer, api.Fail(api.ErrNotFound, "not found"))
			return
		}
	}
	payload, err := json.Marshal(command)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	var intent haven.Command
	if err = json.Unmarshal(payload, &intent); err != nil {
		writeError(c.Writer, err)
		return
	}
	if action == "delete_checkpoint" {
		intent.Action = "checkpoint_delete"
	}
	operation, err := adapter.service.Submit(request.Context(), principal(c), request.Header.Get("Idempotency-Key"), intent)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	if strings.Contains(request.Header.Get("Prefer"), "respond-async") {
		view, err := adapter.service.PublicOperation(request.Context(), principal(c), operation.ID)
		if err != nil {
			writeError(c.Writer, err)
			return
		}
		c.Header("Location", "/v1/operations/"+operation.ID)
		writeJSON(c.Writer, 202, view)
		return
	}
	operation, err = adapter.service.Wait(request.Context(), principal(c), operation.ID)
	if err != nil && operation.Status != "failed" {
		writeError(c.Writer, err)
		return
	}
	view, err := adapter.service.PublicOperation(request.Context(), principal(c), operation.ID)
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	if view.Error != nil {
		writeError(c.Writer, view.Error)
		return
	}
	respond(c.Writer, view.Result, nil)
}
func writeJSON(w http.ResponseWriter, status int, value any) { httpwire.JSON(w, status, value) }
func writeError(w http.ResponseWriter, err error)            { httpwire.Error(w, err) }
func readJSON(w http.ResponseWriter, r *http.Request, value any) error {
	return httpwire.ReadJSON(w, r, value)
}
func respond(w http.ResponseWriter, value any, err error) { httpwire.Respond(w, value, err) }
