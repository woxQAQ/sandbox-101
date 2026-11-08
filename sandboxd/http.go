package sandboxd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) Handler() http.Handler {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.HandleMethodNotAllowed = true
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.NoRoute(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			s.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		writeError(c.Writer, api.Fail(api.NotFound, "not found"))
	})
	router.NoMethod(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/v1/") {
			s.authenticate(c)
			if c.IsAborted() {
				return
			}
		}
		writeJSON(
			c.Writer,
			405,
			&api.Error{Code: api.Invalid, Message: "method not allowed"},
		)
	})
	router.Match(
		[]string{http.MethodGet, http.MethodHead},
		"/healthz",
		func(c *gin.Context) {
			writeJSON(c.Writer, 200, map[string]string{"status": "ok"})
		},
	)
	router.Match(
		[]string{http.MethodGet, http.MethodHead},
		"/metrics",
		s.authenticate,
		s.metrics,
	)
	router.Any("/preview/*path", gin.WrapF(s.preview))
	v1 := router.Group("/v1", s.authenticate)
	v1.POST("/tenants", s.putTenant)
	v1.POST("/drain", func(c *gin.Context) {
		if !c.MustGet("principal").(Principal).Root {
			writeJSON(
				c.Writer,
				403,
				&api.Error{Code: "forbidden", Message: "root access required"},
			)
			return
		}
		s.Drain()
		writeJSON(c.Writer, 200, map[string]bool{"draining": true})
	})
	v1.GET("/sandboxes", func(c *gin.Context) {
		v, err := s.List(
			c.Request.Context(),
			c.MustGet("principal").(Principal),
		)
		respond(c.Writer, v, err)
	})
	v1.GET("/templates", func(c *gin.Context) {
		v, err := s.Templates(
			c.Request.Context(),
			c.MustGet("principal").(Principal),
		)
		respond(c.Writer, v, err)
	})
	v1.GET("/checkpoints", func(c *gin.Context) {
		v, err := s.Checkpoints(
			c.Request.Context(),
			c.MustGet("principal").(Principal),
		)
		respond(c.Writer, v, err)
	})
	v1.GET("/sandboxes/:id", func(c *gin.Context) {
		v, err := s.Get(
			c.Request.Context(),
			c.MustGet("principal").(Principal),
			c.Param("id"),
		)
		respond(c.Writer, v, err)
	})
	v1.GET("/operations/:id", func(c *gin.Context) {
		v, err := s.Operation(
			c.Request.Context(),
			c.MustGet("principal").(Principal),
			c.Param("id"),
		)
		if err == nil {
			v.Result, err = s.resultTokens(
				c.MustGet("principal").(Principal),
				v.Result,
			)
		}
		respond(c.Writer, v, err)
	})
	v1.GET("/sandboxes/:id/connect", func(c *gin.Context) {
		s.relay(
			c.Writer,
			c.Request,
			c.MustGet("principal").(Principal),
			c.Param("id"),
		)
	})
	v1.POST("/claim", func(c *gin.Context) {
		s.mutate(c, "claim", "", "")
	})
	v1.POST("/templates", func(c *gin.Context) {
		s.mutate(c, "template_put", "", "")
	})
	v1.DELETE("/sandboxes/:id", func(c *gin.Context) {
		s.mutate(c, "release", c.Param("id"), "")
	})
	v1.POST("/sandboxes/:id/:action", func(c *gin.Context) {
		s.mutate(c, c.Param("action"), c.Param("id"), "")
	})
	v1.DELETE("/checkpoints/:id", func(c *gin.Context) {
		s.mutate(c, "delete_checkpoint", "", c.Param("id"))
	})
	v1.POST("/checkpoints/:id/fork", func(c *gin.Context) {
		s.mutate(c, "fork", "", c.Param("id"))
	})
	return router
}

func (s *Service) authenticate(c *gin.Context) {
	p, err := s.principal(c.Request)
	if err != nil && c.Request.Method == http.MethodGet &&
		c.FullPath() == "/v1/operations/:id" &&
		strings.HasPrefix(c.GetHeader("Authorization"), "Bearer ") {
		p, err = s.authenticateToken(
			c.Request.Context(),
			strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "),
			true,
		)
	}
	if err != nil {
		writeError(c.Writer, err)
		c.Abort()
		return
	}
	c.Set("principal", p)
}

func (s *Service) metrics(c *gin.Context) {
	if !c.MustGet("principal").(Principal).Root {
		writeJSON(
			c.Writer,
			403,
			&api.Error{Code: "forbidden", Message: "root access required"},
		)
		return
	}
	bs, err := s.store.sandboxes(c.Request.Context())
	if err != nil {
		writeError(c.Writer, err)
		return
	}
	counts := map[string]int{}
	for _, b := range bs {
		counts[b.State]++
	}
	c.Header("Content-Type", "text/plain; version=0.0.4")
	for _, state := range []string{"provisioning", "running", "pausing", "paused", "resuming", "failed", "terminated"} {
		if _, err := fmt.Fprintf(
			c.Writer,
			"sandboxd_sandboxes{state=%q} %d\n",
			state,
			counts[state],
		); err != nil {
			return
		}
	}
}

func (s *Service) putTenant(c *gin.Context) {
	p := c.MustGet("principal").(Principal)
	if !p.Root {
		writeJSON(
			c.Writer,
			403,
			&api.Error{Code: "forbidden", Message: "root access required"},
		)
		return
	}
	var t Tenant
	if err := readJSON(c.Writer, c.Request, &t); err != nil {
		writeError(c.Writer, err)
		return
	}
	t, err := s.PutTenant(c.Request.Context(), p, t)
	if err != nil {
		writeError(c.Writer, err)
	} else {
		writeJSON(c.Writer, 201, t)
	}
}

func (s *Service) principal(r *http.Request) (Principal, error) {
	h := r.Header.Get("Authorization")
	if !strings.HasPrefix(h, "Bearer ") {
		return Principal{}, api.Fail(api.NotFound, "not found")
	}
	p, err := s.Authenticate(r.Context(), strings.TrimPrefix(h, "Bearer "))
	if err == nil && p.Root && r.Header.Get("X-Tenant-ID") != "" {
		p.TenantID = r.Header.Get("X-Tenant-ID")
	}
	return p, err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var e *api.Error
	if !errors.As(err, &e) {
		e = &api.Error{Code: api.Internal, Message: "internal error"}
	}
	status := 500
	switch e.Code {
	case api.NotFound:
		status = 404
		e = &api.Error{Code: api.NotFound, Message: "not found"}
	case api.Invalid:
		status = 400
	case api.Conflict:
		status = 409
	case api.Exhausted:
		status = 429
	case api.Incompatible:
		status = 422
	case api.Unknown, api.ManualRecovery:
		status = 503
	}
	writeJSON(w, status, e)
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return api.Fail(api.Invalid, "invalid JSON request")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return api.Fail(api.Invalid, "request must contain one JSON value")
	}
	return nil
}

func (s *Service) mutate(
	c *gin.Context,
	action, sandboxID, checkpointID string,
) {
	w, r := c.Writer, c.Request
	p := c.MustGet("principal").(Principal)
	var command Command
	if r.Method == http.MethodPost {
		if err := readJSON(w, r, &command); err != nil {
			writeError(w, err)
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
		s.createPreview(w, r, p, sandboxID)
		return
	}
	o, err := s.Submit(r.Context(), p, r.Header.Get("Idempotency-Key"), command)
	if err != nil {
		writeError(w, err)
		return
	}
	if strings.Contains(r.Header.Get("Prefer"), "respond-async") {
		w.Header().Set("Location", "/v1/operations/"+o.ID)
		writeJSON(w, 202, o)
		return
	}
	result, err := s.Wait(r.Context(), p, o.ID)
	respond(w, result, err)
}

func respond(w http.ResponseWriter, v any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, v)
}

func (s *Service) relay(
	w http.ResponseWriter,
	r *http.Request,
	p Principal,
	id string,
) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "sandbox-v1") ||
		!hasToken(r.Header.Get("Connection"), "upgrade") {
		writeJSON(
			w,
			426,
			&api.Error{
				Code:    api.Invalid,
				Message: "Connection: Upgrade and Upgrade: sandbox-v1 required",
			},
		)
		return
	}
	if r.ProtoMajor != 1 {
		writeError(w, api.Fail(api.Incompatible, "HTTP/1.1 Upgrade required"))
		return
	}
	b, err := s.DataReady(r.Context(), p, id)
	if err != nil {
		writeError(w, err)
		return
	}
	unlock := s.lock(id)
	b, err = s.Get(r.Context(), p, id)
	if err != nil || b.State != "running" || !time.Now().Before(b.Deadline) {
		unlock()
		if err == nil {
			err = api.Fail(api.Conflict, "lease inactive")
		}
		writeError(w, err)
		return
	}
	vm, err := s.verifiedVM(r.Context(), b)
	if err != nil {
		unlock()
		writeError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	up, err := s.opts.Agent.Connect(ctx, vm)
	cancel()
	if err != nil {
		unlock()
		writeError(w, err)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		_ = up.Close()
		unlock()
		writeError(w, api.Fail(api.Incompatible, "HTTP/1.1 Upgrade required"))
		return
	}
	_, publication, err := s.lockDataAdmission(r.Context(), p, id)
	if err != nil {
		_ = up.Close()
		unlock()
		writeError(w, err)
		return
	}
	// Hijack only transfers local ownership. Both endpoints must be registered
	// before configure can commit a fence and close the admitted streams.
	down, rw, err := hj.Hijack()
	if err != nil {
		publication()
		_ = up.Close()
		unlock()
		return
	}
	untrackUp := s.track(id, up)
	untrackDown := s.track(id, down)
	publication()
	unlock()
	defer untrackUp()
	defer untrackDown()
	defer func() { _ = up.Close() }()
	defer func() { _ = down.Close() }()
	if _, err = rw.WriteString(
		"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n",
	); err != nil {
		return
	}
	if err = rw.Flush(); err != nil {
		return
	}
	done := make(chan struct{})
	go s.enforceLease(id, done, down, up)
	defer close(done)
	// Forward bytes already read with HTTP headers before handing raw sockets to io.Copy's splice path.
	if n := rw.Reader.Buffered(); n > 0 {
		if _, err = io.CopyN(up, rw.Reader, int64(n)); err != nil {
			return
		}
	}
	relayBytes(down, up)
}

func hasToken(value, want string) bool {
	for v := range strings.SplitSeq(value, ",") {
		if strings.EqualFold(strings.TrimSpace(v), want) {
			return true
		}
	}
	return false
}

func relayBytes(a, b net.Conn) {
	done := make(chan struct{}, 2)
	copyOne := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if h, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = h.CloseWrite()
		} else {
			_ = dst.Close()
		}
		done <- struct{}{}
	}
	go copyOne(a, b)
	go copyOne(b, a)
	<-done
	<-done
}

func (s *Service) enforceLease(
	id string,
	done <-chan struct{},
	conns ...net.Conn,
) {
	for {
		b, err := getSandbox(s.ctx, s.store.db, id)
		if err != nil || b.State != "running" ||
			!time.Now().Before(b.Deadline) {
			for _, c := range conns {
				_ = c.Close()
			}
			return
		}
		delay := min(time.Until(b.Deadline), time.Second)
		timer := time.NewTimer(delay)
		select {
		case <-done:
			timer.Stop()
			return
		case <-s.ctx.Done():
			timer.Stop()
			for _, c := range conns {
				_ = c.Close()
			}
			return
		case <-timer.C:
		}
	}
}
