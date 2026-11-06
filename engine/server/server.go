// Package server exposes the engine contract over a permission-protected UDS.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/woxQAQ/sandbox-engine/api"
)

func Handler(client api.Client) http.Handler {
	router := gin.New()
	router.RedirectTrailingSlash = false
	router.RedirectFixedPath = false
	router.UseRawPath = true
	_ = router.SetTrustedProxies(nil)
	router.NoRoute(func(c *gin.Context) {
		respond(c, nil, api.Fail(api.NotFound, "endpoint not found"))
	})
	v1 := router.Group("/v1")
	v1.POST("/operations", func(c *gin.Context) {
		var req api.Request
		d := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
		d.DisallowUnknownFields()
		err := d.Decode(&req)
		if err == nil {
			var extra any
			if x := d.Decode(&extra); x != io.EOF {
				err = api.Fail(api.Invalid, "exactly one JSON request required")
			}
		}
		if err != nil {
			respond(c, nil, api.Fail(api.Invalid, "invalid request: %v", err))
			return
		}
		result, err := client.Submit(c.Request.Context(), req)
		respond(c, result, err)
	})
	v1.GET("/operations", func(c *gin.Context) {
		result, err := client.FindOperation(c.Request.Context(), c.Query("key"))
		respond(c, result, err)
	})
	v1.GET("/operations/:id", func(c *gin.Context) {
		result, err := client.GetOperation(c.Request.Context(), c.Param("id"))
		respond(c, result, err)
	})
	v1.GET("/vms", func(c *gin.Context) {
		result, err := client.ListVMs(c.Request.Context())
		respond(c, result, err)
	})
	v1.GET("/vms/:id", func(c *gin.Context) {
		result, err := client.GetVM(c.Request.Context(), c.Param("id"))
		respond(c, result, err)
	})
	v1.GET("/snapshots", func(c *gin.Context) {
		result, err := client.ListSnapshots(c.Request.Context())
		respond(c, result, err)
	})
	v1.GET("/snapshots/:id", func(c *gin.Context) {
		result, err := client.GetSnapshot(c.Request.Context(), c.Param("id"))
		respond(c, result, err)
	})
	return router
}

func respond(c *gin.Context, result any, err error) {
	w := c.Writer
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		var a *api.Error
		if !errors.As(err, &a) {
			a = &api.Error{Code: api.Internal, Message: err.Error()}
		}
		status := http.StatusInternalServerError
		switch a.Code {
		case api.NotFound:
			status = http.StatusNotFound
		case api.Invalid:
			status = http.StatusBadRequest
		case api.Conflict:
			status = http.StatusConflict
		case api.Exhausted:
			status = http.StatusInsufficientStorage
		case api.Incompatible:
			status = http.StatusUnprocessableEntity
		case api.ManualRecovery:
			status = http.StatusServiceUnavailable
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(a)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

// Serve never removes a preexisting socket: its ownership cannot be proven by
// name alone. engined's exclusive writer lock permits its explicit stale-socket
// cleanup before calling this function.
func Serve(ctx context.Context, socket string, c api.Client) error {
	if err := os.MkdirAll(filepath.Dir(socket), 0700); err != nil {
		return err
	}
	l, err := net.Listen("unix", socket)
	if err != nil {
		return err
	}
	defer l.Close()
	if err = os.Chmod(socket, 0600); err != nil {
		return err
	}
	srv := &http.Server{Handler: Handler(c), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
		case <-done:
		}
	}()
	err = srv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
