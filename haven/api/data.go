package api

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/types"
)

type closeWriter interface {
	// CloseWrite sends EOF to the peer while leaving the read side open.
	CloseWrite() error
}

// connectSandbox handles the client HTTP Upgrade to the guest Workload portal.
// It delivers a raw duplex stream after the 101 response; bytes already read
// with the headers belong to the upgraded stream. gin's ResponseWriter always
// exposes Hijacker; a connection that cannot be hijacked fails Hijack itself.
func (h *Services) connectSandbox(c *gin.Context) {
	w := c.Writer
	r := c.Request
	id := c.Param("id")
	owner := principal(c)
	if !strings.EqualFold(r.Header.Get("Upgrade"), "sandbox-v1") || !hasToken(r.Header.Get("Connection"), "upgrade") {
		c.AbortWithStatusJSON(http.StatusUpgradeRequired, types.ApiError{
			Code:    types.CodeInvalid,
			Message: "Connection: Upgrade and Upgrade: sandbox-v1 required",
		})
		return
	}
	if r.ProtoMajor != 1 {
		c.AbortWithStatusJSON(http.StatusUnprocessableEntity, types.ApiError{
			Code:    types.CodeIncompatible,
			Message: "HTTP/1.1 Upgrade required",
		})
		return
	}
	if _, err := h.Dataplane.Ready(r.Context(), owner, id); err != nil {
		writeError(c, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	upstream, err := h.Dataplane.Connect(ctx, owner, id)
	cancel()
	if err != nil {
		writeError(c, err)
		return
	}
	defer func() {
		_ = upstream.Close()
	}()
	var raw net.Conn
	var buffer *bufio.ReadWriter
	downstream, err := h.Dataplane.Attach(r.Context(), owner, dataplane.HandoffRequest{
		SandboxID: id,
		Upstream:  upstream,
		Accept: func() (net.Conn, error) {
			var err error
			raw, buffer, err = w.Hijack()
			return raw, err
		},
	})
	if err != nil {
		if raw == nil {
			writeError(c, err)
		}
		return
	}
	defer func() {
		_ = downstream.Close()
	}()
	if _, err = buffer.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: sandbox-v1\r\n\r\n"); err != nil {
		return
	}
	if err = buffer.Flush(); err != nil {
		return
	}
	// Bytes read with the headers belong to the upgraded stream.
	if count := buffer.Reader.Buffered(); count > 0 {
		if _, err = io.CopyN(upstream, buffer.Reader, int64(count)); err != nil {
			return
		}
	}
	relayBytes(downstream, upstream)
}

func hasToken(value, want string) bool {
	for item := range strings.SplitSeq(value, ",") {
		if strings.EqualFold(strings.TrimSpace(item), want) {
			return true
		}
	}
	return false
}

func relayBytes(left, right net.Conn) {
	done := make(chan struct{}, 2)
	copyOne := func(destination, source net.Conn) {
		_, _ = io.Copy(destination, source)
		if half, ok := destination.(closeWriter); ok {
			_ = half.CloseWrite()
		} else {
			_ = destination.Close()
		}
		done <- struct{}{}
	}
	go copyOne(left, right)
	go copyOne(right, left)
	<-done
	<-done
}

// previewSandbox creates a signed preview URL bounded by a lease. It is a
// dataplane admission, not an accepted operation, so it takes no idempotency key.
func (h *Services) previewSandbox(c *gin.Context) {
	if _, ok := decode(c, "preview", c.Param("id"), ""); !ok {
		return
	}
	query, err := url.ParseQuery(c.Request.URL.RawQuery)
	if err != nil {
		invalidRequest(c, "invalid preview query")
		return
	}
	port, err := strconv.Atoi(query.Get("port"))
	if len(query["port"]) != 1 || err != nil || port < 1 || port > 65535 {
		invalidRequest(c, "port must be an integer from 1 to 65535")
		return
	}
	ttl := int64(300)
	if values, present := query["ttl_seconds"]; present {
		ttl, err = strconv.ParseInt(values[0], 10, 64)
		if len(values) != 1 || err != nil || ttl <= 0 || ttl > math.MaxInt64/int64(time.Second) {
			invalidRequest(c, "ttl_seconds must be a positive duration in whole seconds")
			return
		}
	}
	path, err := h.Dataplane.PreviewURL(c.Request.Context(), principal(c), dataplane.PreviewRequest{
		SandboxID: c.Param("id"),
		Port:      port,
		TTL:       time.Duration(ttl) * time.Second,
	})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, map[string]string{
		"url": path,
	})
}

type previewTargetKey struct{}

func newPreviewTransport() *http.Transport {
	return &http.Transport{
		MaxIdleConns:          32,
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			target := ctx.Value(previewTargetKey{}).(dataplane.PreviewTarget)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return target.Forward(ctx)
		},
	}
}

func (h *Services) preview(c *gin.Context, transport *http.Transport) {
	w := c.Writer
	r := c.Request
	path := strings.TrimPrefix(r.URL.Path, "/preview/")
	capability, tail, _ := strings.Cut(path, "/")
	body, signature, ok := strings.Cut(capability, ".")
	if !ok {
		notFound(c)
		return
	}
	target, err := h.Dataplane.AdmitPreview(r.Context(), body, signature)
	if err != nil {
		writeError(c, err)
		return
	}
	port := strconv.Itoa(target.Port)
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(&url.URL{
				Scheme: "http",
				Host:   net.JoinHostPort(target.PoolHost, port),
			})
			// URL.Host selects the isolated pool. Host is the guest-facing HTTP authority.
			request.Out.Host = net.JoinHostPort("127.0.0.1", port)
			request.Out.URL.Path = "/" + tail
			request.Out.URL.RawPath = ""
			request.Out.Header.Del("Authorization")
			request.Out.Header.Del("Cookie")
			request.SetXForwarded()
		},
		ModifyResponse: func(response *http.Response) error {
			response.Header.Set("Referrer-Policy", "no-referrer")
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			w.Header().Set("Referrer-Policy", "no-referrer")
			if admission, ok := errors.AsType[dataplane.AdmissionError](err); ok {
				writeError(c, admission)
				return
			}
			c.AbortWithStatusJSON(http.StatusBadGateway, types.ApiError{
				Code:    types.CodeInternal,
				Message: "preview upstream unavailable",
			})
		},
	}
	proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), previewTargetKey{}, target)))
}
