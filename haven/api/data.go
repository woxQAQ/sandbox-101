package api

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/woxQAQ/haven/dataplane"
	"github.com/woxQAQ/haven/httpwire"
	"github.com/woxQAQ/haven/types"
)

// connectSandbox handles the client HTTP Upgrade to the guest Workload portal.
// It delivers a raw duplex stream after the 101 response; bytes already read
// with the headers belong to the upgraded stream. gin's ResponseWriter always
// exposes Hijacker; a connection that cannot be hijacked fails Hijack itself.
func (h *handler) connectSandbox(c *gin.Context) {
	w := c.Writer
	r := c.Request
	id := c.Param("id")
	owner := principal(c)
	if !strings.EqualFold(r.Header.Get("Upgrade"), "sandbox-v1") || !hasToken(r.Header.Get("Connection"), "upgrade") {
		httpwire.NewFailure(http.StatusUpgradeRequired, types.CodeInvalid, "Connection: Upgrade and Upgrade: sandbox-v1 required").Write(w)
		return
	}
	if r.ProtoMajor != 1 {
		httpwire.NewFailure(http.StatusUnprocessableEntity, types.CodeIncompatible, "HTTP/1.1 Upgrade required").Write(w)
		return
	}
	if _, err := h.services.Dataplane.Ready(r.Context(), owner, id); err != nil {
		httpwire.Error(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	upstream, err := h.services.Dataplane.Connect(ctx, owner, id)
	cancel()
	if err != nil {
		httpwire.Error(w, err)
		return
	}
	defer func() {
		_ = upstream.Close()
	}()
	var raw net.Conn
	var buffer *bufio.ReadWriter
	downstream, err := h.services.Dataplane.Attach(r.Context(), owner, dataplane.HandoffRequest{
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
			httpwire.Error(w, err)
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
		if half, ok := destination.(interface{ CloseWrite() error }); ok {
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
func (h *handler) previewSandbox(c *gin.Context) {
	if _, ok := decode(c, "preview", c.Param("id"), ""); !ok {
		return
	}
	port, _ := strconv.Atoi(c.Request.URL.Query().Get("port"))
	ttl, _ := strconv.ParseInt(c.Request.URL.Query().Get("ttl_seconds"), 10, 64)
	if ttl == 0 {
		ttl = 300
	}
	path, err := h.services.Dataplane.PreviewURL(c.Request.Context(), principal(c), dataplane.PreviewRequest{
		SandboxID: c.Param("id"),
		Port:      port,
		TTL:       time.Duration(ttl) * time.Second,
	})
	httpwire.Respond(c.Writer, map[string]string{
		"url": path,
	}, err)
}

func (h *handler) preview(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preview/")
	capability, tail, _ := strings.Cut(path, "/")
	body, signature, ok := strings.Cut(capability, ".")
	if !ok {
		httpwire.NotFound().Write(w)
		return
	}
	target, err := h.services.Dataplane.ResolvePreview(r.Context(), body, signature)
	if err != nil {
		httpwire.Error(w, err)
		return
	}
	if _, err = h.services.Dataplane.Ready(r.Context(), target.Principal, target.SandboxID); err != nil {
		httpwire.Error(w, err)
		return
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return h.services.Dataplane.Forward(ctx, target.Principal, target.SandboxID, target.Port)
		},
	}
	defer transport.CloseIdleConnections()
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(&url.URL{
				Scheme: "http",
				Host:   net.JoinHostPort("127.0.0.1", strconv.Itoa(target.Port)),
			})
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
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.Header().Set("Referrer-Policy", "no-referrer")
			httpwire.NewFailure(http.StatusBadGateway, types.CodeInternal, "preview upstream unavailable").Write(w)
		},
	}
	proxy.ServeHTTP(w, r)
}
