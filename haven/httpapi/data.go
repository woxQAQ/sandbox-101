package httpapi

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

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/types"
)

func (adapter *Adapter) relay(w http.ResponseWriter, r *http.Request, principal authorization.Principal, id string) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "sandbox-v1") || !hasToken(r.Header.Get("Connection"), "upgrade") {
		writeJSON(w, 426, &types.ApiError{
			Code:    types.ErrInvalid,
			Message: "Connection: Upgrade and Upgrade: sandbox-v1 required",
		})
		return
	}
	if r.ProtoMajor != 1 {
		writeFailure(w, types.ApiError{
			Code:    types.ErrIncompatible,
			Message: "HTTP/1.1 Upgrade required",
		})
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeFailure(w, types.ApiError{
			Code:    types.ErrIncompatible,
			Message: "HTTP/1.1 Upgrade required",
		})
		return
	}
	if _, err := adapter.data.DataReady(r.Context(), principal, id); err != nil {
		writeError(w, err)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	upstream, err := adapter.data.Connect(ctx, principal, id)
	cancel()
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() {
		_ = upstream.Close()
	}()
	var raw net.Conn
	var buffer *bufio.ReadWriter
	downstream, err := adapter.data.AttachData(r.Context(), principal, id, upstream, func() (net.Conn, error) {
		var err error
		raw, buffer, err = hijacker.Hijack()
		return raw, err
	})
	if err != nil {
		if raw == nil {
			writeError(w, err)
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

func (adapter *Adapter) createPreview(w http.ResponseWriter, r *http.Request, principal authorization.Principal, id string) {
	port, _ := strconv.Atoi(r.URL.Query().Get("port"))
	ttl, _ := strconv.ParseInt(r.URL.Query().Get("ttl_seconds"), 10, 64)
	if ttl == 0 {
		ttl = 300
	}
	path, err := adapter.data.PreviewURL(r.Context(), principal, id, port, time.Duration(ttl)*time.Second)
	respond(w, map[string]string{
		"url": path,
	}, err)
}

func (adapter *Adapter) preview(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/preview/")
	capability, tail, _ := strings.Cut(path, "/")
	body, signature, ok := strings.Cut(capability, ".")
	if !ok {
		writeFailure(w, types.ApiError{
			Code:    types.ErrNotFound,
			Message: "not found",
		})
		return
	}
	target, err := adapter.data.ResolvePreview(r.Context(), body, signature)
	if err != nil {
		writeError(w, err)
		return
	}
	if _, err = adapter.data.DataReady(r.Context(), target.Principal, target.SandboxID); err != nil {
		writeError(w, err)
		return
	}
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return adapter.data.Forward(ctx, target.Principal, target.SandboxID, target.Port)
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
			writeJSON(w, 502, &types.ApiError{
				Code:    types.ErrInternal,
				Message: "preview upstream unavailable",
			})
		},
	}
	proxy.ServeHTTP(w, r)
}
