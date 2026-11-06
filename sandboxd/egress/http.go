package egress

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/elazarl/goproxy"
)

// tunnel is immutable and is attached only after the CONNECT TLS handshake.
type tunnel struct {
	host string
	port string
}

type tunnelContextKey struct{}

func deniedResponse(
	r *http.Request,
	status int,
	message string,
) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Content-Type": []string{"text/plain; charset=utf-8"},
		},
		Body:          io.NopCloser(strings.NewReader(message)),
		ContentLength: int64(len(message)),
		Request:       r,
		Close:         true,
	}
}

func (b *binding) startHTTP() {
	tr := &http.Transport{
		Proxy:       nil,
		DialContext: b.dial,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    b.manager.opts.RootCAs,
		},
		TLSHandshakeTimeout:    b.manager.opts.DialTimeout,
		ResponseHeaderTimeout:  30 * time.Second,
		ExpectContinueTimeout:  time.Second,
		MaxResponseHeaderBytes: 1 << 20,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxConnsPerHost:        b.policy.max,
		// Fresh numeric dialing on every request prevents a pooled connection
		// from bypassing changed DNS answers or destination policy.
		ForceAttemptHTTP2: false,
	}
	b.transport = tr
	proxy := goproxy.NewProxyHttpServer()
	proxy.Logger = discardLogger
	proxy.Tr = tr
	proxy.KeepHeader = true
	proxy.NonproxyHandler = http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(
				w,
				"egress: absolute proxy URL required",
				http.StatusBadRequest,
			)
		},
	)
	proxy.ConnectDial = func(string, string) (net.Conn, error) { return nil, ErrDenied }
	proxy.OnRequest().
		HandleConnect(goproxy.FuncHttpsHandler(func(address string, ctx *goproxy.ProxyCtx) (*goproxy.ConnectAction, string) {
			host, port, err := authority(address, "")
			rh, rp, herr := authority(ctx.Req.Host, "")
			if err != nil || herr != nil || rh != host || rp != port ||
				b.resolveOnly(host) != nil {
				ctx.Resp = deniedResponse(
					ctx.Req,
					http.StatusForbidden,
					"egress: CONNECT denied\n",
				)
				_ = b.audit("", "CONNECT", "deny", "destination")
				return goproxy.RejectConnect, address
			}
			t := tunnel{host: host, port: port}
			return &goproxy.ConnectAction{
				Action: goproxy.ConnectHijack,
				Hijack: func(_ *http.Request, client net.Conn, c *goproxy.ProxyCtx) {
					b.serveTunnel(client, c, proxy, t)
				},
			}, net.JoinHostPort(host, port)
		}))
	proxy.OnRequest().
		DoFunc(func(r *http.Request, ctx *goproxy.ProxyCtx) (*http.Request, *http.Response) {
			deny := func(reason string) (*http.Request, *http.Response) {
				_ = b.audit("", safeMethod(r.Method), "deny", reason)
				return r, deniedResponse(
					r,
					http.StatusForbidden,
					"egress: request denied\n",
				)
			}
			if b.ctx.Err() != nil || r.URL == nil || r.URL.User != nil ||
				r.URL.Fragment != "" ||
				r.Method == http.MethodConnect ||
				r.Header.Get("Upgrade") != "" ||
				len(r.Trailer) != 0 ||
				r.Header.Get("Trailer") != "" {
				return deny("request")
			}
			if r.URL.Scheme != "http" && r.URL.Scheme != "https" {
				return deny("scheme")
			}
			defaultPort := "80"
			if r.URL.Scheme == "https" {
				defaultPort = "443"
			}
			host, port, err := authority(r.URL.Host, defaultPort)
			rh, rp, herr := authority(r.Host, defaultPort)
			if err != nil || herr != nil || rh != host || rp != port {
				return deny("host")
			}
			if t, ok := r.Context().Value(tunnelContextKey{}).(tunnel); ok {
				if r.URL.Scheme != "https" || t.host != host || t.port != port {
					return deny("tunnel_host")
				}
			} else if r.URL.Scheme != "http" {
				return deny("tls_required")
			}
			if b.resolveOnly(host) != nil {
				return deny("destination")
			}
			// Inspect placeholders before removing proxy/hop-by-hop headers.
			if err := b.validatePlaceholders(r, host); err != nil {
				return deny("placeholder")
			}
			stripHopHeaders(r.Header)
			r.Header.Set("Accept-Encoding", "identity")
			if err := b.inject(r, host); err != nil {
				return r, deniedResponse(
					r,
					http.StatusServiceUnavailable,
					"egress: audit unavailable\n",
				)
			}
			ctx.RoundTripper = goproxy.RoundTripperFunc(
				func(req *http.Request, _ *goproxy.ProxyCtx) (*http.Response, error) {
					requestCtx, cancel := context.WithCancel(req.Context())
					stop := context.AfterFunc(b.ctx, cancel)
					req = req.Clone(requestCtx)
					req.RequestURI = ""
					resp, err := tr.RoundTrip(req)
					if err != nil {
						stop()
						cancel()
						_ = b.audit(
							host,
							safeMethod(req.Method),
							"deny",
							"upstream",
						)
						return deniedResponse(
							req,
							http.StatusBadGateway,
							"egress: upstream unavailable\n",
						), nil
					}
					resp.Body = &cancelBody{
						ReadCloser: resp.Body,
						done:       func() { stop(); cancel() },
					}
					_ = b.audit(host, safeMethod(req.Method), "allow", "")
					return resp, nil
				},
			)
			return r, nil
		})
	proxy.OnResponse().
		DoFunc(func(resp *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
			if resp == nil {
				return deniedResponse(
					ctx.Req,
					http.StatusBadGateway,
					"egress: upstream unavailable\n",
				)
			}
			if resp.StatusCode == http.StatusSwitchingProtocols ||
				resp.Header.Get("Upgrade") != "" {
				_ = resp.Body.Close()
				return deniedResponse(
					ctx.Req,
					http.StatusBadGateway,
					"egress: upstream unavailable\n",
				)
			}
			return b.scrubResponse(resp)
		})
	b.server = &http.Server{
		Handler:           proxy,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       b.manager.opts.IdleTimeout,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          discardLogger,
		BaseContext:       func(net.Listener) context.Context { return b.ctx },
	}
	b.workers.Go(
		func() { ; _ = b.server.Serve(guardedListener{b.listeners[0], b}) },
	)
}

// Methods outside this set remain supported, but are not copied into audit.
func safeMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT":
		return method
	}
	return "OTHER"
}

func stripHopHeaders(h http.Header) {
	for _, line := range h.Values("Connection") {
		for key := range strings.SplitSeq(line, ",") {
			h.Del(strings.TrimSpace(key))
		}
	}
	for _, key := range []string{"Connection", "Proxy-Connection", "Proxy-Authorization", "Proxy-Authenticate", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
		h.Del(key)
	}
}

type cancelBody struct {
	io.ReadCloser

	done func()
}

func (b *cancelBody) Close() error { err := b.ReadCloser.Close(); b.done(); return err }
