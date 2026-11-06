package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"time"

	"github.com/elazarl/goproxy"
)

const (
	mitmMaxHeaderBytes    = 1 << 20
	mitmReadHeaderTimeout = 10 * time.Second
)

// serveTunnel keeps the original tracked/idle connection underneath TLS. The
// worker is registered under the same lock that prevents additions at revoke.
func (b *binding) serveTunnel(
	client net.Conn,
	ctx *goproxy.ProxyCtx,
	proxy *goproxy.ProxyHttpServer,
	t tunnel,
) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		_ = client.Close()
		return
	}
	b.workers.Add(1)
	b.mu.Unlock()
	defer b.workers.Done()
	defer func() { _ = client.Close() }()

	if err := client.SetDeadline(
		time.Now().Add(b.manager.opts.DialTimeout),
	); err != nil {
		return
	}
	if _, err := io.WriteString(
		client,
		"HTTP/1.1 200 Connection established\r\n\r\n",
	); err != nil {
		return
	}
	reader := bufio.NewReaderSize(client, 4096)
	first, err := reader.Peek(1)
	if err != nil {
		return
	}
	// A TLS handshake record starts with 0x16. Reject plaintext without parsing
	// any request line or headers, and retain any buffered handshake bytes.
	if first[0] != 0x16 {
		_ = b.audit("", "CONNECT", "deny", "tls_required")
		_ = deniedResponse(
			ctx.Req,
			http.StatusForbidden,
			"egress: request denied\n",
		).Write(client)
		return
	}
	cfg, err := b.tunnelTLSConfig(t, ctx)
	if err != nil {
		return
	}
	secure := tls.Server(&bufferedTunnelConn{Conn: client, reader: reader}, cfg)
	if err := secure.HandshakeContext(b.ctx); err != nil {
		return
	}
	if err := secure.SetDeadline(time.Time{}); err != nil {
		return
	}

	listener := &tunnelListener{conn: secure, done: make(chan struct{})}
	server := &http.Server{
		ReadHeaderTimeout: mitmReadHeaderTimeout,
		MaxHeaderBytes:    mitmMaxHeaderBytes,
		IdleTimeout:       b.manager.opts.IdleTimeout,
		ErrorLog:          discardLogger,
		BaseContext:       func(net.Listener) context.Context { return b.ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// ServeHTTP dispatches CONNECT before request hooks, so reject it here
			// to prevent recursive hijacking of the inner server.
			if r.Method == http.MethodConnect {
				_ = b.audit("", "CONNECT", "deny", "request")
				w.Header().Set("Connection", "close")
				http.Error(w, "egress: request denied", http.StatusForbidden)
				return
			}
			if r.URL.Scheme == "" && r.URL.Host == "" {
				r.URL.Scheme = "https"
				r.URL.Host = r.Host
			}
			r = r.WithContext(
				context.WithValue(r.Context(), tunnelContextKey{}, t),
			)
			proxy.ServeHTTP(w, r)
		}),
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				// StateClosed runs after the handler and connection cleanup. Only
				// then can Serve return and let revoke finish waiting for us.
				_ = listener.Close()
			}
		},
	}
	defer func() { _ = server.Close() }()
	_ = server.Serve(listener)
}

func (b *binding) tunnelTLSConfig(
	t tunnel,
	ctx *goproxy.ProxyCtx,
) (*tls.Config, error) {
	cfg, err := goproxy.TLSConfigFromCA(
		&b.manager.ca,
	)(
		net.JoinHostPort(t.host, t.port),
		ctx,
	)
	if err != nil {
		return nil, ErrDenied
	}
	cfg.MinVersion = tls.VersionTLS12
	cfg.NextProtos = []string{"http/1.1"}
	cfg.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		sni, err := canonicalHost(hello.ServerName)
		_, ipErr := netip.ParseAddr(t.host)
		if hello.ServerName == "" && ipErr == nil {
			sni, err = t.host, nil
		}
		if err != nil || sni != t.host || b.ctx.Err() != nil {
			return nil, ErrDenied
		}
		return nil, nil
	}
	return cfg, nil
}

type bufferedTunnelConn struct {
	net.Conn

	reader *bufio.Reader
}

func (c *bufferedTunnelConn) Read(
	p []byte,
) (int, error) {
	return c.reader.Read(p)
}

// tunnelListener hands an already tracked TLS connection to net/http once.
// The next Accept waits until StateClosed; Close always releases that waiter.
// No second inbound connection or parser goroutine is created here.
type tunnelListener struct {
	conn     net.Conn
	done     chan struct{}
	mu       sync.Mutex
	accepted bool
	once     sync.Once
}

func (l *tunnelListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	first := !l.accepted
	l.accepted = true
	l.mu.Unlock()
	select {
	case <-l.done:
		return nil, net.ErrClosed
	default:
	}
	if first {
		return l.conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *tunnelListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *tunnelListener) Addr() net.Addr { return l.conn.LocalAddr() }
