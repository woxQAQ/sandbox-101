package egress

import (
	"context"
	"net"
	"strconv"

	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

// Resolution belongs to our guarded dialer, not the library's single-IP DNS
// resolver. This also preserves the original host for allowlist evaluation.
type deferredResolver struct{}

func (deferredResolver) Resolve(
	ctx context.Context,
	_ string,
) (context.Context, net.IP, error) {
	return ctx, nil, nil
}

type socksRules struct{ b *binding }

func (s socksRules) Allow(
	ctx context.Context,
	req *socks5.Request,
) (context.Context, bool) {
	allowed := req.Command == statute.CommandConnect && len(s.b.secrets) == 0 &&
		s.b.ctx.Err() == nil
	if !allowed {
		_ = s.b.audit("", "SOCKS5", "deny", "socks_policy")
	}
	return ctx, allowed
}

func (b *binding) startSOCKS() {
	server := socks5.NewServer(
		socks5.WithLogger(socks5.NewLogger(discardLogger)),
		socks5.WithResolver(deferredResolver{}),
		socks5.WithRule(socksRules{b}),
		socks5.WithDialAndRequest(
			func(_ context.Context, network, _ string, req *socks5.Request) (net.Conn, error) {
				host := req.RawDestAddr.FQDN
				if host == "" {
					host = req.RawDestAddr.IP.String()
				}
				host, err := canonicalHost(host)
				if err != nil {
					return nil, ErrDenied
				}
				c, err := b.dial(
					b.ctx,
					network,
					net.JoinHostPort(host, strconv.Itoa(req.RawDestAddr.Port)),
				)
				if err != nil {
					_ = b.audit("", "SOCKS5", "deny", "destination")
					return nil, ErrDenied
				}
				_ = b.audit(host, "SOCKS5", "allow", "")
				return c, nil
			},
		),
	)
	b.workers.Go(func() {
		ln := guardedListener{b.listeners[1], b}
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.workers.Go(func() { ; _ = server.ServeConn(c) })
		}
	})
}
