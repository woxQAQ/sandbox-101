package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

type compiledPolicy struct {
	mode    string
	hosts   map[string]struct{}
	cidrs   []netip.Prefix
	max     int
	private bool
}

func compilePolicy(p Policy, allowPrivate bool) (compiledPolicy, error) {
	c := compiledPolicy{
		mode:    p.Mode,
		max:     p.MaxConnections,
		hosts:   make(map[string]struct{}),
		private: allowPrivate,
	}
	if c.mode != "none" && c.mode != "relay" && c.mode != "direct" {
		return c, errors.New("egress: unsupported network mode")
	}
	if c.max == 0 {
		c.max = 256
	}
	if c.max < 1 || c.max > 65536 {
		return c, errors.New("egress: invalid connection limit")
	}
	for _, h := range p.AllowedHosts {
		h, err := canonicalHost(h)
		if err != nil {
			return c, errors.New("egress: invalid allowed host")
		}
		c.hosts[h] = struct{}{}
	}
	for _, raw := range p.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || prefix.Addr().Is4In6() {
			return c, errors.New("egress: invalid allowed CIDR")
		}
		c.cidrs = append(c.cidrs, prefix.Masked())
	}
	return c, nil
}

// Canonical names deliberately exclude wildcards, zones, userinfo, Unicode and
// ambiguous numeric spellings. IDNs must be supplied as ASCII A-labels.
func canonicalHost(host string) (string, error) {
	if host == "" || strings.ContainsAny(host, "%/@\\\x00\r\n\t ") {
		return "", ErrDenied
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if addr, err := netip.ParseAddr(host); err == nil {
		if addr.Zone() != "" {
			return "", ErrDenied
		}
		return addr.Unmap().String(), nil
	}
	if len(host) > 253 {
		return "", ErrDenied
	}
	numeric := true
	for part := range strings.SplitSeq(host, ".") {
		if len(part) == 0 || len(part) > 63 || part[0] == '-' ||
			part[len(part)-1] == '-' {
			return "", ErrDenied
		}
		for _, ch := range part {
			if (ch < 'a' || ch > 'z') && (ch < '0' || ch > '9') && ch != '-' {
				return "", ErrDenied
			}
			if ch < '0' || ch > '9' {
				numeric = false
			}
		}
	}
	if numeric {
		return "", ErrDenied
	}
	return host, nil
}

func authority(raw, defaultPort string) (string, string, error) {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		if defaultPort == "" {
			return "", "", ErrDenied
		}
		host, port = raw, defaultPort
		if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
			host = host[1 : len(host)-1]
		}
	}
	host, err = canonicalHost(host)
	if err != nil {
		return "", "", ErrDenied
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return "", "", ErrDenied
	}
	return host, strconv.Itoa(p), nil
}

func prefixes(ss ...string) []netip.Prefix {
	ps := make([]netip.Prefix, len(ss))
	for i, s := range ss {
		ps[i] = netip.MustParsePrefix(s)
	}
	return ps
}

func contains(ps []netip.Prefix, ip netip.Addr) bool {
	for _, p := range ps {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// Non-unicast, metadata, documentation, benchmarks and IPv6 transition formats
// cannot be exempted. In particular, NAT64 never gets interpreted as public v6.
var forbidden = prefixes(
	"0.0.0.0/8",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.88.99.0/24",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"224.0.0.0/4",
	"240.0.0.0/4",
	"100.100.100.200/32",
	"168.63.129.16/32",
	"::/96",
	"::1/128",
	"64:ff9b::/96",
	"64:ff9b:1::/48",
	"100::/64",
	"2001::/23",
	"2001:db8::/32",
	"2002::/16",
	"3fff::/20",
	"fe80::/10",
	"ff00::/8",
)

var (
	internal = prefixes(
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
		"fc00::/7",
	)
	globalV6 = netip.MustParsePrefix("2000::/3")
)

func (p compiledPolicy) checkIP(ip netip.Addr) error {
	if !ip.IsValid() || ip.Zone() != "" {
		return ErrDenied
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || contains(forbidden, ip) {
		return ErrDenied
	}
	inCIDRs := contains(p.cidrs, ip)
	if len(p.cidrs) > 0 && !inCIDRs {
		return ErrDenied
	}
	if contains(internal, ip) {
		if p.private && inCIDRs {
			return nil
		}
		return ErrDenied
	}
	if ip.Is6() && !globalV6.Contains(ip) {
		return ErrDenied
	}
	return nil
}

func (b *binding) resolve(
	ctx context.Context,
	host string,
) ([]netip.Addr, error) {
	if len(b.policy.hosts) != 0 {
		if _, ok := b.policy.hosts[host]; !ok {
			return nil, ErrDenied
		}
	}
	var ips []netip.Addr
	if ip, err := netip.ParseAddr(host); err == nil {
		ips = []netip.Addr{ip}
	} else {
		var err error
		ips, err = b.manager.opts.Resolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, ErrDenied
		}
	}
	if len(ips) == 0 || len(ips) > 64 {
		return nil, ErrDenied
	}
	// Validate the ENTIRE answer before dialing even its first address.
	for _, ip := range ips {
		if err := b.policy.checkIP(ip); err != nil {
			return nil, err
		}
	}
	return ips, nil
}

func (b *binding) dial(
	ctx context.Context,
	network, address string,
) (net.Conn, error) {
	if network != "tcp" && network != "tcp4" && network != "tcp6" {
		return nil, ErrDenied
	}
	host, port, err := authority(address, "")
	if err != nil {
		return nil, ErrDenied
	}
	ctx, cancel := context.WithTimeout(ctx, b.manager.opts.DialTimeout)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	if b.ctx.Err() != nil {
		return nil, ErrRevoked
	}
	ips, err := b.resolve(ctx, host)
	if err != nil {
		return nil, ErrDenied
	}
	for _, ip := range ips {
		// The resolver is never consulted by the low-level dialer.
		c, err := b.manager.opts.DialContext(
			ctx,
			network,
			net.JoinHostPort(ip.Unmap().String(), port),
		)
		if err != nil {
			continue
		}
		t, err := b.track(
			&idleConn{Conn: c, timeout: b.manager.opts.IdleTimeout},
			false,
		)
		if err != nil {
			return nil, err
		}
		return t, nil
	}
	return nil, errors.New("egress: upstream unavailable")
}

// resolveOnly shares the same bounded, revoke-aware timeout as numeric dialing.
func (b *binding) resolveOnly(host string) error {
	ctx, cancel := context.WithTimeout(b.ctx, b.manager.opts.DialTimeout)
	defer cancel()
	_, err := b.resolve(ctx, host)
	return err
}
