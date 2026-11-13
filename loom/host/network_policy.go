package host

import (
	"net/netip"

	api "github.com/woxQAQ/haven/loom"
)

// No host-policy exemption mechanism is implemented. This list is enforced in
// the namespace even when an allowlist contains a broad prefix such as /0.
var directDeniedCIDRs = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
	"192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
	"203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "168.63.129.16/32",
}

func ValidateNetworkPolicy(p *api.NetworkPolicy) error {
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.MaxConnections < 1 || p.MaxConnections > 65535 {
		return api.Fail(api.Invalid, "network policy requires version 1 and max_connections in 1..65535")
	}
	if len(p.AllowedHosts) != 0 {
		return api.Fail(api.Incompatible, "direct policy cannot enforce allowed_hosts; use relay or an explicit IPv4 CIDR policy")
	}
	if len(p.AllowedCIDRs) > 256 {
		return api.Fail(api.Invalid, "at most 256 direct IPv4 prefixes are supported")
	}
	seen := make(map[netip.Prefix]bool)
	for _, raw := range p.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() || prefix.String() != raw {
			return api.Fail(api.Invalid, "direct destination must be a canonical IPv4 CIDR: %q", raw)
		}
		if seen[prefix] {
			return api.Fail(api.Invalid, "duplicate direct CIDR: %s", raw)
		}
		seen[prefix] = true
		for _, denied := range directDeniedCIDRs {
			d := netip.MustParsePrefix(denied)
			if prefix.Bits() >= d.Bits() && d.Contains(prefix.Addr()) {
				return api.Fail(api.Invalid, "direct CIDR is private, reserved or metadata: %s", raw)
			}
		}
	}
	return nil
}

func validateNetworkPolicy(policy *api.NetworkPolicy) error { return ValidateNetworkPolicy(policy) }
