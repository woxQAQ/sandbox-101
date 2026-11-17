package machine

import (
	"fmt"
	"net/netip"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func DirectNetworkPolicy(policy egress.Policy) (*loom.NetworkPolicy, error) {
	if policy.Mode != "direct" || len(policy.AllowedHosts) != 0 {
		return nil, fmt.Errorf("%w: direct mode requires IPv4 CIDRs without hostname rules", errs.ErrInvalid)
	}
	limit := policy.MaxConnections
	if limit == 0 {
		limit = 256
	}
	if limit < 1 || limit > 65535 || len(policy.AllowedCIDRs) > 256 {
		return nil, fmt.Errorf("%w: invalid direct connection budget", errs.ErrInvalid)
	}
	seen := map[netip.Prefix]bool{}
	for _, raw := range policy.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() || prefix.String() != raw || seen[prefix] {
			return nil, fmt.Errorf("%w: direct mode requires unique canonical IPv4 CIDRs", errs.ErrInvalid)
		}
		seen[prefix] = true
	}
	result := &loom.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   append([]string(nil), policy.AllowedCIDRs...),
		MaxConnections: limit,
	}
	return result, host.ValidateNetworkPolicy(result)
}
