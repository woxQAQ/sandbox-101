package engine

import (
	"context"
	"errors"
	"net/netip"

	"github.com/woxQAQ/sandbox-engine/api"
)

// No host-policy exemption mechanism is implemented. This list is enforced in
// the namespace even when an allowlist contains a broad prefix such as /0.
var directDeniedCIDRs = []string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
	"192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
	"203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "168.63.129.16/32",
}

func validateNetworkPolicy(p *api.NetworkPolicy) error {
	if p == nil {
		return nil
	}
	if p.Version != 1 || p.MaxConnections < 1 || p.MaxConnections > 65535 {
		return api.Fail(
			api.Invalid,
			"network policy requires version 1 and max_connections in 1..65535",
		)
	}
	if len(p.AllowedHosts) != 0 {
		return api.Fail(
			api.Incompatible,
			"direct policy cannot enforce allowed_hosts; use relay or an explicit IPv4 CIDR policy",
		)
	}
	if len(p.AllowedCIDRs) > 256 {
		return api.Fail(
			api.Invalid,
			"at most 256 direct IPv4 prefixes are supported",
		)
	}
	seen := make(map[netip.Prefix]bool)
	for _, raw := range p.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() ||
			prefix.String() != raw {
			return api.Fail(
				api.Invalid,
				"direct destination must be a canonical IPv4 CIDR: %q",
				raw,
			)
		}
		if seen[prefix] {
			return api.Fail(api.Invalid, "duplicate direct CIDR: %s", raw)
		}
		seen[prefix] = true
		for _, denied := range directDeniedCIDRs {
			d := netip.MustParsePrefix(denied)
			if prefix.Bits() >= d.Bits() && d.Contains(prefix.Addr()) {
				return api.Fail(
					api.Invalid,
					"direct CIDR is private, reserved or metadata: %s",
					raw,
				)
			}
		}
	}
	return nil
}

func (e *Engine) verifyNetworkPolicy(ctx context.Context, v *vmRecord) error {
	if v.VM.NetworkPolicy == nil || v.VM.Instance == "" ||
		v.Resource.Instance != v.VM.Instance ||
		v.VM.NetworkPolicyInstance != v.VM.Instance {
		return api.Fail(
			api.Conflict,
			"direct activation requires a committed policy for the current instance",
		)
	}
	if err := validateNetworkPolicy(v.VM.NetworkPolicy); err != nil {
		return err
	}
	b, ok := e.opts.Backend.(NetworkPolicyBackend)
	if !ok {
		return api.Fail(
			api.Incompatible,
			"backend cannot verify direct network policy",
		)
	}
	return b.VerifyNetworkPolicy(ctx, v.Resource, v.VM.NetworkPolicy)
}

// failClosed preserves the diagnostic and attempts shutdown even when process or
// policy inspection failed. Network(false) independently verifies link ownership.
func (e *Engine) failClosed(
	ctx context.Context,
	v *vmRecord,
	cause error,
) error {
	err := e.opts.Backend.Network(ctx, v.Resource, false)
	v.VM.NetworkEnabled = false
	if err == nil {
		v.VM.NetworkActual = false
	}
	v.VM.Verified = false
	cause = errors.Join(cause, err)
	v.VM.BlockedReason = cause.Error()
	return errors.Join(cause, e.saveVM(*v))
}

func (e *Engine) installNetworkPolicy(o *opRecord, v *vmRecord) error {
	obs, err := e.observe(v)
	if err != nil {
		return err
	}
	if v.VM.NetworkEnabled || obs.Network {
		return api.Fail(
			api.Conflict,
			"disable all VM interfaces before replacing or revoking direct policy",
		)
	}
	b, ok := e.opts.Backend.(NetworkPolicyBackend)
	if !ok {
		return api.Fail(
			api.Incompatible,
			"backend cannot install direct network policy",
		)
	}
	// Invalidate the previous durable permission before touching the kernel. A
	// crash between the nft transaction and publication cannot enable old rules.
	v.VM.NetworkPolicy = nil
	v.VM.NetworkPolicyInstance = ""
	if err = e.saveVM(*v); err != nil {
		return err
	}
	if err = e.saveOp(o, "network_policy_intent"); err != nil {
		return err
	}
	p := o.Operation.Request.Policy
	if err = b.InstallNetworkPolicy(e.ctx, v.Resource, p); err != nil {
		return e.failClosed(e.ctx, v, err)
	}
	if err = e.saveOp(o, "network_policy_installed"); err != nil {
		return err
	}
	if err = b.VerifyNetworkPolicy(e.ctx, v.Resource, p); err != nil {
		return e.failClosed(e.ctx, v, err)
	}
	v.VM.NetworkPolicy = p
	if p != nil {
		v.VM.NetworkPolicyInstance = v.VM.Instance
	}
	v.VM.BlockedReason = ""
	if err = e.saveVM(*v); err != nil {
		return err
	}
	return e.saveOp(o, "network_policy_committed")
}

func (e *Engine) setNetwork(o *opRecord, v *vmRecord) error {
	enable := o.Operation.Request.NetworkEnabled
	if enable {
		obs, err := e.observe(v)
		if err != nil {
			return err
		}
		if !obs.Alive || v.VM.Phase != "running" || !v.VM.AgentReady {
			return e.failClosed(
				e.ctx,
				v,
				api.Fail(
					api.Conflict,
					"network requires a verified running VM with agent collaboration complete",
				),
			)
		}
		if err := e.verifyNetworkPolicy(e.ctx, v); err != nil {
			return e.failClosed(e.ctx, v, err)
		}
	}
	// Revocation does not depend on CH API reachability or policy readability.
	v.VM.NetworkEnabled = enable
	if err := e.saveVM(*v); err != nil {
		return err
	}
	if err := e.saveOp(o, "network_intent"); err != nil {
		return err
	}
	if err := e.opts.Backend.Network(e.ctx, v.Resource, enable); err != nil {
		return e.failClosed(e.ctx, v, err)
	}
	if err := e.saveOp(o, "network_applied"); err != nil {
		return err
	}
	v.VM.NetworkActual = enable
	v.VM.BlockedReason = ""
	return e.saveVM(*v)
}
