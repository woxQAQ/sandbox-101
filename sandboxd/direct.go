package sandboxd

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
	"github.com/woxQAQ/sandboxd/egress"
)

func checkEgressPolicy(policy egress.Policy, secrets bool) error {
	if policy.Mode != "none" && policy.Mode != "relay" &&
		policy.Mode != "direct" {
		return Fail(
			ErrInvalid,
			"network mode must be none, relay or direct",
		)
	}
	if secrets && policy.Mode != "relay" {
		return Fail(ErrInvalid, "host credentials require relay mode")
	}
	if policy.Mode == "direct" {
		_, err := directPolicy(policy)
		return err
	}
	return nil
}

func directPolicy(policy egress.Policy) (*api.NetworkPolicy, error) {
	if policy.Mode != "direct" || len(policy.AllowedHosts) != 0 {
		return nil, Fail(
			ErrInvalid,
			"direct mode requires an IPv4 address policy without allowed_hosts",
		)
	}
	limit := policy.MaxConnections
	if limit == 0 {
		limit = 256
	}
	if limit < 1 || limit > 65535 || len(policy.AllowedCIDRs) > 256 {
		return nil, Fail(
			ErrInvalid,
			"invalid direct connection budget or CIDR count",
		)
	}
	seen := make(map[netip.Prefix]bool)
	for _, raw := range policy.AllowedCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() ||
			prefix.String() != raw ||
			seen[prefix] {
			return nil, Fail(
				ErrInvalid,
				"direct mode requires unique canonical IPv4 CIDRs",
			)
		}
		seen[prefix] = true
	}
	return &api.NetworkPolicy{
		Version:        1,
		AllowedCIDRs:   append([]string(nil), policy.AllowedCIDRs...),
		MaxConnections: limit,
	}, nil
}

// InstallDirect connects the host proxy manager's installation hook to durable
// engine operations. The caller already holds the sandbox lifecycle lock. It
// returns only after policy installation and never enables a network interface.
func (s *Service) InstallDirect(
	ctx context.Context,
	binding egress.Binding,
	policy egress.Policy,
) (func() error, error) {
	p, err := directPolicy(policy)
	if err != nil {
		return nil, err
	}
	b, err := getSandbox(ctx, s.store.db, binding.SandboxID)
	if err != nil {
		return nil, err
	}
	if b.TenantID != binding.TenantID || b.VMID == "" || b.RequiresSecrets {
		return nil, Fail(
			ErrConflict,
			"direct binding ownership or credential mode changed",
		)
	}
	vm, err := s.opts.Engine.GetVM(ctx, b.VMID)
	if err != nil {
		return nil, engineFailure(err)
	}
	if !vm.Verified || !vm.AgentReady || vm.Phase != "running" ||
		vm.Instance == "" ||
		vm.VSockPath != binding.VSockPath {
		return nil, Fail(
			ErrConflict,
			"direct binding VM is not the verified activation instance",
		)
	}
	generation := newID("direct_")
	request := func(step, action string, policy *api.NetworkPolicy) api.Request {
		return api.Request{
			Key:              "sbx-binding:" + generation + ":" + step,
			Action:           action,
			VMID:             vm.ID,
			ExpectedInstance: vm.Instance,
			Policy:           policy,
		}
	}
	var attempt uint64
	cleanup := func() error {
		// Service shutdown cancels s.ctx before closing egress. Cleanup must still
		// reach the engine, and every request remains conditioned on this instance.
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		current, lookupErr := s.opts.Engine.GetVM(cleanupCtx, vm.ID)
		if isEngineCode(lookupErr, api.NotFound) ||
			(lookupErr == nil && current.Instance != vm.Instance) {
			return nil
		}
		if lookupErr != nil {
			return engineFailure(lookupErr)
		}
		for _, step := range []struct{ suffix, action string }{{"off", "network"}, {"revoke", "network_policy"}} {
			op, cleanupErr := api.Wait(
				cleanupCtx,
				s.opts.Engine,
				request(
					fmt.Sprintf("cleanup:%d:%s", attempt, step.suffix),
					step.action,
					nil,
				),
			)
			if cleanupErr != nil {
				if op.Status == "failed" {
					attempt++
				}
				// An instance can change after the lookup. Confirm that specific
				// condition before treating a conflict as completed cleanup.
				if isEngineCode(cleanupErr, api.Conflict) ||
					isEngineCode(cleanupErr, api.NotFound) {
					current, lookupErr = s.opts.Engine.GetVM(cleanupCtx, vm.ID)
					if isEngineCode(lookupErr, api.NotFound) ||
						(lookupErr == nil && current.Instance != vm.Instance) {
						return nil
					}
				}
				return engineFailure(cleanupErr)
			}
		}
		return nil
	}
	if _, err = api.Wait(
		ctx,
		s.opts.Engine,
		request("off", "network", nil),
	); err != nil {
		return cleanup, engineFailure(err)
	}
	_, err = api.Wait(
		ctx,
		s.opts.Engine,
		request("policy", "network_policy", p),
	)
	return cleanup, engineFailure(err)
}
