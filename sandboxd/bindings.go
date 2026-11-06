package sandboxd

import (
	"context"
	"sort"

	"github.com/woxQAQ/sandboxd/egress"
)

// bind reuses only the exact policy, VM instance and in-memory credential set.
// The manager itself refuses duplicate Bind calls to avoid silently weakening policy.
func (s *Service) bind(ctx context.Context, b Sandbox, vsockPath string) error {
	if s.opts.Egress == nil {
		return nil
	}
	s.mu.Lock()
	secrets := s.secrets[b.ID]
	s.mu.Unlock()
	parts := []string{"egress-binding-v1", vsockPath, string(encode(b.Policy))}
	names := make([]string, 0, len(secrets))
	for k := range secrets {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		v := secrets[k]
		parts = append(parts, k, string(encode(v)), v.Value)
	}
	fingerprint := s.store.sign(parts...)
	s.mu.Lock()
	current := s.bindings[b.ID]
	s.mu.Unlock()
	if current == fingerprint {
		return nil
	}
	if current != "" {
		if err := s.revoke(b.ID); err != nil {
			return err
		}
	}
	if err := s.opts.Egress.Bind(
		ctx,
		b.ID,
		b.TenantID,
		vsockPath,
		b.Policy,
		secrets,
	); err != nil {
		return err
	}
	s.mu.Lock()
	s.bindings[b.ID] = fingerprint
	s.mu.Unlock()
	return nil
}

func (s *Service) revoke(id string) error {
	if s.opts.Egress == nil {
		return nil
	}
	if err := s.opts.Egress.Revoke(id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.bindings, id)
	s.mu.Unlock()
	return nil
}

func (s *Service) Audit(ctx context.Context, event egress.AuditEvent) error {
	return s.store.audit(ctx, event)
}
