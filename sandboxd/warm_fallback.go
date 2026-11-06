package sandboxd

import (
	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) activateWarm(o Operation, b *Sandbox) error {
	v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
	if err != nil {
		return err
	}
	if b.Instance != "" && b.Instance != v.Instance {
		return api.Fail(api.Conflict, "warm instance changed before activation")
	}
	b.Instance = v.Instance
	if err = saveSandbox(s.ctx, s.store.db, *b); err != nil {
		return err
	}
	// A replacement VM must not reuse activation keys accepted for the warm VM.
	o.ID += ":warm"
	return s.activate(o, b, true)
}

func (s *Service) discardWarm(o Operation, b *Sandbox, t Template) error {
	if s.opts.Egress != nil {
		if err := s.revoke(b.ID); err != nil {
			return err
		}
	}
	if b.VMID != "" {
		v, err := s.opts.Engine.GetVM(s.ctx, b.VMID)
		if err != nil && !isCode(err, api.NotFound) {
			return err
		}
		if isCode(err, api.NotFound) {
			if reclaimErr := s.reclaimQuota(
				"vm_disk",
				b.VMID,
			); reclaimErr != nil {
				return reclaimErr
			}
		}
		if err == nil {
			if b.Instance != "" && b.Instance != v.Instance {
				return api.Fail(
					api.Conflict,
					"rejected warm instance changed; cleanup requires review",
				)
			}
			if _, err = s.engine(
				o,
				"warm-reject:"+b.ID,
				api.Request{
					Action:           "delete",
					VMID:             b.VMID,
					ExpectedInstance: v.Instance,
				},
			); err != nil {
				return err
			}
		}
	}
	b.VMID = ""
	b.Instance = ""
	b.ProvisionSource = "golden"
	if t.SnapshotID == "" {
		b.ProvisionSource = "cold"
	}
	return s.reserveWarmReplacement(o, *b)
}
