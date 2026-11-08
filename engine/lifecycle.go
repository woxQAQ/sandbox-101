package engine

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) observe(v *vmRecord) (Observation, error) {
	obs, err := e.opts.Backend.Inspect(e.ctx, v.Resource)
	if err != nil {
		if obs.Network {
			v.VM.NetworkActual = true
		}
		return obs, e.failClosed(e.ctx, v, err)
	}
	v.VM.NetworkActual = obs.Network
	if obs.Network {
		if !v.VM.NetworkEnabled {
			return obs, e.failClosed(
				e.ctx,
				v,
				api.Fail(
					api.Conflict,
					"interfaces active without durable network intent",
				),
			)
		}
		if err = e.verifyNetworkPolicy(e.ctx, v); err != nil {
			return obs, e.failClosed(e.ctx, v, err)
		}
	}
	v.VM.Verified = true
	v.VM.PID = obs.PID
	v.VM.StartTime = obs.StartTime
	v.VM.NetworkActual = obs.Network
	if !obs.Alive {
		v.VM.AgentReady = false
		if v.VM.Phase == "running" {
			v.VM.Phase = "stopped"
		}
	}
	if err = e.saveVM(*v); err != nil {
		return obs, err
	}
	return obs, nil
}

func (e *Engine) start(o *opRecord, v *vmRecord, restore bool) error {
	if err := e.checkImagePublication(v.Resource, nil); err != nil {
		return err
	}
	obs, err := e.observe(v)
	if err != nil {
		return err
	}
	if o.Journal.Resource == nil {
		if obs.Alive {
			return api.Fail(api.Conflict, "VM already has a live VMM")
		}
		if v.VM.Phase != "created" && v.VM.Phase != "stopped" {
			return api.Fail(api.Conflict, "VM resources are not ready")
		}
		if err = e.opts.Backend.Check(e.ctx, v.VM.Config); err != nil {
			return err
		}
		if err = e.opts.Backend.Network(e.ctx, v.Resource, false); err != nil {
			return err
		}
		// An installed policy never authorizes a different running instance.
		v.VM.NetworkEnabled = false
		v.VM.NetworkActual = false
		v.VM.NetworkPolicy = nil
		v.VM.NetworkPolicyInstance = ""
		v.Resource.Instance = newID("run-")
		v.Resource.RunDir = filepath.Join(
			v.Resource.Dir,
			"runs",
			v.Resource.Instance,
		)
		v.VM.Instance = v.Resource.Instance
		v.VM.VSockPath = filepath.Join(v.Resource.RunDir, "io", "vsock")
		v.VM.AgentReady = false
		o.Journal.Resource = &v.Resource
		if err = e.saveOp(o, "launch_intent"); err != nil {
			return err
		}
		if err = e.saveVM(*v); err != nil {
			return err
		}
	} else {
		v.Resource = *o.Journal.Resource
		v.VM.Instance = v.Resource.Instance
		v.VM.VSockPath = filepath.Join(v.Resource.RunDir, "io", "vsock")
		if err = e.saveVM(*v); err != nil {
			return err
		}
	}
	// An instance receipt/cgroup is authoritative even if the daemon died before
	// recording Launch's return value. The same intent never starts a second VMM.
	obs, err = e.observe(v)
	if err != nil {
		return err
	}
	if !obs.Alive {
		if o.Operation.Step != "launch_intent" &&
			o.Operation.Step != "restore_launch_intent" {
			return api.Fail(
				api.Unknown,
				"VMM exited during a previous attempt; use a new operation key after inspection",
			)
		}
		if err = e.opts.Backend.Network(e.ctx, v.Resource, false); err != nil {
			return err
		}
		if err = e.opts.Backend.Launch(e.ctx, v.Resource, restore); err != nil {
			return err
		}
		if err = e.saveOp(o, "launched"); err != nil {
			return err
		}
	}
	if err = e.opts.Backend.Boot(e.ctx, v.Resource, restore); err != nil {
		return err
	}
	if err = e.saveOp(o, "booted"); err != nil {
		return err
	}
	if err = e.opts.Backend.Agent(e.ctx, v.Resource, restore); err != nil {
		if !restore {
			netErr := e.opts.Backend.Network(e.ctx, v.Resource, false)
			stopErr := e.opts.Backend.Stop(e.ctx, v.Resource)
			after, inspectErr := e.opts.Backend.Inspect(e.ctx, v.Resource)
			if inspectErr == nil && !after.Alive {
				v.VM.Phase = "stopped"
				v.VM.PID = 0
				v.VM.StartTime = 0
				v.VM.NetworkActual = false
				v.VM.AgentReady = false
				_ = e.saveVM(*v)
			}
			return fmt.Errorf(
				"agent handshake: %w (network=%v stop=%v inspect=%v)",
				err,
				netErr,
				stopErr,
				inspectErr,
			)
		}
		_, _ = e.observe(v)
		return err
	}
	obs, err = e.observe(v)
	if err != nil {
		return err
	}
	if !obs.Alive {
		return api.Fail(api.Unknown, "VMM exited before completion")
	}
	if err = e.saveOp(o, "agent_ready"); err != nil {
		return err
	}
	v.VM.Phase = "running"
	v.VM.AgentReady = true
	v.VM.BlockedReason = ""
	v.VM.Verified = true
	return e.saveVM(*v)
}

func (e *Engine) stop(o *opRecord, v *vmRecord) error {
	if _, err := e.observe(v); err != nil {
		return err
	}
	if err := e.saveOp(o, "stop_intent"); err != nil {
		return err
	}
	// Always attempt process termination even when interface shutdown fails.
	netErr := e.opts.Backend.Network(e.ctx, v.Resource, false)
	if netErr == nil {
		v.VM.NetworkActual = false
	}
	stopErr := e.opts.Backend.Stop(e.ctx, v.Resource)
	obs, err := e.opts.Backend.Inspect(e.ctx, v.Resource)
	if err != nil {
		v.VM.Verified = false
		_ = e.saveVM(*v)
		return err
	}
	if obs.Alive {
		return api.Fail(api.ManualRecovery, "VMM exit unconfirmed: %v", stopErr)
	}
	v.VM.Phase = "stopped"
	v.VM.PID = 0
	v.VM.StartTime = 0
	v.VM.AgentReady = false
	v.VM.Verified = true
	if err = e.saveVM(*v); err != nil {
		return err
	}
	if netErr != nil {
		return netErr
	}
	return stopErr
}

// Reconcile verifies ownership before changing phase or touching network state.
// The Linux backend's pidfds reduce event latency; this periodic pass is the
// correctness path and also reattaches to processes after a daemon restart.
func (e *Engine) Reconcile(ctx context.Context) error {
	vms, err := e.ListVMs(ctx)
	if err != nil {
		return err
	}
	for _, vm := range vms {
		l := e.vmLock(vm.ID)
		if !l.TryLock() {
			continue
		}
		v, x := e.loadVM(ctx, vm.ID)
		if x != nil {
			l.Unlock()
			continue
		}
		if x = e.restoreVMFence(&v); x != nil {
			l.Unlock()
			return x
		}
		obs, x := e.opts.Backend.Inspect(ctx, v.Resource)
		if x != nil {
			if obs.Network {
				v.VM.NetworkActual = true
			}
			_ = e.failClosed(ctx, &v, x)
			l.Unlock()
			continue
		}
		v.VM.NetworkActual = obs.Network
		// Check direct rules even when only one interface is up. Unknown, stale,
		// or uncommitted policy reality always triggers interface shutdown.
		if obs.Network {
			if !v.VM.NetworkEnabled {
				x = api.Fail(
					api.Conflict,
					"interfaces active without durable network intent",
				)
			} else {
				x = e.verifyNetworkPolicy(ctx, &v)
			}
			if x != nil {
				_ = e.failClosed(ctx, &v, x)
				l.Unlock()
				continue
			}
		}
		v.VM.Verified = true
		v.VM.PID = obs.PID
		v.VM.StartTime = obs.StartTime
		v.VM.NetworkActual = obs.Network
		if !obs.Alive {
			if x = e.opts.Backend.Network(ctx, v.Resource, false); x != nil {
				v.VM.BlockedReason = x.Error()
			} else {
				v.VM.NetworkActual = false
			}
			if v.VM.Phase == "running" {
				v.VM.Phase = "stopped"
			}
			v.VM.AgentReady = false
		}
		if v.VM.OperationID != "" {
			o, x := e.loadOp(ctx, v.VM.OperationID)
			if x == nil && terminal(o.Operation.Status) &&
				!o.Journal.RestoreBlocked &&
				o.Journal.BlockedRestoreID == "" &&
				o.Journal.ResumeRequired {
				if !obs.Alive || !obs.Paused {
					x = nil
				} else {
					x = e.opts.Backend.Resume(ctx, v.Resource)
				}
				if x == nil {
					o.Journal.ResumeRequired = false
					_, _ = e.db.Exec(
						"UPDATE operations SET body=? WHERE id=?",
						encode(o),
						o.Operation.ID,
					)
					v.VM.OperationID = ""
				} else {
					v.VM.BlockedReason = x.Error()
				}
			}
		}
		_ = e.saveVM(v)
		l.Unlock()
	}
	return nil
}
