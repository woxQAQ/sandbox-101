package sandboxd

import (
	"errors"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) fork(o *Operation) error {
	if o.Step == "rollback" {
		var cause error
		if o.Error != nil {
			cause = o.Error
		} else {
			cause = api.Fail(api.Internal, "fork rolled back")
		}
		return s.rollbackFork(o, cause)
	}
	lockID := o.Command.SandboxID
	if o.Command.CheckpointID != "" {
		lockID = "checkpoint:" + o.Command.CheckpointID
	}
	unlock := s.lock(lockID)
	defer unlock()
	if o.SnapshotID == "" {
		if o.Command.CheckpointID != "" {
			cp, err := s.store.checkpoint(s.ctx, o.Command.CheckpointID)
			if err != nil {
				return s.rollbackFork(o, err)
			}
			o.SnapshotID = cp.SnapshotID
		} else {
			source, err := getSandbox(s.ctx, s.store.db, o.Command.SandboxID)
			if err != nil {
				return s.rollbackFork(o, err)
			}
			switch source.State {
			case "paused":
				o.SnapshotID = source.SnapshotID
			case "running":
				snap, err := s.engine(*o, "fork-snapshot", api.Request{Action: "snapshot", VMID: source.VMID, ExpectedInstance: source.Instance})
				if err != nil {
					return s.rollbackFork(o, err)
				}
				o.SnapshotID = snap.SnapshotID
				o.OwnedSnapshot = true
			default:
				return s.rollbackFork(o, api.Fail(api.Conflict, "source is not forkable"))
			}
		}
		if err := s.saveOperationReference(*o); err != nil {
			return err
		}
	}
	type branchResult struct {
		index int
		b     Sandbox
		err   error
	}
	results := make(chan branchResult, len(o.IDs))
	// Each branch has its own engine lock and backing. They can load concurrently.
	for i, id := range o.IDs {
		go func(i int, id string) {
			release := s.lock(id)
			defer release()
			b, err := getSandbox(s.ctx, s.store.db, id)
			if err == nil && b.State == "terminated" {
				err = api.Fail(api.Conflict, "branch was released")
			}
			if err == nil && b.State != "running" {
				if b.VMID == "" {
					vm, e := s.engine(*o, "fork-clone:"+id, api.Request{Action: "clone", SnapshotID: o.SnapshotID})
					b.VMID = vm.VMID
					if saveErr := s.saveSandboxReference(b); saveErr != nil {
						err = saveErr
					} else {
						err = e
					}
				}
				if err == nil {
					err = s.activate(*o, &b, true)
				}
			}
			results <- branchResult{i, b, err}
		}(i, id)
	}
	branches := make([]Sandbox, len(o.IDs))
	var failure error
	for range o.IDs {
		r := <-results
		branches[r.index] = r.b
		if r.err != nil {
			failure = errors.Join(failure, r.err)
		}
	}
	if failure != nil {
		return s.rollbackFork(o, failure)
	}
	if o.OwnedSnapshot {
		if err := s.retireSnapshot(*o, o.SnapshotID, "", ""); err != nil {
			return err
		}
	}
	o.Result.Sandboxes = branches
	return nil
}
func (s *Service) rollbackFork(o *Operation, cause error) error {
	if s.ctx.Err() != nil {
		return cause
	}
	o.Step = "rollback"
	o.Error = internalError(cause)
	if err := saveOp(s.ctx, s.store.db, *o); err != nil {
		return err
	}
	for _, id := range o.IDs {
		unlock := s.lock(id)
		b, err := getSandbox(s.ctx, s.store.db, id)
		if err == nil {
			err = s.release(o, &b)
		}
		unlock()
		if err != nil {
			return errors.Join(cause, err)
		}
	}
	if o.OwnedSnapshot {
		if err := s.retireSnapshot(*o, o.SnapshotID, "", ""); err != nil {
			return errors.Join(cause, err)
		}
	}
	return cause
}
