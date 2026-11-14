package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type forkCleanupAdmission struct {
	Group forkStep
	Index int
}

func (parent *forkCleanupAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	group, err := loadFork(ctx, tx, parent.Group.OperationID)
	if err != nil {
		return err
	}
	if group.Hash != parent.Group.Hash || group.State.Phase != "compensating" || terminal(group.Status) || parent.Index < 0 || parent.Index >= len(group.Input.Targets) || child.Result.SandboxID != group.Input.Targets[parent.Index].ID || (kind != "lifecycle" && kind != "discard") {
		return loom.Fail(loom.Conflict, "fork cleanup no longer owns its target")
	}
	target := group.Input.Targets[parent.Index]
	current, err := loadSandbox(ctx, tx, target.ID, target.TenantID)
	if err != nil {
		return err
	}
	if current.Identity != target.Identity {
		return loom.Fail(loom.ManualRecovery, "fork cleanup birth ownership changed")
	}
	return insertResourceLink(ctx, tx, group.OperationID, fmt.Sprintf("retire:%d", parent.Index), "", child, kind, candidate, hash)
}

func (service *Service) retireForkBranches(step forkStep) error {
	for index, target := range step.Input.Targets {
		role := fmt.Sprintf("retire:%d", index)
		if _, _, err := loadResourceLink(service.ctx, service.store.db, step.OperationID, role); errors.Is(err, sql.ErrNoRows) {
			source, err := loadStep(service.ctx, service.store.db, step.State.Children[index])
			if err != nil {
				return err
			}
			parent := &forkCleanupAdmission{
				Group: step,
				Index: index,
			}
			if source.Status == "failed" {
				_, err = service.admitDiscard(service.ctx, DiscardRequest{
					TenantID:          target.TenantID,
					Key:               step.OperationID + ":discard:" + target.ID,
					SandboxID:         target.ID,
					FailedOperationID: source.OperationID,
					ExpectedInstance:  source.Input.Resource.Instance,
				}, parent)
			} else {
				_, err = service.admitLifecycle(service.ctx, Principal{
					Root:     true,
					TenantID: target.TenantID,
				}, step.OperationID+":release:"+target.ID, Command{
					Action:    "release",
					SandboxID: target.ID,
				}, parent)
			}
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if _, err := service.runResourceLink(step.OperationID, role); err != nil {
			return err
		}
	}
	return nil
}

// Failed births have no successful generation to release. Their exact discard
// receipt, current tombstone and original settings jointly authorize retiring the
// business fence; a missing VM by itself grants no handoff authority.
func retireForkDiscard(ctx context.Context, tx *sql.Tx, step forkStep, index int, link resourceLink) error {
	discard, err := loadDiscard(ctx, tx, link.ChildID)
	if err != nil {
		return err
	}
	target := step.Input.Targets[index]
	source := discard.Input.Source
	if discard.Status != "succeeded" || discard.State.Phase != "complete" || discard.Input.Request.SandboxID != target.ID || discard.Input.Request.TenantID != target.TenantID || source.OperationID != step.State.Children[index] || source.Input.SandboxID != target.ID || source.Input.TenantID != target.TenantID {
		return loom.Fail(loom.ManualRecovery, "fork discard receipt does not prove target retirement")
	}
	sandbox, err := loadSandbox(ctx, tx, target.ID, target.TenantID)
	if err != nil {
		return err
	}
	if sandbox.Identity != target.Identity || sandbox.SourceOperation != source.OperationID || sandbox.OperationID != discard.OperationID || sandbox.State != "terminated" || sandbox.VMID != source.Candidate || sandbox.Instance != "" {
		return loom.Fail(loom.ManualRecovery, "fork discard current tombstone changed")
	}
	var present int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM vms WHERE id=?", source.Candidate).Scan(&present); err != nil {
		return err
	}
	if present != 0 {
		return loom.Fail(loom.ManualRecovery, "fork discard retains a current VM")
	}
	settings, err := loadSettings(ctx, tx, target.ID)
	if err != nil {
		return err
	}
	expected := step.Input.Settings
	expected.Generation = source.OperationID
	if !reflect.DeepEqual(settings, expected) {
		return loom.Fail(loom.ManualRecovery, "undelivered fork settings changed")
	}
	next := sandbox
	next.VMID = ""
	next.OperationID = ""
	desired := settings
	desired.ActiveEpoch = ""
	desired.Instance = ""
	desired.Deadline = time.Time{}
	desired.PausedUntil = time.Time{}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM configuration_fences WHERE sandbox_id=? AND operation_id=?", []any{
			target.ID,
			source.OperationID,
		}},
		{"UPDATE sandbox_settings SET body=? WHERE sandbox_id=? AND body=?", []any{
			encode(desired),
			target.ID,
			encode(settings),
		}},
		{"UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='terminated' AND body=?", []any{
			encode(next),
			target.ID,
			target.TenantID,
			encode(sandbox),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}
