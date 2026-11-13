package haven

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
)

func (service *Service) inspectStoppedGenerations(generations []resourceStep) error {
	for _, generation := range generations {
		observation, err := service.opts.Backend.Inspect(service.ctx, generation.Input.Resource)
		if err != nil {
			return err
		}
		if observation.Alive || observation.Network {
			return loom.Fail(loom.Unknown, "known instance host removal unconfirmed")
		}
	}
	return nil
}

func verifyGenerationDeletion(intent loom.DeletionIntent, generations []resourceStep) error {
	for _, generation := range generations {
		for path, identity := range discardFixedNodes(generation) {
			node, ok := intent.Nodes[path]
			if !ok || node != identity {
				return fmt.Errorf("deletion receipt differs from retained generation proof")
			}
		}
	}
	return nil
}

func (service *Service) cleanupGenerations(ctx context.Context, tx *sql.Tx, source resourceStep, holderKind, holderID string) ([]resourceStep, error) {
	history, err := service.generationHistoryAt(ctx, tx, source, holderKind, holderID)
	if err != nil {
		return nil, err
	}
	return append(history, source), nil
}

func generationDeletionMembers(generations []resourceStep) []loom.VMDeletionMember {
	members := make([]loom.VMDeletionMember, 0, len(generations))
	for _, generation := range generations {
		members = append(members, loom.VMDeletionMember{Resource: generation.Input.Resource, DiskIdentity: generation.State.DiskIdentity, Materials: generation.State.Materials, Unpublished: generation.Status == "failed", OperationID: generation.OperationID})
	}
	return members
}

func (service *Service) retireGeneration(ctx context.Context, tx *sql.Tx, generation resourceStep) error {
	if generation.Input.Restore != nil {
		source := generation.Input.Restore.Snapshot
		var statements []struct {
			q    string
			args []any
		}
		if generation.State.BackingReady {
			statements = []struct {
				q    string
				args []any
			}{
				{"DELETE FROM backing WHERE path=? AND vm_id=? AND digest=?", []any{filepath.Join(generation.Input.Resource.RestoreDir, "memory-ranges"), generation.Candidate, source.Manifest.Files["ch/memory-ranges"]}},
				{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='backing' AND holder_id=?", []any{source.ID, generation.Input.BackingHolder}},
				{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", []any{generation.Input.TenantID, source.ID, "backing:" + generation.Input.BackingHolder}},
			}
		} else {
			statements = []struct {
				q    string
				args []any
			}{
				{"DELETE FROM source_leases WHERE snapshot_id=? AND operation_id=? AND step_key=?", []any{source.ID, generation.OperationID, generation.Key}},
				{"DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{source.ID, generation.OperationID + ":resource"}},
				{"DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='engine_dependency' AND holder_id=?", []any{generation.Input.TenantID, source.ID, generation.OperationID + ":resource"}},
			}
		}
		statements = append(statements, struct {
			q    string
			args []any
		}{"DELETE FROM reservations WHERE owner=?", []any{"restore:" + generation.OperationID}})
		for _, statement := range statements {
			if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
		if err := service.retireDeletedSnapshot(ctx, tx, *generation.Input.Restore); err != nil {
			return err
		}
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_versions WHERE id=? AND vm_id=? AND operation_id=? AND step_key=?", []any{generation.OperationID, generation.Candidate, generation.OperationID, generation.Key}},
		{"DELETE FROM instances WHERE id=? AND vm_id=? AND operation_id=?", []any{generation.Input.Resource.Instance, generation.Candidate, generation.OperationID}},
	} {
		if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}

func checkGenerationCounts(ctx context.Context, tx *sql.Tx, source resourceStep, generations []resourceStep) error {
	var expectedBacking int
	for _, generation := range generations {
		if generation.State.BackingReady {
			expectedBacking++
		}
	}
	checks := []struct {
		q    string
		want int
	}{
		{"SELECT COUNT(*) FROM instances WHERE vm_id=?", len(generations)},
		{"SELECT COUNT(*) FROM resource_versions WHERE vm_id=?", len(generations)},
		{"SELECT COUNT(*) FROM backing WHERE vm_id=?", expectedBacking},
	}
	for _, check := range checks {
		var n int
		if err := tx.QueryRowContext(ctx, check.q, source.Candidate).Scan(&n); err != nil || n != check.want {
			return fmt.Errorf("unrecorded VM generation or backing retained: %v", err)
		}
	}
	return nil
}
