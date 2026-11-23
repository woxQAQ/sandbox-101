package operation

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// publish freezes public resource fields before the terminal transaction commits.
// The accepted command and ordered result identities already belong to the operation.
func publish(ctx context.Context, tx *sql.Tx, id string, view types.Operation) error {
	q := dbgen.New(tx)
	params := dbgen.InsertPublicationParams{
		OperationID:    id,
		ResponseHash:   Digest(Encode(view)),
		ChargeTenantID: view.ChargeTenantID,
	}
	if t := view.Result.Template; t != nil {
		params.TemplateID = sql.NullString{
			String: t.ID,
			Valid:  true,
		}
		params.TemplateState = t.State
	}
	if err := store.RequireOne(q.InsertPublication(ctx, params)); err != nil {
		return err
	}
	if c := view.Result.Checkpoint; c != nil {
		if err := store.RequireOne(q.InsertPublishedCheckpoint(ctx, dbgen.InsertPublishedCheckpointParams{
			OperationID:  id,
			CheckpointID: c.ID,
			TenantID:     c.TenantID,
			SandboxID:    c.SandboxID,
			SnapshotID:   c.SnapshotID,
			Vcpus:        int64(c.Shape.VCPUs),
			MemoryMib:    c.Shape.MemoryMiB,
			DiskMib:      c.Shape.DiskMiB,
			CreatedAt:    c.CreatedAt.Format(time.RFC3339Nano),
		})); err != nil {
			return err
		}
	}
	delivered := view.Result.Sandboxes
	if view.Result.Sandbox != nil {
		delivered = []types.Sandbox{
			*view.Result.Sandbox,
		}
	}
	for index, s := range delivered {
		if err := insertPublishedSandbox(ctx, q, id, int64(index), s); err != nil {
			return err
		}
	}
	_, err := readPublication(ctx, tx, id)
	return err
}

func insertPublishedSandbox(ctx context.Context, q *dbgen.Queries, id string, index int64, s types.Sandbox) error {
	if err := store.RequireOne(q.InsertPublishedSandbox(ctx, dbgen.InsertPublishedSandboxParams{
		OperationID:        id,
		Position:           index,
		SandboxID:          s.ID,
		TenantID:           s.TenantID,
		State:              s.State,
		VmID:               s.VMID,
		SnapshotID:         s.SnapshotID,
		Instance:           s.Instance,
		TemplateID:         s.TemplateID,
		ProvisionSource:    s.ProvisionSource,
		Vcpus:              int64(s.Shape.VCPUs),
		MemoryMib:          s.Shape.MemoryMiB,
		DiskMib:            s.Shape.DiskMiB,
		Deadline:           s.Deadline.Format(time.RFC3339Nano),
		PausedUntil:        s.PausedUntil.Format(time.RFC3339Nano),
		TtlSeconds:         s.TTLSeconds,
		OnExpire:           s.OnExpire,
		AutoRenew:          s.AutoRenew,
		PolicyMode:         s.Policy.Mode,
		MaxConnections:     int64(s.Policy.MaxConnections),
		RequiresSecrets:    s.RequiresSecrets,
		ResumeAttempt:      int64(s.ResumeAttempt),
		Epoch:              s.Epoch,
		SandboxOperationID: s.OperationID,
		LastError:          s.LastError,
		CreatedAt:          s.CreatedAt.Format(time.RFC3339Nano),
	})); err != nil {
		return err
	}
	for kind, rules := range map[string][]string{
		"host": s.Policy.AllowedHosts,
		"cidr": s.Policy.AllowedCIDRs,
	} {
		for position, value := range rules {
			if err := store.RequireOne(q.InsertPublishedRule(ctx, dbgen.InsertPublishedRuleParams{
				OperationID:     id,
				SandboxPosition: index,
				Kind:            kind,
				Position:        int64(position),
				Value:           value,
			})); err != nil {
				return err
			}
		}
	}
	return nil
}

func readPublishedObjects(ctx context.Context, q *dbgen.Queries, record dbgen.OperationPublication, view *types.Operation) error {
	if record.TemplateVersionID.Valid {
		v, err := q.GetPublishedTemplate(ctx, record.TemplateVersionID.String)
		if err != nil {
			return err
		}
		view.Result.Template = &types.Template{
			ID:       v.TemplateID,
			TenantID: v.TenantID,
			State:    record.TemplateState,
			Image:    v.Image,
			Shape: types.Shape{
				VCPUs:     int(v.Vcpus),
				MemoryMiB: v.MemoryMib,
				DiskMiB:   v.DiskMib,
			},
			Warmup:     v.Warmup,
			SnapshotID: v.SnapshotID,
			TargetWarm: int(v.TargetWarm),
		}
	}
	c, err := q.GetPublishedCheckpoint(ctx, record.OperationID)
	if err == nil {
		created, err := time.Parse(time.RFC3339Nano, c.CreatedAt)
		if err != nil {
			return err
		}
		view.Result.Checkpoint = &types.Checkpoint{
			ID:         c.CheckpointID,
			TenantID:   c.TenantID,
			SandboxID:  c.SandboxID,
			SnapshotID: c.SnapshotID,
			Shape: types.Shape{
				VCPUs:     int(c.Vcpus),
				MemoryMiB: c.MemoryMib,
				DiskMiB:   c.DiskMib,
			},
			CreatedAt: created,
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	rows, err := q.ListPublishedSandboxes(ctx, record.OperationID)
	if err != nil {
		return err
	}
	var sandboxes []types.Sandbox
	for index, row := range rows {
		s, err := publishedSandbox(row)
		if err != nil {
			return err
		}
		if row.Position != int64(index) || !slices.Contains(view.IDs, s.ID) || s.Epoch == "" || s.CreatedAt.IsZero() {
			return fmt.Errorf("frozen sandbox delivery identity is incomplete")
		}
		sandboxes = append(sandboxes, s)
	}
	rules, err := q.ListPublishedRules(ctx, record.OperationID)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.SandboxPosition < 0 || rule.SandboxPosition >= int64(len(sandboxes)) {
			return fmt.Errorf("frozen sandbox rule owner missing")
		}
		policy := &sandboxes[rule.SandboxPosition].Policy
		values := &policy.AllowedHosts
		if rule.Kind == "cidr" {
			values = &policy.AllowedCIDRs
		}
		if int64(len(*values)) != rule.Position {
			return fmt.Errorf("frozen sandbox rule sequence changed")
		}
		*values = append(*values, rule.Value)
	}
	if view.Command.Action == "fork" {
		view.Result.Sandboxes = sandboxes
	} else if len(sandboxes) == 1 {
		view.Result.Sandbox = &sandboxes[0]
	} else if len(sandboxes) > 1 {
		return fmt.Errorf("unexpected sandbox delivery count")
	}
	return nil
}

func publishedSandbox(row dbgen.PublishedSandbox) (types.Sandbox, error) {
	s := types.Sandbox{
		ID:              row.SandboxID,
		TenantID:        row.TenantID,
		State:           row.State,
		VMID:            row.VmID,
		SnapshotID:      row.SnapshotID,
		Instance:        row.Instance,
		TemplateID:      row.TemplateID,
		ProvisionSource: row.ProvisionSource,
		Shape: types.Shape{
			VCPUs:     int(row.Vcpus),
			MemoryMiB: row.MemoryMib,
			DiskMiB:   row.DiskMib,
		},
		TTLSeconds: row.TtlSeconds,
		OnExpire:   row.OnExpire,
		AutoRenew:  row.AutoRenew,
		Policy: types.Policy{
			Mode:           row.PolicyMode,
			MaxConnections: int(row.MaxConnections),
		},
		RequiresSecrets: row.RequiresSecrets,
		ResumeAttempt:   uint64(row.ResumeAttempt),
		Epoch:           row.Epoch,
		OperationID:     row.SandboxOperationID,
		LastError:       row.LastError,
	}
	var err error
	s.Deadline, err = time.Parse(time.RFC3339Nano, row.Deadline)
	if err != nil {
		return s, err
	}
	s.PausedUntil, err = time.Parse(time.RFC3339Nano, row.PausedUntil)
	if err != nil {
		return s, err
	}
	s.CreatedAt, err = time.Parse(time.RFC3339Nano, row.CreatedAt)
	return s, err
}
