package operation

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// equal follows the command contract for omitted environment and policy lists.
func (c Intent) equal(other Intent) bool {
	if !c.LeaseDeadline.Equal(other.LeaseDeadline) || !slices.Equal(c.Policy.AllowedHosts, other.Policy.AllowedHosts) || !slices.Equal(c.Policy.AllowedCIDRs, other.Policy.AllowedCIDRs) {
		return false
	}
	c.LeaseDeadline = other.LeaseDeadline
	c.Policy.AllowedHosts = nil
	other.Policy.AllowedHosts = nil
	c.Policy.AllowedCIDRs = nil
	other.Policy.AllowedCIDRs = nil
	if len(c.Env) == 0 {
		c.Env = nil
	}
	if len(other.Env) == 0 {
		other.Env = nil
	}
	return reflect.DeepEqual(c, other)
}

func insertCommand(ctx context.Context, q *dbgen.Queries, id string, c Intent, embedded bool) error {
	if err := store.RequireOne(q.InsertCommand(ctx, dbgen.InsertCommandParams{
		OperationID: id,
		ClaimAction: sql.NullString{
			String: c.Action,
			Valid:  embedded,
		},
		ObservedGeneration: c.ObservedGeneration,
		SandboxID:          c.SandboxID,
		CheckpointID:       c.CheckpointID,
		TemplateID:         c.TemplateID,
		TtlSeconds:         c.TTLSeconds,
		OnExpire:           c.OnExpire,
		AutoRenew:          c.AutoRenew,
		LeaseDeadline:      c.LeaseDeadline.Format(time.RFC3339Nano),
		PolicyMode:         c.Policy.Mode,
		MaxConnections:     int64(c.Policy.MaxConnections),
		BranchCount:        int64(c.Count),
		HasTemplate:        c.Template != nil,
		PolicyRuleCount:    int64(len(c.Policy.AllowedHosts) + len(c.Policy.AllowedCIDRs)),
		EnvironmentCount:   int64(len(c.Env)),
	})); err != nil {
		return err
	}
	if t := c.Template; t != nil {
		if err := store.RequireOne(q.InsertCommandTemplate(ctx, dbgen.InsertCommandTemplateParams{
			OperationID: id,
			ID:          t.ID,
			TenantID:    t.TenantID,
			Image:       t.Image,
			Vcpus:       int64(t.Shape.VCPUs),
			MemoryMib:   t.Shape.MemoryMiB,
			DiskMib:     t.Shape.DiskMiB,
			SnapshotID:  t.SnapshotID,
			State:       t.State,
			TargetWarm:  int64(t.TargetWarm),
			Warmup:      t.Warmup,
		})); err != nil {
			return err
		}
	}
	for kind, values := range map[string][]string{
		"host": c.Policy.AllowedHosts,
		"cidr": c.Policy.AllowedCIDRs,
	} {
		for i, v := range values {
			if err := store.RequireOne(q.InsertCommandRule(ctx, dbgen.InsertCommandRuleParams{
				OperationID: id,
				Kind:        kind,
				Position:    int64(i),
				Value:       v,
			})); err != nil {
				return err
			}
		}
	}
	for name, v := range c.Env {
		p := dbgen.InsertCommandEnvParams{
			OperationID: id,
			Name:        name,
			Value:       v.Value,
		}
		if v.Guest != nil {
			p.Guest = sql.NullBool{
				Bool:  *v.Guest,
				Valid: true,
			}
		}
		if v.Inject != nil {
			p.Header = sql.NullString{
				String: v.Inject.Header,
				Valid:  true,
			}
			p.Placeholder = sql.NullString{
				String: v.Inject.Placeholder,
				Valid:  true,
			}
			p.HostCount = sql.NullInt64{
				Int64: int64(len(v.Inject.Hosts)),
				Valid: v.Inject.Hosts != nil,
			}
		}
		if err := store.RequireOne(q.InsertCommandEnv(ctx, p)); err != nil {
			return err
		}
		if v.Inject != nil {
			for i, h := range v.Inject.Hosts {
				if err := store.RequireOne(q.InsertCommandHost(ctx, dbgen.InsertCommandHostParams{
					OperationID: id,
					Name:        name,
					Position:    int64(i),
					Host:        h,
				})); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func readCommand(ctx context.Context, q *dbgen.Queries, row dbgen.Operation) (Intent, error) {
	p, err := q.GetCommand(ctx, row.ID)
	if err != nil {
		return Intent{}, err
	}
	c := Intent{
		Action:             row.Action,
		ObservedGeneration: p.ObservedGeneration,
		SandboxID:          p.SandboxID,
		CheckpointID:       p.CheckpointID,
		TemplateID:         p.TemplateID,
		TTLSeconds:         p.TtlSeconds,
		OnExpire:           p.OnExpire,
		AutoRenew:          p.AutoRenew,
		Policy: types.Policy{
			Mode:           p.PolicyMode,
			MaxConnections: int(p.MaxConnections),
		},
		Count: int(p.BranchCount),
	}
	if p.ClaimAction.Valid != (row.RequestKind == "resource_command") {
		return c, fmt.Errorf("admitted command kind changed")
	}
	if p.ClaimAction.Valid {
		c.Action = p.ClaimAction.String
	}
	c.LeaseDeadline, err = time.Parse(time.RFC3339Nano, p.LeaseDeadline)
	if err != nil {
		return c, err
	}
	t, err := q.GetCommandTemplate(ctx, row.ID)
	if err == nil {
		c.Template = &types.Template{
			ID:       t.ID,
			TenantID: t.TenantID,
			Image:    t.Image,
			Shape: types.Shape{
				VCPUs:     int(t.Vcpus),
				MemoryMiB: t.MemoryMib,
				DiskMiB:   t.DiskMib,
			},
			SnapshotID: t.SnapshotID,
			State:      t.State,
			TargetWarm: int(t.TargetWarm),
			Warmup:     t.Warmup,
		}
	} else if err != sql.ErrNoRows {
		return c, err
	}
	if (c.Template != nil) != p.HasTemplate {
		return c, fmt.Errorf("%w: command template presence changed", errs.ErrManualRecovery)
	}
	rules, err := q.GetCommandRules(ctx, row.ID)
	if err != nil {
		return c, err
	}
	if int64(len(rules)) != p.PolicyRuleCount {
		return c, fmt.Errorf("%w: command policy rule count changed", errs.ErrManualRecovery)
	}
	for _, r := range rules {
		values := &c.Policy.AllowedHosts
		if r.Kind == "cidr" {
			values = &c.Policy.AllowedCIDRs
		}
		if r.Position != int64(len(*values)) {
			return c, fmt.Errorf("command policy rule order changed")
		}
		*values = append(*values, r.Value)
	}
	env, err := q.GetCommandEnv(ctx, row.ID)
	if err != nil {
		return c, err
	}
	if int64(len(env)) != p.EnvironmentCount {
		return c, fmt.Errorf("%w: command environment count changed", errs.ErrManualRecovery)
	}
	if len(env) > 0 {
		c.Env = make(map[string]types.EnvVar, len(env))
	}
	for _, e := range env {
		v := types.EnvVar{
			Value: e.Value,
		}
		if e.Guest.Valid {
			v.Guest = &e.Guest.Bool
		}
		if e.HostCount.Valid && !e.Header.Valid {
			return c, fmt.Errorf("%w: injection hosts have no descriptor", errs.ErrManualRecovery)
		}
		if e.Header.Valid {
			v.Inject = &types.Injection{
				Header:      e.Header.String,
				Placeholder: e.Placeholder.String,
			}
			if e.HostCount.Valid {
				v.Inject.Hosts = []string{}
			}
		}
		c.Env[e.Name] = v
	}
	hosts, err := q.GetCommandHosts(ctx, row.ID)
	if err != nil {
		return c, err
	}
	for _, h := range hosts {
		i := c.Env[h.Name].Inject
		if i == nil || i.Hosts == nil || h.Position != int64(len(i.Hosts)) {
			return c, fmt.Errorf("command injection host order changed")
		}
		i.Hosts = append(i.Hosts, h.Host)
	}
	for _, e := range env {
		if e.HostCount.Valid && int64(len(c.Env[e.Name].Inject.Hosts)) != e.HostCount.Int64 {
			return c, fmt.Errorf("%w: command injection host count changed", errs.ErrManualRecovery)
		}
	}
	return c, nil
}
