package haven

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

func insertResourceLink(ctx context.Context, tx *sql.Tx, parent, role, instance string, child Operation, kind, candidate, hash string) error {
	link := resourceLink{ChildID: child.ID, Kind: kind, RequestHash: hash, Candidate: candidate}
	payload := encode(link)
	return requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_steps VALUES(?,?,'resource_link',?,?,?,?,'pending',?)", parent, "link:"+role, digest(payload), payload, candidate, instance, encode("admitted")))
}

func (service *Service) reserveTemplateIntent(ctx context.Context, tx *sql.Tx, parent Operation, input supplyInput) error {
	current, err := currentTemplate(ctx, tx, input.Template.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, input.Previous) {
		return loom.Fail(loom.Conflict, "template catalog changed before admission")
	}
	if current != nil && current.TenantID != input.Template.TenantID {
		return loom.Fail(loom.Conflict, "template owner cannot change")
	}
	var owner string
	err = tx.QueryRowContext(ctx, "SELECT tenant_id FROM template_intents WHERE template_id=?", input.Template.ID).Scan(&owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if owner != "" && owner != input.Template.TenantID {
		return loom.Fail(loom.Conflict, "template permanent owner cannot change")
	}
	return requireWrite(tx.ExecContext(ctx, "INSERT INTO template_intents VALUES(?,?,?) ON CONFLICT(template_id) DO UPDATE SET operation_id=excluded.operation_id WHERE tenant_id=excluded.tenant_id", input.Template.ID, input.Template.TenantID, parent.ID))
}

func (service *Service) admitSupplyResource(ctx context.Context, tx *sql.Tx, admission *supplyAdmission, child Operation, kind, candidate, hash string) error {
	if admission == nil {
		return nil
	}
	input, parent := admission.Input, admission.Parent
	existing, err := loadSupply(ctx, tx, parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if input.Command.Action != "template_build" && input.Command.Action != "refill" {
			return loom.Fail(loom.Invalid, "new supply resource requires build or refill intent")
		}
		if input.CreatedAt.IsZero() {
			return loom.Fail(loom.Invalid, "supply admission timestamp missing")
		}
		if input.Command.Action == "refill" {
			blocked, err := refillBackoff(ctx, tx, input.Template.ID, input.CreatedAt)
			if err != nil {
				return err
			}
			if blocked {
				return loom.Fail(loom.Exhausted, "template refill backoff is active")
			}
			current, err := loadOwnedTemplate(ctx, tx, input.Template.TenantID, input.Template.ID)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(current, input.Template) || current.TargetWarm < 1 || current.SnapshotID == "" {
				return loom.Fail(loom.Conflict, "refill template version changed")
			}
			var count int
			if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM pool_slots WHERE template_id=?", current.ID).Scan(&count); err != nil {
				return err
			}
			if count >= current.TargetWarm {
				return loom.Fail(loom.Exhausted, "template pool occupation reached target")
			}
		}
		frozen := encode(struct {
			Action  string
			Request Command
		}{input.Command.Action, input.Command})
		payload := encode(input)
		parent.Result.TemplateID = input.Template.ID
		parent.Result.SandboxID = input.SandboxID
		parent.Result.VMID = child.Result.VMID
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{parent.ID, "tenant", input.RequestTenantID, parent.Key, digest(frozen), frozen, "pending", encode(parent.Result)}},
			{"INSERT INTO operation_credentials VALUES(?,?)", []any{parent.ID, admission.MAC}},
			{"INSERT INTO operation_steps VALUES(?,'supply',?,?,?,?,?,'pending',?)", []any{parent.ID, input.Command.Action, digest(payload), payload, input.Template.ID, "", encode(supplyState{Phase: "admitted"})}},
			{"INSERT INTO resource_refs VALUES('image',?,'step',?)", []any{input.Template.Config.Image, parent.ID + ":supply"}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
		if input.Command.Action == "template_build" {
			if err = service.reserveTemplateIntent(ctx, tx, parent, input); err != nil {
				return err
			}
		} else {
			resource, err := loadStep(ctx, tx, child.ID)
			if err != nil {
				return err
			}
			pool := poolReservation{Template: input.Template, SandboxID: input.SandboxID, VMID: child.Result.VMID, Source: generationRef{OperationID: child.ID, InputHash: resource.Hash}, CreatedAt: input.CreatedAt}
			if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO pool_slots VALUES(?,?,?,?,'pending',?)", parent.ID, input.Template.ID, input.Template.Version, child.Result.VMID, encode(pool))); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	} else {
		if terminal(existing.Status) || !reflect.DeepEqual(existing.Input, input) {
			return loom.Fail(loom.Conflict, "supply no longer owns resource admission")
		}
		if input.Command.Action == "template_build" && admission.Role != "release" && admission.Role != "discard" {
			var owner, pointer string
			if err = tx.QueryRowContext(ctx, "SELECT tenant_id,operation_id FROM template_intents WHERE template_id=?", input.Template.ID).Scan(&owner, &pointer); err != nil {
				return err
			}
			if owner != input.Template.TenantID || pointer != parent.ID {
				return loom.Fail(loom.Conflict, "template intent was superseded")
			}
		}
	}
	return insertResourceLink(ctx, tx, parent.ID, admission.Role, "", child, kind, candidate, hash)
}
