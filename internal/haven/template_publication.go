package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

func (service *Service) publishTemplate(ctx context.Context, tx *sql.Tx, step supplyStep, snapshot string) error {
	current, err := loadSupply(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || string(encode(current.Input)) != string(encode(step.Input)) || !reflect.DeepEqual(current.State, step.State) || terminal(current.Status) {
		return fmt.Errorf("template publication intent changed")
	}
	input := step.Input
	var owner, pointer string
	if err = tx.QueryRowContext(ctx, "SELECT tenant_id,operation_id FROM template_intents WHERE template_id=?", input.Template.ID).Scan(&owner, &pointer); err != nil {
		return err
	}
	if owner != input.Template.TenantID || pointer != step.OperationID {
		return fmt.Errorf("template publication was superseded")
	}
	previous, err := currentTemplate(ctx, tx, input.Template.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(previous, input.Previous) {
		return fmt.Errorf("template prior publication changed")
	}
	desired := input.Template
	desired.State = "ready"
	desired.SnapshotID = snapshot
	if snapshot != "" {
		source, err := service.snapshotPublication(ctx, tx, desired.TenantID, snapshot)
		if err != nil {
			return err
		}
		if !sameConfig(source.Snapshot.Manifest.Config, desired.Config) {
			return fmt.Errorf("template snapshot configuration changed")
		}
		if input.Command.Action == "template_put" {
			if input.Source == nil || !reflect.DeepEqual(*input.Source, source) {
				return fmt.Errorf("template import original publication changed")
			}
		} else {
			captured, status, err := loadResourceLink(ctx, tx, step.OperationID, "capture")
			if err != nil {
				return err
			}
			if status != "succeeded" || captured.ChildID != source.CaptureOperation {
				return fmt.Errorf("build snapshot lacks original capture completion")
			}
			released, status, err := loadResourceLink(ctx, tx, step.OperationID, "release")
			if err != nil {
				return err
			}
			if status != "succeeded" {
				return fmt.Errorf("build scratch retirement incomplete")
			}
			resource, err := loadStep(ctx, tx, released.ChildID)
			if err != nil {
				return err
			}
			if resource.Status != "succeeded" || resource.State.Phase != "complete" || resource.Input.SandboxID != input.SandboxID {
				return fmt.Errorf("build scratch deletion proof changed")
			}
		}
	}
	if previous == nil || previous.SnapshotID != snapshot {
		if snapshot != "" {
			if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO resource_refs VALUES('snapshot',?,'template',?)", snapshot, desired.ID)); err != nil {
				return err
			}
			if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", desired.TenantID, snapshot, "template:"+desired.ID)); err != nil {
				return err
			}
		}
		if previous != nil && previous.SnapshotID != "" {
			if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM resource_refs WHERE resource_kind='snapshot' AND resource_id=? AND holder_kind='template' AND holder_id=?", previous.SnapshotID, desired.ID)); err != nil {
				return err
			}
			if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM quota_refs WHERE tenant_id=? AND kind='snapshot' AND resource_key=? AND holder_kind='active_use' AND holder_id=?", desired.TenantID, previous.SnapshotID, "template:"+desired.ID)); err != nil {
				return err
			}
		}
	}
	if previous == nil || previous.Config.Image != desired.Config.Image {
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO resource_refs VALUES('image',?,'template',?)", desired.Config.Image, desired.ID)); err != nil {
			return err
		}
		if previous != nil {
			if err = requireWrite(tx.ExecContext(ctx, "DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='template' AND holder_id=?", previous.Config.Image, desired.ID)); err != nil {
				return err
			}
		}
	} else {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='template' AND holder_id=?", desired.Config.Image, desired.ID).Scan(&count); err != nil || count != 1 {
			return fmt.Errorf("template prior image responsibility missing: %v", err)
		}
	}
	if previous == nil {
		err = requireWrite(tx.ExecContext(ctx, "INSERT INTO templates VALUES(?,?,?)", desired.ID, desired.TenantID, encode(desired)))
	} else {
		err = requireWrite(tx.ExecContext(ctx, "UPDATE templates SET body=? WHERE id=? AND tenant=? AND body=?", encode(desired), desired.ID, desired.TenantID, encode(*previous)))
	}
	if err != nil {
		return err
	}
	var body []byte
	var status, scope, tenant string
	if err = tx.QueryRowContext(ctx, "SELECT body,status,scope_kind,scope_id FROM operations WHERE id=?", step.OperationID).Scan(&body, &status, &scope, &tenant); err != nil {
		return err
	}
	if terminal(status) || scope != "tenant" || tenant != input.RequestTenantID {
		return fmt.Errorf("template result authority changed")
	}
	var result operationResult
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	result.TemplateID = desired.ID
	result.SnapshotID = snapshot
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='step' AND holder_id=?", []any{
			desired.Config.Image,
			step.OperationID + ":supply",
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='supply' AND input_hash=? AND body=? AND status IN ('pending','running')", []any{
			encode(supplyState{
				FinishedAt: time.Now().UTC(),
				Phase:      "complete",
				SnapshotID: snapshot,
			}),
			step.OperationID,
			step.Hash,
			encode(step.State),
		}},
		{"UPDATE operations SET status='succeeded',body=? WHERE id=? AND status IN ('pending','running')", []any{
			encode(result),
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}
