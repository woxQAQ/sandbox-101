package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type lifecycleInput struct {
	CaptureFailures []generationRef `json:"capture_failures,omitempty"`
	PauseOperation  string          `json:"pause_operation,omitempty"`
	PriorFence      string          `json:"prior_fence,omitempty"`
	Rollback        *rollbackInput  `json:"rollback,omitempty"`
	SourceInstance  string          `json:"source_instance"`
	CheckpointID    string          `json:"checkpoint_id,omitempty"`
	RequestTenantID string          `json:"request_tenant_id"`
	OwnerTenantID   string          `json:"owner_tenant_id"`
	Command         Command         `json:"command"`
	Sandbox         sandboxRecord   `json:"sandbox"`
	Settings        sandboxSettings `json:"settings"`
	Source          generationRef   `json:"source"`
}
type (
	lifecycleState struct {
		Phase      string `json:"phase"`
		SnapshotID string `json:"snapshot_id,omitempty"`
	}
	lifecycleStep struct {
		OperationID, Hash, Status string
		Input                     lifecycleInput
		State                     lifecycleState
	}
	lifecycleAdmission struct {
		Upper     resourceParent
		Parent    Operation
		Input     lifecycleInput
		MAC, Role string
	}
	resourceLink struct {
		ChildID     string `json:"child_id"`
		Kind        string `json:"kind"`
		RequestHash string `json:"request_hash"`
		Candidate   string `json:"candidate"`
	}
)

func loadLifecycle(ctx context.Context, q queryer, id string) (lifecycleStep, error) {
	step := lifecycleStep{
		OperationID: id,
	}
	var payload, body []byte
	var action, candidate, instance string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='lifecycle'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &instance)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
		return step, fmt.Errorf("lifecycle input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != step.Input.Command.Action || candidate != step.Input.Sandbox.ID || instance != step.Input.Sandbox.Instance {
		return step, fmt.Errorf("lifecycle frozen identity changed")
	}
	return step, nil
}

// admitLifecycleLink commits the parent intent and its resource dependency in the
// resource admission transaction. It never polls another service or infers a
// missing resource receipt from a current query.
func (service *Service) admitLifecycleLink(ctx context.Context, tx *sql.Tx, admission *lifecycleAdmission, child Operation, kind, candidate, requestHash string) error {
	if admission == nil {
		return nil
	}
	parent := admission.Parent
	input := admission.Input
	existing, err := loadLifecycle(ctx, tx, parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		var fence string
		fenceErr := tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", input.Sandbox.ID).Scan(&fence)
		if input.Command.Action == "release" && ((fenceErr == nil && fence != input.PriorFence) || (errors.Is(fenceErr, sql.ErrNoRows) && input.PriorFence != "")) {
			return loom.Fail(loom.Conflict, "release source configuration fence changed")
		}
		if fenceErr == nil && input.Command.Action != "release" {
			return loom.Fail(loom.Conflict, "sandbox configuration is fenced")
		}
		if fenceErr != nil && !errors.Is(fenceErr, sql.ErrNoRows) {
			return fenceErr
		}
		current, err := loadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, input.Sandbox) {
			return loom.Fail(loom.Conflict, "sandbox changed before lifecycle admission")
		}
		settings, err := loadSettings(ctx, tx, current.ID)
		if err != nil {
			return err
		}
		if string(encode(settings)) != string(encode(input.Settings)) {
			return loom.Fail(loom.Conflict, "sandbox configuration changed before lifecycle admission")
		}
		if input.Command.Action == "release" && len(input.CaptureFailures) > 0 {
			if err = service.checkReleaseCaptures(ctx, tx, input); err != nil {
				return err
			}
		}
		frozen := encode(struct {
			Action  string
			Request Command
		}{input.Command.Action, input.Command})
		payload := encode(input)
		parent.Result.SandboxID = current.ID
		parent.Result.VMID = current.VMID
		parent.Result.SnapshotID = child.Result.SnapshotID
		if input.Rollback != nil {
			parent.Result.SnapshotID = input.Rollback.Checkpoint.SnapshotID
		}
		if input.Command.Action == "resume" {
			parent.Result.VMID = child.Result.VMID
			parent.Result.SnapshotID = input.Sandbox.SnapshotID
		}
		for _, statement := range []struct {
			q    string
			args []any
		}{
			{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
				parent.ID,
				"tenant",
				input.RequestTenantID,
				parent.Key,
				digest(frozen),
				frozen,
				"pending",
				encode(parent.Result),
			}},
			{"INSERT INTO operation_credentials VALUES(?,?)", []any{
				parent.ID,
				admission.MAC,
			}},
			{"INSERT INTO operation_steps VALUES(?,'lifecycle',?,?,?,?,?,'pending',?)", []any{
				parent.ID,
				input.Command.Action,
				digest(payload),
				payload,
				input.Sandbox.ID,
				input.Sandbox.Instance,
				encode(lifecycleState{
					Phase: "admitted",
				}),
			}},
			{"INSERT INTO configuration_fences VALUES(?,?) ON CONFLICT(sandbox_id) DO UPDATE SET operation_id=excluded.operation_id", []any{
				current.ID,
				parent.ID,
			}},
		} {
			if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
				return err
			}
		}
		if input.Rollback != nil {
			if err = service.admitRollbackLease(ctx, tx, parent, input); err != nil {
				return err
			}
		}
		if admission.Upper != nil {
			if err = admission.Upper.admitResource(ctx, service, tx, parent, "lifecycle", input.Sandbox.ID, digest(frozen)); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	} else {
		if terminal(existing.Status) || !reflect.DeepEqual(existing.Input, input) {
			return loom.Fail(loom.Conflict, "lifecycle no longer owns resource admission")
		}
		var fence string
		if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", input.Sandbox.ID).Scan(&fence); err != nil {
			return err
		}
		if fence != parent.ID {
			return loom.Fail(loom.Conflict, "lifecycle configuration was superseded")
		}
	}
	return insertResourceLink(ctx, tx, parent.ID, admission.Role, input.Sandbox.Instance, child, kind, candidate, requestHash)
}

func loadResourceLink(ctx context.Context, q queryer, parent, role string) (resourceLink, string, error) {
	var link resourceLink
	var payload []byte
	var hash, status, candidate string
	err := q.QueryRowContext(ctx, "SELECT input,input_hash,status,candidate_resource_id FROM operation_steps WHERE operation_id=? AND step_key=? AND action='resource_link'", parent, "link:"+role).Scan(&payload, &hash, &status, &candidate)
	if err != nil {
		return link, status, err
	}
	if digest(payload) != hash {
		return link, status, fmt.Errorf("resource link checksum changed")
	}
	if err = json.Unmarshal(payload, &link); err != nil {
		return link, status, err
	}
	var childHash string
	if err = q.QueryRowContext(ctx, "SELECT request_hash FROM operations WHERE id=?", link.ChildID).Scan(&childHash); err != nil {
		return link, status, err
	}
	if link.RequestHash != childHash || link.Candidate != candidate {
		return link, status, fmt.Errorf("resource link immutable child changed")
	}
	return link, status, nil
}

func (service *Service) completeResourceLink(parent, role string, link resourceLink) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, status, err := loadResourceLink(service.ctx, tx, parent, role)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, link) {
		return fmt.Errorf("resource link completion changed")
	}
	if terminal(status) {
		return nil
	}
	var childStatus string
	key := map[string]string{
		"cold_start":      "resource",
		"release":         "resource",
		"clone":           "resource",
		"restore":         "resource",
		"lifecycle":       "lifecycle",
		"capture_discard": "capture_discard",
		"capture_cleanup": "capture_cleanup",
		"capture":         "capture",
		"snapshot_delete": "snapshot_delete",
		"discard":         "discard",
		"stop":            "stop",
	}[link.Kind]
	if key == "" {
		return fmt.Errorf("unsupported resource link kind %q", link.Kind)
	}
	if err = tx.QueryRowContext(service.ctx, "SELECT status FROM operation_steps WHERE operation_id=? AND step_key=?", link.ChildID, key).Scan(&childStatus); err != nil {
		return err
	}
	if !terminal(childStatus) {
		return ErrInterrupted
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status=?,body=? WHERE operation_id=? AND step_key=? AND status='pending'", childStatus, encode("complete"), parent, "link:"+role)); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) runResourceLink(parent, role string) (resourceLink, error) {
	link, status, err := loadResourceLink(service.ctx, service.store.db, parent, role)
	if err != nil {
		return link, err
	}
	if status == "failed" {
		return link, loom.Fail(loom.Conflict, "retained lifecycle resource failed")
	}
	if status == "succeeded" {
		return link, nil
	}
	runErr := service.run(link.ChildID)
	if errors.Is(runErr, ErrInterrupted) {
		return link, runErr
	}
	if err = service.completeResourceLink(parent, role, link); err != nil {
		return link, errors.Join(err, runErr)
	}
	_, status, err = loadResourceLink(service.ctx, service.store.db, parent, role)
	if err != nil {
		return link, err
	}
	if status != "succeeded" {
		return link, loom.Fail(loom.Conflict, "retained lifecycle resource failed")
	}
	return link, nil
}
