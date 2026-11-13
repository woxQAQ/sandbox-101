package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/woxQAQ/haven/api"
	"github.com/woxQAQ/haven/loom"
)

func operationTimes(ctx context.Context, q queryer, id string) (time.Time, time.Time, error) {
	var created, finished string
	if err := q.QueryRowContext(ctx, "SELECT created_at,finished_at FROM operation_metadata WHERE operation_id=?", id).Scan(&created, &finished); err != nil {
		return time.Time{}, time.Time{}, err
	}
	start, err := time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	var end time.Time
	if finished != "" {
		end, err = time.Parse(time.RFC3339Nano, finished)
	}
	return start, end, err
}

// commitPublications freezes the public terminal view in the workflow's own
// transaction. Later resource changes cannot rewrite a previously returned result.
// The transaction's original context still bounds its lifetime during shutdown.
func commitPublications(tx *sql.Tx) error {
	ctx := context.Background()
	rows, err := tx.QueryContext(ctx, `SELECT o.id FROM operations o LEFT JOIN operation_publications p ON p.operation_id=o.id WHERE o.status IN ('succeeded','failed') AND p.operation_id IS NULL AND (EXISTS(SELECT 1 FROM operation_credentials c WHERE c.operation_id=o.id) OR EXISTS(SELECT 1 FROM operation_steps s WHERE s.operation_id=o.id AND s.action='activate')) ORDER BY o.rowid`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			break
		}
		ids = append(ids, id)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	for _, id := range ids {
		view, err := projectOperation(ctx, tx, id)
		if err != nil {
			return err
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO operation_publications VALUES(?,?)", id, encode(view))); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func loadPublication(ctx context.Context, q queryer, id string) (api.Operation, error) {
	var view api.Operation
	var body []byte
	if err := q.QueryRowContext(ctx, "SELECT body FROM operation_publications WHERE operation_id=?", id).Scan(&body); err != nil {
		return view, err
	}
	if err := json.Unmarshal(body, &view); err != nil {
		return view, err
	}
	if view.ID != id || view.Result.OperationID != id || !terminal(view.Status) || view.Result.Tokens != nil {
		return view, loom.Fail(loom.ManualRecovery, "frozen delivery identity changed")
	}
	return view, nil
}

func isEffectCode(err error, code loom.Code) bool {
	var failure *loom.Error
	return errors.As(err, &failure) && failure.Code == code
}

func wireSandbox(value Sandbox) api.Sandbox {
	return api.Sandbox{ID: value.ID, TenantID: value.TenantID, State: value.State, VMID: value.VMID, Instance: value.Instance, SnapshotID: value.SnapshotID, TemplateID: value.TemplateID, ProvisionSource: value.ProvisionSource, Shape: api.Shape(value.Shape), Deadline: value.Deadline, PausedUntil: value.PausedUntil, TTLSeconds: value.TTLSeconds, OnExpire: value.OnExpire, AutoRenew: value.AutoRenew, Policy: api.Policy(value.Policy), RequiresSecrets: value.RequiresSecrets, ResumeAttempt: value.ResumeAttempt, Epoch: value.Epoch, OperationID: value.OperationID, LastError: value.LastError, CreatedAt: value.CreatedAt}
}

func wireTemplate(value templateRecord) api.Template {
	return api.Template{ID: value.ID, TenantID: value.TenantID, Image: value.Config.Image, Shape: api.Shape(value.Config.Shape), SnapshotID: value.SnapshotID, State: value.State, TargetWarm: value.TargetWarm, Warmup: value.Warmup}
}

func wireCheckpoint(value Checkpoint) api.Checkpoint {
	return api.Checkpoint{ID: value.ID, TenantID: value.TenantID, SandboxID: value.SandboxID, SnapshotID: value.SnapshotID, Shape: api.Shape(value.Shape), CreatedAt: value.CreatedAt}
}

func acceptedCommand(request []byte) (api.Command, error) {
	var command api.Command
	var envelope struct {
		Request json.RawMessage
		Claim   json.RawMessage
	}
	if err := json.Unmarshal(request, &envelope); err != nil {
		return command, err
	}
	payload := envelope.Request
	if len(envelope.Claim) > 0 {
		payload = envelope.Claim
	}
	if err := json.Unmarshal(payload, &command); err != nil {
		return command, err
	}
	if command.Action == "" {
		return command, loom.Fail(loom.ManualRecovery, "public operation lacks accepted command")
	}
	if command.Action == "checkpoint_delete" {
		command.Action = "delete_checkpoint"
	}
	return command, nil
}

func projectOperation(ctx context.Context, q queryer, id string) (api.Operation, error) {
	var out api.Operation
	var request, body []byte
	var scope string
	if err := q.QueryRowContext(ctx, "SELECT id,idem_key,scope_kind,scope_id,request,status,body FROM operations WHERE id=?", id).Scan(&out.ID, &out.Key, &scope, &out.TenantID, &request, &out.Status, &body); err != nil {
		return out, err
	}
	if scope != "tenant" {
		return out, loom.Fail(loom.NotFound, "operation not found")
	}
	command, commandErr := acceptedCommand(request)
	if commandErr != nil {
		return out, commandErr
	}
	out.Command = command
	var result operationResult
	if err := json.Unmarshal(body, &result); err != nil {
		return out, err
	}
	var err error
	out.CreatedAt, out.FinishedAt, err = operationTimes(ctx, q, id)
	if err != nil {
		return out, err
	}
	out.SnapshotID = result.SnapshotID
	out.IDs = append([]string(nil), result.SandboxIDs...)
	if len(out.IDs) == 0 && result.SandboxID != "" {
		out.IDs = []string{result.SandboxID}
	}
	out.Result.OperationID = id
	if !terminal(out.Status) {
		out.Step = "pending"
		return out, nil
	}
	out.Step = "done"
	if out.Status == "failed" {
		out.Step = "failed"
		code := api.ErrInternal
		message := result.Error
		prefix, rest, found := strings.Cut(message, ": ")
		if found {
			switch api.ErrorCode(prefix) {
			case api.ErrNotFound, api.ErrForbidden, api.ErrConflict, api.ErrInvalid, api.ErrExhausted, api.ErrIncompatible, api.ErrUnknown, api.ErrManualRecovery:
				code, message = api.ErrorCode(prefix), rest
			}
		}
		if message == "refill admission exhausted" {
			code = api.ErrExhausted
		}
		out.Error = &api.Error{Code: code, Message: message}
	}
	if out.Command.Action == "fork" && out.Status == "succeeded" {
		group, err := loadFork(ctx, q, id)
		if err != nil {
			return out, err
		}
		for index, childID := range group.State.Children {
			child, err := loadPublication(ctx, q, childID)
			if err != nil {
				return out, err
			}
			if child.Status != "succeeded" || child.Result.Sandbox == nil || child.Result.Sandbox.ID != group.Input.Targets[index].ID || child.Result.Sandbox.TenantID != group.Input.OwnerTenantID {
				return out, loom.Fail(loom.ManualRecovery, "fork historical branch delivery changed")
			}
			out.Result.Sandboxes = append(out.Result.Sandboxes, *child.Result.Sandbox)
			out.ChargeTenantID = child.Result.Sandbox.TenantID
		}
	} else if out.Command.Action != "checkpoint" && out.Command.Action != "delete_checkpoint" {
		for _, sandboxID := range out.IDs {
			sandbox, err := getSandbox(ctx, q, Principal{Root: true}, sandboxID)
			if err == nil {
				view := wireSandbox(sandbox)
				if out.Command.Action == "fork" {
					out.Result.Sandboxes = append(out.Result.Sandboxes, view)
				} else {
					out.Result.Sandbox = &view
				}
				out.ChargeTenantID = view.TenantID
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) && !isEffectCode(err, loom.NotFound) {
				return out, err
			}
		}
	}
	if result.CheckpointID != "" {
		var value Checkpoint
		var payload []byte
		if err = q.QueryRowContext(ctx, "SELECT body FROM checkpoint_bindings WHERE id=?", result.CheckpointID).Scan(&payload); err == nil {
			if err = json.Unmarshal(payload, &value); err != nil {
				return out, err
			}
			view := wireCheckpoint(value)
			out.Result.Checkpoint = &view
			out.ChargeTenantID = view.TenantID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
	}
	if result.TemplateID != "" {
		var payload []byte
		if err = q.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", result.TemplateID).Scan(&payload); err == nil {
			var value templateRecord
			if err = json.Unmarshal(payload, &value); err != nil {
				return out, err
			}
			view := wireTemplate(value)
			out.Result.Template = &view
			out.ChargeTenantID = view.TenantID
		} else if !errors.Is(err, sql.ErrNoRows) {
			return out, err
		}
	}
	return out, nil
}

func (service *Service) PublicOperation(ctx context.Context, principal Principal, id string) (api.Operation, error) {
	_, err := service.Operation(ctx, principal, id)
	if err != nil {
		return api.Operation{}, err
	}
	// Status and its frozen publication share one SQLite read snapshot. Async
	// completion between these reads must never look like lost history.
	tx, err := service.store.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return api.Operation{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var credential, status string
	if err = tx.QueryRowContext(ctx, "SELECT c.request_mac,o.status FROM operations o JOIN operation_credentials c ON c.operation_id=o.id WHERE o.id=? AND o.scope_kind='tenant'", id).Scan(&credential, &status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return api.Operation{}, loom.Fail(loom.NotFound, "operation not found")
		}
		return api.Operation{}, err
	}
	var out api.Operation
	if !terminal(status) {
		out, err = projectOperation(ctx, tx, id)
		if err != nil {
			return out, err
		}
	} else {
		out, err = loadPublication(ctx, tx, id)
		if errors.Is(err, sql.ErrNoRows) {
			return out, loom.Fail(loom.ManualRecovery, "public terminal result is missing")
		}
		if err != nil {
			return out, err
		}
	}
	if principal.SandboxID != "" && (out.Command.SandboxID != principal.SandboxID || (out.Command.Action != "renew" && out.Command.Action != "release")) {
		return api.Operation{}, loom.Fail(loom.NotFound, "operation not found")
	}
	if out.ID != id || out.Status != status {
		return out, loom.Fail(loom.ManualRecovery, "public result identity changed")
	}
	if out.Status == "succeeded" {
		sandboxes := out.Result.Sandboxes
		if out.Result.Sandbox != nil {
			sandboxes = append(append([]api.Sandbox(nil), sandboxes...), *out.Result.Sandbox)
		}
		for _, sandbox := range sandboxes {
			if sandbox.State == "terminated" {
				continue
			}
			if out.Result.Tokens == nil {
				out.Result.Tokens = make(map[string]string)
			}
			out.Result.Tokens[sandbox.ID] = service.sandboxToken(sandboxRecord{TenantID: sandbox.TenantID, ID: sandbox.ID})
		}
	}
	return out, nil
}
