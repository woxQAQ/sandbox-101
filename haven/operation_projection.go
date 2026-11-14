package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/types"
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

func acceptedCommand(request []byte) (types.Command, error) {
	var command types.Command
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
		return command, fmt.Errorf("%w: public operation lacks accepted command", ErrManualRecovery)
	}
	if command.Action == "checkpoint_delete" {
		command.Action = "delete_checkpoint"
	}
	return command, nil
}

// projectOperation builds a business view in the caller's SQLite snapshot.
// Terminal views are frozen before commit; later queries read that publication.
// Diagnostics, HTTP errors and bearer tokens are outside the persisted view.
func projectOperation(ctx context.Context, q queryer, id string) (types.Operation, error) {
	var out types.Operation
	var request, body []byte
	var scope string
	if err := q.QueryRowContext(ctx, "SELECT id,idem_key,scope_kind,scope_id,request,status,body FROM operations WHERE id=?", id).Scan(&out.ID, &out.Key, &scope, &out.TenantID, &request, &out.Status, &body); err != nil {
		return out, err
	}
	if scope != "tenant" {
		return out, fmt.Errorf("%w: operation not found", ErrNotFound)
	}
	command, err := acceptedCommand(request)
	if err != nil {
		return out, err
	}
	out.Command = command
	var result operationResult
	if err := json.Unmarshal(body, &result); err != nil {
		return out, err
	}
	out.CreatedAt, out.FinishedAt, err = operationTimes(ctx, q, id)
	if err != nil {
		return out, err
	}
	out.SnapshotID = result.SnapshotID
	out.IDs = append([]string(nil), result.SandboxIDs...)
	if len(out.IDs) == 0 && result.SandboxID != "" {
		out.IDs = []string{
			result.SandboxID,
		}
	}
	out.Result.OperationID = id
	if !terminal(out.Status) {
		out.Step = "pending"
		return out, nil
	}
	out.Step = "done"
	if out.Status == "failed" {
		out.Step = "failed"
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
				return out, fmt.Errorf("%w: fork historical branch delivery changed", ErrManualRecovery)
			}
			out.Result.Sandboxes = append(out.Result.Sandboxes, *child.Result.Sandbox)
			out.ChargeTenantID = child.Result.Sandbox.TenantID
		}
	} else if out.Command.Action != "checkpoint" && out.Command.Action != "delete_checkpoint" {
		for _, sandboxID := range out.IDs {
			sandbox, err := getSandbox(ctx, q, Principal{
				Root: true,
			}, sandboxID)
			if err == nil {
				view := wireSandbox(sandbox)
				if out.Command.Action == "fork" {
					out.Result.Sandboxes = append(out.Result.Sandboxes, view)
				} else {
					out.Result.Sandbox = &view
				}
				out.ChargeTenantID = view.TenantID
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, ErrNotFound) {
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
