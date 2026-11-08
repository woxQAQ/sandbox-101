package engine

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (e *Engine) finish(o *opRecord, cause error) error {
	if e.opts.Fault != nil {
		if err := e.opts.Fault("finish_candidate_ready"); err != nil {
			return err
		}
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	// Reload first: a committed terminal response is immutable, including when
	// checkpointing its commit failed. JSON cloning isolates every mutable member.
	durable, err := e.loadOp(e.ctx, o.Operation.ID)
	if err != nil {
		return err
	}
	if terminal(durable.Operation.Status) {
		*o = durable
		return nil
	}
	var next opRecord
	if err = json.Unmarshal(encode(o), &next); err != nil {
		return err
	}
	if !reflect.DeepEqual(durable.Operation.Request, next.Operation.Request) ||
		durable.Operation.ID != next.Operation.ID || durable.Operation.VMID != next.Operation.VMID ||
		!reflect.DeepEqual(durable.Journal, next.Journal) {
		return api.Fail(api.Conflict, "operation completion intent changed")
	}
	if cause != nil &&
		(next.Operation.Request.Action == "restore" || next.Operation.Request.Action == "clone") &&
		(next.Journal.Resource != nil || !next.Journal.Lease) {
		// 010's failed-candidate fence precedes the public terminal receipt. Failure
		// to persist it leaves both the caller and durable operation nonterminal.
		next.Journal.RestoreBlocked = true
		if err = requireCompletionWrite(e.db.Exec(
			"UPDATE operations SET body=? WHERE id=?",
			encode(next),
			next.Operation.ID,
		)); err != nil {
			return err
		}
	}
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var raw []byte
	if err = tx.QueryRow("SELECT body FROM operations WHERE id=?", next.Operation.ID).
		Scan(&raw); err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &durable); err != nil {
		return err
	}
	if terminal(durable.Operation.Status) {
		*o = durable
		return nil
	}
	if !reflect.DeepEqual(durable.Operation.Request, next.Operation.Request) ||
		!reflect.DeepEqual(durable.Journal, next.Journal) ||
		durable.Operation.VMID != next.Operation.VMID {
		return api.Fail(api.Conflict, "durable completion intent changed")
	}
	generation := cause == nil &&
		(next.Operation.Request.Action == "start" || next.Operation.Request.Action == "restore" || next.Operation.Request.Action == "clone")
	next.Operation.ResultInstance = ""
	if next.Operation.VMID != "" {
		queryErr := tx.QueryRow("SELECT body FROM vms WHERE id=?", next.Operation.VMID).
			Scan(&raw)
		if generation && queryErr != nil {
			return queryErr
		}
		if queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
			return queryErr
		}
		if queryErr == nil {
			var v vmRecord
			if err = json.Unmarshal(raw, &v); err != nil {
				return err
			}
			if generation {
				r := next.Journal.Resource
				req := next.Operation.Request
				if r == nil || r.Instance == "" ||
					r.VMID != v.VM.ID ||
					next.Operation.VMID != v.VM.ID ||
					v.VM.OperationID != next.Operation.ID ||
					v.VM.Instance != r.Instance ||
					!reflect.DeepEqual(v.Resource, *r) ||
					v.VM.Phase != "running" ||
					!v.VM.Verified ||
					!v.VM.AgentReady ||
					(req.Action == "clone" && req.VMID != "") ||
					(req.Action != "clone" && req.VMID != v.VM.ID) {
					return api.Fail(
						api.Conflict,
						"generation completion lacks full owned candidate proof",
					)
				}
				next.Operation.ResultInstance = r.Instance
			}
			if v.VM.OperationID == next.Operation.ID {
				if !next.Journal.ResumeRequired &&
					!next.Journal.RestoreBlocked &&
					next.Journal.BlockedRestoreID == "" {
					v.VM.OperationID = ""
				}
				if cause != nil {
					v.VM.BlockedReason = cause.Error()
				}
				if err = requireCompletionWrite(tx.Exec(
					"UPDATE vms SET body=? WHERE id=?",
					encode(v),
					v.VM.ID,
				)); err != nil {
					return err
				}
			}
		}
	}
	if cause != nil {
		next.Operation.Status = "failed"
		next.Operation.Error = asAPI(cause)
	} else {
		next.Operation.Status = "succeeded"
		next.Operation.Step = "complete"
	}
	next.Operation.UpdatedAt = time.Now().UTC()
	if next.Journal.Lease && !next.Journal.RestoreBlocked {
		// Admission creates one lease per operation. A successful restore has
		// already transferred it to backing and cleared Journal.Lease.
		if err = requireCompletionWrite(tx.Exec(
			"DELETE FROM leases WHERE operation=? AND snapshot=?",
			next.Operation.ID,
			next.Operation.Request.SnapshotID,
		)); err != nil {
			return err
		}
		next.Journal.Lease = false
	}
	if err = requireCompletionWrite(tx.Exec(
		"UPDATE operations SET status=?,body=? WHERE id=?",
		next.Operation.Status,
		encode(next),
		next.Operation.ID,
	)); err != nil {
		return err
	}
	if e.opts.Fault != nil {
		if err = e.opts.Fault("finish_before_commit"); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	*o = next
	return e.checkpoint()
}

// Every required completion write targets one existing primary-key row. A nil
// SQL error alone does not prove that the statement applied its intended effect.
func requireCompletionWrite(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	if result == nil {
		return errors.New("completion write returned no result")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("completion write effect unavailable: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("completion write affected %d rows, expected 1", rows)
	}
	return nil
}
