package gc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply"
	"github.com/woxQAQ/haven/types"
)

func (c *Collector) admitChild(ctx context.Context, parent Step, item types.GCItem) (operation.State, string, error) {
	key := parent.OperationID + ":" + item.Kind + ":" + item.ID
	switch item.Kind {
	case "image_work", "image_cache":
		result, err := c.images.CollectCandidate(ctx, authorization.Principal{
			Root:     true,
			TenantID: parent.Input.RequestTenantID,
		}, key, item)
		return result, "", err
	case "supply":
		source, err := supply.LoadSupply(ctx, c.store.Database, item.ID)
		if err != nil {
			return operation.State{}, "", err
		}
		if source.Hash != item.Proof {
			return operation.State{}, "", fmt.Errorf("%w: GC frozen supply input changed", errs.ErrManualRecovery)
		}
		result, err := c.supply.AdmitSupplyCleanup(ctx, authorization.Principal{
			Root:     true,
			TenantID: parent.Input.RequestTenantID,
		}, key, supply.SupplyCleanupRequest{
			OperationID: item.ID,
		})
		return result, "", err
	case "snapshot":
		source, err := c.proofs.SnapshotPublication(ctx, c.store.Database, item.TenantID, item.ID)
		if err != nil {
			return operation.State{}, "", err
		}
		if operation.Digest(operation.Encode(source.Snapshot)) != item.Proof {
			return operation.State{}, "", fmt.Errorf("%w: GC frozen snapshot input changed", errs.ErrManualRecovery)
		}
		eligible, _, err := c.snapshotCandidate(ctx, source.Snapshot)
		if err != nil {
			return operation.State{}, "", err
		}
		if !eligible {
			return operation.State{}, "", fmt.Errorf("%w: snapshot gained an active consumer", errs.ErrConflict)
		}
		result, err := c.snapshots.AdmitSnapshotDelete(ctx, machine.SnapshotDeleteRequest{
			TenantID:   item.TenantID,
			Key:        key,
			SnapshotID: item.ID,
		})
		return result, item.TenantID, err
	case "raw_capture":
		capture, err := machine.LoadCapture(ctx, c.store.Database, item.ID)
		if err != nil {
			return operation.State{}, "", err
		}
		if capture.Hash != item.Proof {
			return operation.State{}, "", fmt.Errorf("%w: GC frozen raw capture changed", errs.ErrManualRecovery)
		}
		result, err := c.raw.Admit(ctx, machine.CaptureCleanupRequest{
			TenantID:           item.TenantID,
			Key:                key,
			CaptureOperationID: item.ID,
			StopOperationID:    item.OperationID,
		}, nil)
		return result, item.TenantID, err
	default:
		return operation.State{}, "", fmt.Errorf("%w: unsupported GC candidate", errs.ErrInvalid)
	}
}

func (c *Collector) run(step Step) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	ctx, cancel := context.WithDeadline(c.ctx, step.Input.Deadline)
	defer cancel()
	previous := step.State
	if !c.imageArtifactsMu.TryLock() {
		step.State.Report.Deferred = append(step.State.Report.Deferred, types.GCItem{
			Kind:   "busy",
			Reason: "artifact publication active",
		})
		step.State.Next = len(step.Input.Report.Candidates)
	} else {
		c.imageArtifactsMu.Unlock()
		for step.State.Next < len(step.Input.Report.Candidates) {
			item := step.Input.Report.Candidates[step.State.Next]
			if ctx.Err() != nil {
				for _, remaining := range step.Input.Report.Candidates[step.State.Next:] {
					remaining.Reason = "pass context budget elapsed"
					step.State.Report.Deferred = append(step.State.Report.Deferred, remaining)
				}
				step.State.Next = len(step.Input.Report.Candidates)
				break
			}
			child, scope, err := c.admitChild(ctx, step, item)
			if child.ID != "" {
				item.OperationID = child.ID
				if err == nil {
					if scope == "" {
						_, err = c.operationMonitor.WaitManagement(ctx, authorization.Principal{
							Root: true,
						}, child.ID)
					} else {
						_, err = c.operationMonitor.Wait(ctx, authorization.Principal{
							TenantID: scope,
						}, child.ID)
					}
				}
			}
			if err == nil && child.ID != "" {
				step.State.Report.Collected = append(step.State.Report.Collected, item)
			} else {
				item.Reason = "candidate retained; child admission or completion unconfirmed"
				step.State.Report.Deferred = append(step.State.Report.Deferred, item)
			}
			step.State.Next++
			if err = c.save(step, previous, false); err != nil {
				return err
			}
			previous = step.State
			step.Status = "running"
		}
	}
	step.State.Report.FinishedAt = time.Now().UTC()
	return c.save(step, previous, true)
}

func (c *Collector) save(step Step, previous progress, complete bool) error {
	c.storeWriteMu.Lock()
	defer c.storeWriteMu.Unlock()
	tx, err := c.store.Database.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operations := operation.New(tx)
	queries := dbgen.New(tx)
	status, phase, finished := "running", "collecting", ""
	if complete {
		status, phase, finished = "succeeded", "complete", step.State.Report.FinishedAt.Format(time.RFC3339Nano)
	}
	if err = store.RequireOne(queries.SetGCStatus(c.ctx, dbgen.SetGCStatusParams{
		OperationID:    step.OperationID,
		InputHash:      step.Hash,
		PreviousStatus: step.Status,
		NextStatus:     status,
		NextPhase: sql.NullString{
			String: phase,
			Valid:  true,
		},
		CompletedAt: finished,
	})); err != nil {
		return err
	}
	if err = store.RequireOne(queries.AdvanceGCProgress(c.ctx, dbgen.AdvanceGCProgressParams{
		OperationID:       step.OperationID,
		PreviousCandidate: int64(previous.Next),
		NextCandidate:     int64(step.State.Next),
	})); err != nil {
		return err
	}
	for _, group := range []struct {
		disposition string
		items       []types.GCItem
		start       int
	}{
		{"collected", step.State.Report.Collected, len(previous.Report.Collected)},
		{"deferred", step.State.Report.Deferred, len(previous.Report.Deferred)},
	} {
		for i := group.start; i < len(group.items); i++ {
			item := group.items[i]
			if err = store.RequireOne(queries.InsertGCOutcome(c.ctx, dbgen.InsertGCOutcomeParams{
				OperationID:      step.OperationID,
				Disposition:      group.disposition,
				Ordinal:          int64(i),
				Kind:             item.Kind,
				ResourceID:       item.ID,
				TenantID:         item.TenantID,
				CacheBuilder:     item.CacheBuilder,
				CacheDiffID:      item.CacheDiffID,
				Proof:            item.Proof,
				ChildOperationID: item.OperationID,
				Reason:           item.Reason,
			})); err != nil {
				return err
			}
		}
	}
	if complete {
		if err = operations.MarkSucceeded(c.ctx, step.OperationID); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return c.store.Checkpoint()
}
