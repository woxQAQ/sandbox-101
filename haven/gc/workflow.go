package gc

import (
	"context"
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

func (c *Collector) admitChild(ctx context.Context, parent Step, item types.GCItem) (operation.Record, string, error) {
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
			return operation.Record{}, "", err
		}
		if source.Hash != item.Proof {
			return operation.Record{}, "", fmt.Errorf("%w: GC frozen supply input changed", errs.ErrManualRecovery)
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
			return operation.Record{}, "", err
		}
		if operation.Digest(operation.Encode(source.Snapshot)) != item.Proof {
			return operation.Record{}, "", fmt.Errorf("%w: GC frozen snapshot input changed", errs.ErrManualRecovery)
		}
		eligible, _, err := c.snapshotCandidate(ctx, source.Snapshot)
		if err != nil {
			return operation.Record{}, "", err
		}
		if !eligible {
			return operation.Record{}, "", fmt.Errorf("%w: snapshot gained an active consumer", errs.ErrConflict)
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
			return operation.Record{}, "", err
		}
		if capture.Hash != item.Proof {
			return operation.Record{}, "", fmt.Errorf("%w: GC frozen raw capture changed", errs.ErrManualRecovery)
		}
		result, err := c.raw.Admit(ctx, machine.CaptureCleanupRequest{
			TenantID:           item.TenantID,
			Key:                key,
			CaptureOperationID: item.ID,
			StopOperationID:    item.OperationID,
		}, nil)
		return result, item.TenantID, err
	default:
		return operation.Record{}, "", fmt.Errorf("%w: unsupported GC candidate", errs.ErrInvalid)
	}
}

func (c *Collector) run(step Step) error {
	if operation.Terminal(step.Status) {
		return nil
	}
	ctx, cancel := context.WithDeadline(c.ctx, step.Input.Deadline)
	defer cancel()
	if !c.guards.Artifacts().TryLock() {
		step.State.Report.Deferred = append(step.State.Report.Deferred, types.GCItem{
			Kind:   "busy",
			Reason: "artifact publication active",
		})
		step.State.Next = len(step.Input.Report.Candidates)
	} else {
		c.guards.Artifacts().Unlock()
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
						_, err = c.operations.WaitManagement(ctx, authorization.Principal{
							Root: true,
						}, child.ID)
					} else {
						_, err = c.operations.Wait(ctx, authorization.Principal{
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
			if err = c.save(step, false); err != nil {
				return err
			}
		}
	}
	step.State.Report.FinishedAt = time.Now().UTC()
	return c.save(step, true)
}

func (c *Collector) save(step Step, complete bool) error {
	c.admission.Lock()
	defer c.admission.Unlock()
	tx, err := c.store.Database.BeginTx(c.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operations := operation.New(tx)
	if complete {
		err = store.RequireOne(dbgen.New(tx).CompleteLiveStep(c.ctx, dbgen.CompleteLiveStepParams{
			OperationID: step.OperationID,
			StepKey:     "gc",
			InputHash:   step.Hash,
			Body:        operation.Encode(step.State),
		}))
	} else {
		err = store.RequireOne(dbgen.New(tx).RecordLiveStepProgress(c.ctx, dbgen.RecordLiveStepProgressParams{
			OperationID: step.OperationID,
			StepKey:     "gc",
			InputHash:   step.Hash,
			Body:        operation.Encode(step.State),
		}))
	}
	if err != nil {
		return err
	}
	if complete {
		if err = operations.Complete(c.ctx, step.OperationID); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return c.store.Checkpoint()
}
