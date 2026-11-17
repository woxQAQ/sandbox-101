package gc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/gc/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

func (service *Collector) Collect(ctx context.Context, principal authorization.Principal, key string, options types.GCOptions) (types.GCReport, error) {
	var empty types.GCReport
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	if service.store.Reviewed {
		return types.GCReport{
			DryRun:  options.DryRun,
			Blocked: true,
			Deferred: []types.GCItem{
				{
					Kind:   "recovery",
					Reason: "reviewed rollback has no complete ownership history",
				},
			},
		}, nil
	}
	if options.Batch == 0 {
		options.Batch = 16
	}
	if options.Budget == 0 {
		options.Budget = 2 * time.Second
	}
	if options.Batch < 1 || options.Batch > 1024 || options.Budget <= 0 {
		return empty, fmt.Errorf("%w: invalid GC batch or budget", errs.ErrInvalid)
	}
	requestGuard := service.guards.NodeRequest(service.nodeID, key)
	requestGuard.Lock()
	defer requestGuard.Unlock()
	request := operation.Encode(struct {
		Action  string
		Options types.GCOptions
	}{"gc", options})
	hash := operation.Digest(request)
	if !options.DryRun {
		if key == "" || len(key) > 256 {
			return empty, fmt.Errorf("%w: GC mutation requires key", errs.ErrInvalid)
		}
		if cached, found, err := operation.New(service.store.Database).Cached(ctx, operation.Scope{
			Kind: "node",
			ID:   service.nodeID,
		}, key, hash); found || err != nil {
			if err != nil {
				return empty, err
			}
			if !operation.Terminal(cached.Status) {
				service.dispatch(cached.ID)
			}
			return service.wait(ctx, principal, cached.ID)
		}
	}
	budgetCtx, cancel := context.WithTimeout(ctx, options.Budget)
	defer cancel()
	if !service.guards.GarbageCollection().TryLock() {
		return types.GCReport{
			DryRun: options.DryRun,
			Deferred: []types.GCItem{
				{
					Kind:   "busy",
					Reason: "another collector owns inventory",
				},
			},
		}, nil
	}
	defer service.guards.GarbageCollection().Unlock()
	if !service.guards.Artifacts().TryLock() {
		return types.GCReport{
			DryRun: options.DryRun,
			Deferred: []types.GCItem{
				{
					Kind:   "busy",
					Reason: "artifact publication active",
				},
			},
		}, nil
	}
	report, err := service.inventory(budgetCtx)
	service.guards.Artifacts().Unlock()
	report.DryRun = options.DryRun
	if err != nil {
		return report, err
	}
	raw, deferred, err := service.rawCandidates(budgetCtx)
	if err != nil {
		return report, err
	}
	report.Candidates = append(report.Candidates, raw...)
	report.Deferred = append(report.Deferred, deferred...)
	if options.DryRun || report.Blocked {
		return report, nil
	}
	if len(report.Candidates) > options.Batch {
		for _, item := range report.Candidates[options.Batch:] {
			item.Reason = "batch limit"
			report.Deferred = append(report.Deferred, item)
		}
		report.Candidates = report.Candidates[:options.Batch]
	}
	if err = budgetCtx.Err(); err != nil {
		return report, err
	}
	deadline, _ := budgetCtx.Deadline()
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
	}
	report.OperationID = accepted.ID
	input := input{
		StoreID:         service.nodeID,
		RequestTenantID: principal.TenantID,
		Options:         options,
		Deadline:        deadline,
		Report:          report,
	}
	payload := operation.Encode(input)
	service.admission.Lock()
	if err = service.gate.Check(); err == nil {
		var tx *sql.Tx
		tx, err = service.store.Database.BeginTx(ctx, nil)
		if err == nil {
			operations := operation.New(tx)
			err = operations.Insert(ctx, operation.Admission{
				ID:        accepted.ID,
				ScopeKind: "node",
				ScopeID:   service.nodeID,
				Key:       key,
				Hash:      hash,
				Request:   request,
				Status:    "pending",
				Body:      operation.Encode(accepted.Result),
			})
			if err == nil {
				err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
					OperationID:         accepted.ID,
					StepKey:             "gc",
					Action:              "gc",
					InputHash:           operation.Digest(payload),
					Input:               payload,
					CandidateResourceID: input.StoreID,
					ExpectedInstance:    "",
					Status:              "pending",
					Body: operation.Encode(progress{
						Report: report,
					}),
				}))
			}
			if err == nil {
				err = service.fault("gc_admission_before_commit")
			}
			if err == nil {
				err = tx.Commit()
			} else {
				err = errors.Join(err, tx.Rollback())
			}
		}
	}
	service.admission.Unlock()
	if err != nil {
		return report, err
	}
	if err = service.store.Checkpoint(); err != nil {
		return report, err
	}
	service.dispatch(accepted.ID)
	return service.wait(ctx, principal, accepted.ID)
}

func (service *Collector) wait(ctx context.Context, principal authorization.Principal, id string) (types.GCReport, error) {
	_, err := service.operations.WaitManagement(ctx, principal, id)
	step, lookup := Load(context.Background(), service.store.Database, id)
	if lookup != nil {
		return types.GCReport{}, errors.Join(err, lookup)
	}
	return step.State.Report, err
}
