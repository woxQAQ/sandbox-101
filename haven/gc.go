package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type (
	gcInput struct {
		StoreID         string    `json:"store_id"`
		RequestTenantID string    `json:"request_tenant_id"`
		Options         GCOptions `json:"options"`
		Deadline        time.Time `json:"deadline"`
		Report          GCReport  `json:"report"`
	}
	gcState struct {
		Next   int      `json:"next"`
		Report GCReport `json:"report"`
	}
	gcStep struct {
		OperationID, Hash, Status string
		Input                     gcInput
		State                     gcState
	}
)

func loadGC(ctx context.Context, q queryer, id string) (gcStep, error) {
	step := gcStep{
		OperationID: id,
	}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='gc'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
		return step, fmt.Errorf("GC input checksum changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "gc" || candidate != step.Input.StoreID || !loom.ValidResourceID(candidate, "store-") || expected != "" || step.State.Next < 0 || step.State.Next > len(step.Input.Report.Candidates) {
		return step, fmt.Errorf("GC durable progress identity changed")
	}
	return step, nil
}

func (service *Service) CollectGarbage(ctx context.Context, principal Principal, key string, options GCOptions) (GCReport, error) {
	var empty GCReport
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: not found", ErrNotFound)
	}
	if service.store.reviewed {
		return GCReport{
			DryRun:  options.DryRun,
			Blocked: true,
			Deferred: []GCItem{
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
		return empty, fmt.Errorf("%w: invalid GC batch or budget", ErrInvalid)
	}
	requestGuard := service.vmLock("node-request:" + string(encode([]string{
		service.store.identity.Identity,
		key,
	})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	request := encode(struct {
		Action  string
		Options GCOptions
	}{"gc", options})
	hash := digest(request)
	if !options.DryRun {
		if key == "" || len(key) > 256 {
			return empty, fmt.Errorf("%w: GC mutation requires key", ErrInvalid)
		}
		if cached, found, err := cachedScopedOperation(ctx, service.store.db, "node", service.store.identity.Identity, key, hash); found || err != nil {
			if err != nil {
				return empty, err
			}
			if !terminal(cached.Status) {
				service.dispatch(cached.ID)
			}
			return service.waitGC(ctx, principal, cached.ID)
		}
	}
	budgetCtx, cancel := context.WithTimeout(ctx, options.Budget)
	defer cancel()
	if !service.gcMu.TryLock() {
		return GCReport{
			DryRun: options.DryRun,
			Deferred: []GCItem{
				{
					Kind:   "busy",
					Reason: "another collector owns inventory",
				},
			},
		}, nil
	}
	defer service.gcMu.Unlock()
	if !service.artifacts.TryLock() {
		return GCReport{
			DryRun: options.DryRun,
			Deferred: []GCItem{
				{
					Kind:   "busy",
					Reason: "artifact publication active",
				},
			},
		}, nil
	}
	report, err := service.gcInventory(budgetCtx)
	service.artifacts.Unlock()
	report.DryRun = options.DryRun
	if err != nil {
		return report, err
	}
	raw, deferred, err := service.rawGCCandidates(budgetCtx)
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
	operation := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
	}
	report.OperationID = operation.ID
	input := gcInput{
		StoreID:         service.store.identity.Identity,
		RequestTenantID: principal.TenantID,
		Options:         options,
		Deadline:        deadline,
		Report:          report,
	}
	payload := encode(input)
	service.admission.Lock()
	if err = service.checkAdmissionOpen(); err == nil {
		var tx *sql.Tx
		tx, err = service.store.db.BeginTx(ctx, nil)
		if err == nil {
			for _, statement := range []struct {
				q    string
				args []any
			}{
				{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
					operation.ID,
					"node",
					service.store.identity.Identity,
					key,
					hash,
					request,
					"pending",
					encode(operation.Result),
				}},
				{"INSERT INTO operation_steps VALUES(?,'gc','gc',?,?,?,?,'pending',?)", []any{
					operation.ID,
					digest(payload),
					payload,
					input.StoreID,
					"",
					encode(gcState{
						Report: report,
					}),
				}},
			} {
				if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
					break
				}
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
	if err = service.store.checkpoint(); err != nil {
		return report, err
	}
	service.dispatch(operation.ID)
	return service.waitGC(ctx, principal, operation.ID)
}

func (service *Service) waitGC(ctx context.Context, principal Principal, id string) (GCReport, error) {
	_, err := service.WaitManagement(ctx, principal, id)
	step, lookup := loadGC(context.Background(), service.store.db, id)
	if lookup != nil {
		return GCReport{}, errors.Join(err, lookup)
	}
	return step.State.Report, err
}

func (service *Service) gcChild(ctx context.Context, parent gcStep, item GCItem) (Operation, string, error) {
	key := parent.OperationID + ":" + item.Kind + ":" + item.ID
	switch item.Kind {
	case "image_work", "image_cache":
		request := ImageCleanupRequest{
			ImportOperationID: item.ID,
		}
		if item.Kind == "image_cache" {
			record, err := loadImageCache(ctx, service.store.db, item.CacheBuilder, item.CacheDiffID)
			if err != nil {
				return Operation{}, "", err
			}
			if digest(encode(record)) != item.Proof {
				return Operation{}, "", fmt.Errorf("%w: GC frozen cache ownership changed", ErrManualRecovery)
			}
			request = ImageCleanupRequest{
				CacheBuilder: item.CacheBuilder,
				CacheDiffID:  item.CacheDiffID,
			}
		} else {
			source, err := loadImageImport(ctx, service.store.db, item.ID)
			if err != nil {
				return Operation{}, "", err
			}
			if source.Hash != item.Proof {
				return Operation{}, "", fmt.Errorf("%w: GC frozen import ownership changed", ErrManualRecovery)
			}
		}
		result, err := service.AdmitImageCleanup(ctx, Principal{
			Root:     true,
			TenantID: parent.Input.RequestTenantID,
		}, key, request)
		return result, "", err
	case "supply":
		source, err := loadSupply(ctx, service.store.db, item.ID)
		if err != nil {
			return Operation{}, "", err
		}
		if source.Hash != item.Proof {
			return Operation{}, "", fmt.Errorf("%w: GC frozen supply input changed", ErrManualRecovery)
		}
		result, err := service.AdmitSupplyCleanup(ctx, Principal{
			Root:     true,
			TenantID: parent.Input.RequestTenantID,
		}, key, SupplyCleanupRequest{
			OperationID: item.ID,
		})
		return result, "", err
	case "snapshot":
		source, err := service.snapshotPublication(ctx, service.store.db, item.TenantID, item.ID)
		if err != nil {
			return Operation{}, "", err
		}
		if digest(encode(source.Snapshot)) != item.Proof {
			return Operation{}, "", fmt.Errorf("%w: GC frozen snapshot input changed", ErrManualRecovery)
		}
		eligible, _, err := service.snapshotGCCandidate(ctx, source.Snapshot)
		if err != nil {
			return Operation{}, "", err
		}
		if !eligible {
			return Operation{}, "", fmt.Errorf("%w: snapshot gained an active consumer", ErrConflict)
		}
		result, err := service.AdmitSnapshotDelete(ctx, SnapshotDeleteRequest{
			TenantID:   item.TenantID,
			Key:        key,
			SnapshotID: item.ID,
		})
		return result, item.TenantID, err
	case "raw_capture":
		capture, err := loadCapture(ctx, service.store.db, item.ID)
		if err != nil {
			return Operation{}, "", err
		}
		if capture.Hash != item.Proof {
			return Operation{}, "", fmt.Errorf("%w: GC frozen raw capture changed", ErrManualRecovery)
		}
		result, err := service.AdmitCaptureCleanup(ctx, CaptureCleanupRequest{
			TenantID:           item.TenantID,
			Key:                key,
			CaptureOperationID: item.ID,
			StopOperationID:    item.OperationID,
		})
		return result, item.TenantID, err
	default:
		return Operation{}, "", fmt.Errorf("%w: unsupported GC candidate", ErrInvalid)
	}
}

func (service *Service) runGC(step gcStep) error {
	if terminal(step.Status) {
		return nil
	}
	ctx, cancel := context.WithDeadline(service.ctx, step.Input.Deadline)
	defer cancel()
	if !service.artifacts.TryLock() {
		step.State.Report.Deferred = append(step.State.Report.Deferred, GCItem{
			Kind:   "busy",
			Reason: "artifact publication active",
		})
		step.State.Next = len(step.Input.Report.Candidates)
	} else {
		service.artifacts.Unlock()
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
			child, scope, err := service.gcChild(ctx, step, item)
			if child.ID != "" {
				item.OperationID = child.ID
				if err == nil {
					if scope == "" {
						_, err = service.WaitManagement(ctx, Principal{
							Root: true,
						}, child.ID)
					} else {
						_, err = service.Wait(ctx, Principal{
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
			if err = service.saveGC(step, false); err != nil {
				return err
			}
		}
	}
	step.State.Report.FinishedAt = time.Now().UTC()
	return service.saveGC(step, true)
}

func (service *Service) saveGC(step gcStep, complete bool) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	status := "running"
	if complete {
		status = "succeeded"
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status=?,body=? WHERE operation_id=? AND step_key='gc' AND input_hash=? AND status IN ('pending','running')", status, encode(step.State), step.OperationID, step.Hash)); err != nil {
		return err
	}
	if complete {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", step.OperationID)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
