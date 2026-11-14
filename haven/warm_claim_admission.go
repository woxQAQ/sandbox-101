package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

func (service *Service) validateWarmCatalog(ctx context.Context, tx *sql.Tx, template templateRecord) error {
	rows, err := tx.QueryContext(ctx, "SELECT vm_id FROM warm WHERE template_id=? ORDER BY vm_id", template.ID)
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
	if err = errors.Join(err, rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, vm := range ids {
		var poolID string
		if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM pool_slots WHERE vm_id=?", vm).Scan(&poolID); err != nil {
			return fmt.Errorf("%w: ready warm pool provenance missing", ErrManualRecovery)
		}
		pool, _, err := service.verifyReadyWarm(ctx, tx, poolID)
		if err != nil {
			return err
		}
		if pool.Template.ID != template.ID || pool.Template.TenantID != template.TenantID {
			return fmt.Errorf("%w: ready warm permanent owner changed", ErrManualRecovery)
		}
	}
	return nil
}

func (service *Service) admitWarmClaim(ctx context.Context, principal Principal, key string, admission claimAdmission, template templateRecord, restore restoreSource) (Operation, bool, error) {
	var empty Operation
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return empty, false, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	current, err := loadOwnedTemplate(ctx, tx, template.TenantID, template.ID)
	if err != nil {
		return empty, false, err
	}
	if !reflect.DeepEqual(current, template) {
		return empty, false, fmt.Errorf("%w: warm claim template version changed", ErrConflict)
	}
	if err = service.validateWarmCatalog(ctx, tx, template); err != nil {
		return empty, false, err
	}
	var poolID string
	err = tx.QueryRowContext(ctx, "SELECT p.operation_id FROM warm w JOIN pool_slots p ON p.vm_id=w.vm_id WHERE w.template_id=? AND w.snapshot_id=? AND p.template_version=? AND p.status='ready' ORDER BY w.created_at,w.vm_id LIMIT 1", template.ID, template.SnapshotID, template.Version).Scan(&poolID)
	if errors.Is(err, sql.ErrNoRows) {
		return empty, false, nil
	}
	if err != nil {
		return empty, false, err
	}
	pool, source, err := service.verifyReadyWarm(ctx, tx, poolID)
	if err != nil {
		return empty, false, err
	}
	currentRestore, err := service.loadRestoreSource(ctx, tx, template.TenantID, template.SnapshotID)
	if err != nil {
		return empty, false, err
	}
	if !reflect.DeepEqual(currentRestore, restore) {
		return empty, false, fmt.Errorf("%w: warm fallback source publication changed", ErrConflict)
	}
	var body []byte
	var tenant tenantRecord
	if err = tx.QueryRowContext(ctx, "SELECT body FROM tenants WHERE id=?", template.TenantID).Scan(&body); err != nil {
		return empty, false, err
	}
	if err = json.Unmarshal(body, &tenant); err != nil {
		return empty, false, err
	}
	var slots, storage int64
	if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(sandbox_slots),0),COALESCE(SUM(storage_mib),0) FROM quota_resources WHERE tenant_id=?", template.TenantID).Scan(&slots, &storage); err != nil {
		return empty, false, err
	}
	if slots >= tenant.MaxSandboxes || storage > tenant.MaxStorageMiB {
		return empty, false, fmt.Errorf("%w: tenant claim quota exhausted", ErrExhausted)
	}
	sandbox, err := loadSandbox(ctx, tx, pool.SandboxID, template.TenantID)
	if err != nil {
		return empty, false, err
	}
	oldSandbox := sandbox
	if sandbox.State != "provisioning" || sandbox.SourceOperation != source.OperationID || sandbox.Instance != pool.Instance || sandbox.SlotAssignment != nil {
		return empty, false, fmt.Errorf("%w: warm allocation current sandbox identity changed", ErrManualRecovery)
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			SandboxID:     pool.SandboxID,
			VMID:          pool.VMID,
			SnapshotID:    template.SnapshotID,
			ResourceReady: true,
		},
	}
	settings := admission.Settings
	settings.Generation = operation.ID
	input := warmClaimInput{
		RequestTenantID: principal.TenantID,
		Command:         admission.Command,
		PoolID:          poolID,
		Pool:            pool,
		Source:          source,
		Settings:        settings,
		Restore:         restore,
	}
	payload := encode(input)
	hash := digest(payload)
	sandbox.OperationID = operation.ID
	sandbox.SlotAssignment = &generationRef{
		OperationID: operation.ID,
		InputHash:   hash,
	}
	sandbox.Identity = newSandboxIdentity(template, "warm")
	activation := activationInput{
		Attempt:           "warm",
		AllowFallback:     true,
		RequestTenantID:   principal.TenantID,
		TenantID:          template.TenantID,
		SandboxID:         pool.SandboxID,
		ResourceOperation: source.OperationID,
		ResourceHash:      source.Hash,
		ExpectedInstance:  pool.Instance,
		Settings:          settings,
		ResetIdentity:     true,
	}
	activationPayload := encode(activation)
	frozen := encode(struct {
		Action  string
		Request Command
	}{"claim", admission.Command})
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			operation.ID,
			"tenant",
			principal.TenantID,
			key,
			digest(frozen),
			frozen,
			"pending",
			encode(operation.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			operation.ID,
			admission.MAC,
		}},
		{"INSERT INTO operation_steps VALUES(?,'warm_claim','claim',?,?,?,?,'pending',?)", []any{
			operation.ID,
			hash,
			payload,
			pool.SandboxID,
			pool.Instance,
			encode(warmClaimState{
				Phase: "selected",
			}),
		}},
		{"INSERT INTO sandbox_settings VALUES(?,?)", []any{
			pool.SandboxID,
			encode(settings),
		}},
		{"INSERT INTO configuration_fences VALUES(?,?)", []any{
			pool.SandboxID,
			operation.ID,
		}},
		{"INSERT INTO operation_steps VALUES(?,'activation:warm','activate',?,?,?,?,'pending',?)", []any{
			operation.ID,
			digest(activationPayload),
			activationPayload,
			pool.VMID,
			pool.Instance,
			encode("admitted"),
		}},
		{"UPDATE sandboxes SET body=? WHERE id=? AND tenant=? AND state='provisioning' AND body=?", []any{
			encode(sandbox),
			sandbox.ID,
			template.TenantID,
			encode(oldSandbox),
		}},
		{"UPDATE quota_resources SET sandbox_slots=1 WHERE tenant_id=? AND kind='sandbox_slot' AND resource_key=? AND sandbox_slots=0 AND storage_mib=0 AND source_operation_id=?", []any{
			template.TenantID,
			pool.SandboxID,
			source.Input.SlotOperation,
		}},
		{"DELETE FROM warm WHERE vm_id=? AND template_id=? AND snapshot_id=? AND created_at=?", []any{
			pool.VMID,
			template.ID,
			template.SnapshotID,
			pool.CreatedAt.UnixNano(),
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='warm' AND holder_id=?", []any{
			pool.VMID,
			poolID,
		}},
		{"UPDATE pool_slots SET status='selected' WHERE operation_id=? AND status='ready' AND body=?", []any{
			poolID,
			encode(pool),
		}},
		{"INSERT INTO source_leases VALUES(?,?,'warm_claim')", []any{
			template.SnapshotID,
			operation.ID,
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'step',?)", []any{
			template.SnapshotID,
			operation.ID + ":warm_claim",
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'engine_dependency',?)", []any{
			template.TenantID,
			template.SnapshotID,
			operation.ID + ":warm_claim",
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, false, err
		}
	}
	if err = service.fault("warm_claim_admission_before_commit"); err != nil {
		return empty, false, err
	}
	if err = tx.Commit(); err != nil {
		return empty, false, err
	}
	service.mu.Lock()
	service.opSecrets[operation.ID] = admission.Secrets
	service.mu.Unlock()
	if err = service.store.checkpoint(); err != nil {
		return operation, true, err
	}
	if err = service.fault("warm_claim_admitted"); err != nil {
		return operation, true, err
	}
	service.dispatch(operation.ID)
	return operation, true, nil
}
