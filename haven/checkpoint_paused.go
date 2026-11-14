package haven

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// A paused checkpoint adds a binding to the proven sleep publication. It does
// not launch, capture or renew the source and cannot infer a missing pause receipt.
func (service *Service) admitPausedCheckpoint(ctx context.Context, parent Operation, input lifecycleInput, mac string) (Operation, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return Operation{}, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	sandbox, err := loadSandbox(ctx, tx, input.Sandbox.ID, input.OwnerTenantID)
	if err != nil {
		return Operation{}, err
	}
	settings, err := loadSettings(ctx, tx, sandbox.ID)
	if err != nil {
		return Operation{}, err
	}
	if !reflect.DeepEqual(sandbox, input.Sandbox) || !reflect.DeepEqual(settings, input.Settings) || sandbox.State != "paused" || sandbox.VMID != "" || sandbox.Instance != "" {
		return Operation{}, fmt.Errorf("%w: paused checkpoint source changed", ErrConflict)
	}
	var fence string
	if err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", sandbox.ID).Scan(&fence); err == nil {
		return Operation{}, fmt.Errorf("%w: paused checkpoint source is fenced", ErrConflict)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	source, err := loadStep(ctx, tx, input.Source.OperationID)
	if err != nil {
		return Operation{}, err
	}
	if source.Hash != input.Source.InputHash || source.State.Witness != input.SourceInstance {
		return Operation{}, fmt.Errorf("%w: paused checkpoint original generation changed", ErrManualRecovery)
	}
	proof, err := pausedReceipt(ctx, tx, sandbox, source)
	if err != nil {
		return Operation{}, err
	}
	input.PauseOperation = proof
	snapshot, err := service.loadRestoreSource(ctx, tx, input.OwnerTenantID, sandbox.SnapshotID)
	if err != nil {
		return Operation{}, err
	}
	cp := Checkpoint{
		ID:         input.CheckpointID,
		TenantID:   input.OwnerTenantID,
		SandboxID:  sandbox.ID,
		TemplateID: sandbox.Identity.TemplateID,
		Shape:      snapshot.Snapshot.Manifest.Config.Shape,
		SnapshotID: sandbox.SnapshotID,
		CreatedAt:  time.Now().UTC(),
	}
	parent.Status = "succeeded"
	parent.Result.SnapshotID = cp.SnapshotID
	frozen := encode(struct {
		Action  string
		Request Command
	}{"checkpoint", input.Command})
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,'succeeded',?)", []any{
			parent.ID,
			"tenant",
			input.RequestTenantID,
			parent.Key,
			digest(frozen),
			frozen,
			encode(parent.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			parent.ID,
			mac,
		}},
		{"INSERT INTO operation_steps VALUES(?,'lifecycle','checkpoint',?,?,?,?,'succeeded',?)", []any{
			parent.ID,
			digest(payload),
			payload,
			sandbox.ID,
			"",
			encode(lifecycleState{
				Phase:      "complete",
				SnapshotID: cp.SnapshotID,
			}),
		}},
		{"INSERT INTO checkpoint_bindings VALUES(?,?,?,?,?)", []any{
			cp.ID,
			cp.TenantID,
			cp.SnapshotID,
			parent.ID,
			encode(cp),
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'checkpoint_binding',?)", []any{
			cp.SnapshotID,
			cp.ID,
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'active_use',?)", []any{
			cp.TenantID,
			cp.SnapshotID,
			"checkpoint-binding:" + cp.ID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return Operation{}, err
		}
	}
	if err = service.fault("paused_checkpoint_before_commit"); err != nil {
		return Operation{}, err
	}
	if err = commitPublications(tx); err != nil {
		return Operation{}, err
	}
	return parent, service.store.checkpoint()
}
