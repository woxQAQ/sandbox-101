package haven

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
)

func (service *Service) AdmitImageImport(ctx context.Context, principal Principal, key string, options imageimport.ImageImportOptions) (Operation, error) {
	var empty Operation
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, loom.Fail(loom.NotFound, "not found")
	}
	if key == "" || len(key) > 256 {
		return empty, loom.Fail(loom.Invalid, "image import requires bounded key")
	}
	if service.store.reviewed {
		return empty, loom.Fail(loom.ManualRecovery, "reviewed rollback cannot admit image effects")
	}
	options, err := imageimport.NormalizeImageImportOptions(options)
	if err != nil {
		return empty, err
	}
	guard := service.vmLock("node-request:" + string(encode([]string{
		service.store.identity.Identity,
		key,
	})))
	guard.Lock()
	defer guard.Unlock()
	frozen := encode(struct {
		Action  string
		Options imageimport.ImageImportOptions
	}{"image_import", options})
	hash := digest(frozen)
	if cached, found, err := cachedScopedOperation(ctx, service.store.db, "node", service.store.identity.Identity, key, hash); found || err != nil {
		if found && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	service.artifacts.Lock()
	defer service.artifacts.Unlock()
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	input := imageImportInput{
		RequestTenantID: principal.TenantID,
		Options:         options,
		Target:          filepath.Join(service.store.directory, "images", options.Digest[7:]),
	}
	existing, err := loadImage(ctx, service.store.db, options.Digest)
	if err == nil {
		if existing.Layout != options.Materialization {
			return empty, loom.Fail(loom.Conflict, "published digest already uses another materialization")
		}
		if err = verifyImageFiles(existing, input.Target); err != nil {
			return empty, err
		}
		input.Existing = &existing
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	} else {
		if _, err = os.Lstat(input.Target); !os.IsNotExist(err) {
			return empty, loom.Fail(loom.ManualRecovery, "image destination lacks catalog publication proof")
		}
		plan, err := imageimport.ReadImageImportPlan(ctx, service.store.directory, options)
		if err != nil {
			return empty, err
		}
		input.Plan = &plan
		input.Reservation = options.MaxBytes*4 + options.MaxFiles*8192 + 32<<20
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	operation := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
	}
	input.Work = filepath.Join(service.store.directory, "imports", operation.ID)
	state := imageImportState{
		Phase: "admitted",
	}
	if input.Existing != nil {
		current, err := loadImage(ctx, tx, options.Digest)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Existing) {
			return empty, loom.Fail(loom.Conflict, "published image changed")
		}
		operation.Status = "succeeded"
		operation.Result.ImageDigest = options.Digest
		state.Phase = "complete"
	} else {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM image_intents WHERE digest=?", options.Digest).Scan(&count); err != nil {
			return empty, err
		}
		if count != 0 {
			return empty, loom.Fail(loom.Conflict, "image digest retains another import intent")
		}
		var reserved int64
		if err = tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(bytes),0) FROM reservations").Scan(&reserved); err != nil {
			return empty, err
		}
		available, err := loom.FreeBytes(service.store.directory)
		if err != nil {
			return empty, err
		}
		if reserved > available || service.opts.SafetyBytes > available-reserved || input.Reservation > available-reserved-service.opts.SafetyBytes {
			return empty, loom.Fail(loom.Exhausted, "insufficient image import free space")
		}
	}
	payload := encode(input)
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
			frozen,
			operation.Status,
			encode(operation.Result),
		}},
		{"INSERT INTO operation_steps VALUES(?,'image_import','image_import',?,?,?,? ,?,?)", []any{
			operation.ID,
			digest(payload),
			payload,
			options.Digest,
			"",
			operation.Status,
			encode(state),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if input.Existing == nil {
		if input.Plan.Options.Materialization == "layered-erofs-v1" {
			for _, layer := range input.Plan.Layers {
				if err = imageCleanupFence(ctx, tx, "image_cache", input.Plan.Builder+":"+layer.DiffID); err != nil {
					return empty, err
				}
			}
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO image_intents VALUES(?,?,?,?)", options.Digest, operation.ID, options.Materialization, payload)); err != nil {
			return empty, err
		}
		if err = requireWrite(tx.ExecContext(ctx, "INSERT INTO reservations VALUES(?,?,0,0)", "import:"+operation.ID, input.Reservation)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("image_import_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if !terminal(operation.Status) {
		service.dispatch(operation.ID)
	}
	return operation, nil
}
