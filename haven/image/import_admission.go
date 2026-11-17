package image

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
)

func (i *Images) AdmitImport(ctx context.Context, principal authorization.Principal, key string, options imageimport.ImageImportOptions) (operation.Record, error) {
	var empty operation.Record
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	if key == "" || len(key) > 256 {
		return empty, fmt.Errorf("%w: image import requires bounded key", errs.ErrInvalid)
	}
	if i.reviewed {
		return empty, fmt.Errorf("%w: reviewed rollback cannot admit image effects", errs.ErrManualRecovery)
	}
	options, err := imageimport.NormalizeImageImportOptions(options)
	if err != nil {
		return empty, err
	}
	guard := i.guards.NodeRequest(i.nodeID, key)
	guard.Lock()
	defer guard.Unlock()
	frozen := operation.Encode(struct {
		Action  string
		Options imageimport.ImageImportOptions
	}{"image_import", options})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(i.database).Cached(ctx, operation.Scope{
		Kind: "node",
		ID:   i.nodeID,
	}, key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			i.dispatch(cached.ID)
		}
		return cached, err
	}
	i.guards.Artifacts().Lock()
	defer i.guards.Artifacts().Unlock()
	if err = ctx.Err(); err != nil {
		return empty, err
	}
	input := imageImportInput{
		RequestTenantID: principal.TenantID,
		Options:         options,
		Target:          filepath.Join(i.directory, "images", options.Digest[7:]),
	}
	existing, err := LoadManifest(ctx, i.database, options.Digest)
	if err == nil {
		if existing.Layout != options.Materialization {
			return empty, fmt.Errorf("%w: published digest already uses another materialization", errs.ErrConflict)
		}
		if err = VerifyFiles(existing, input.Target); err != nil {
			return empty, err
		}
		input.Existing = &existing
	} else if !errors.Is(err, sql.ErrNoRows) {
		return empty, err
	} else {
		if _, err = os.Lstat(input.Target); !os.IsNotExist(err) {
			return empty, fmt.Errorf("%w: image destination lacks catalog publication proof", errs.ErrManualRecovery)
		}
		plan, err := imageimport.ReadImageImportPlan(ctx, i.directory, options)
		if err != nil {
			return empty, err
		}
		input.Plan = &plan
		input.Reservation = options.MaxBytes*4 + options.MaxFiles*8192 + 32<<20
	}
	i.admission.Lock()
	defer i.admission.Unlock()
	if err = i.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := i.database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	accepted := operation.Record{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
	}
	input.Work = filepath.Join(i.directory, "imports", accepted.ID)
	state := imageImportState{
		Phase: "admitted",
	}
	catalog := dbgen.New(tx)
	responsibilities := quota.New(tx)
	if input.Existing != nil {
		current, err := LoadManifest(ctx, tx,

			options.Digest)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Existing) {
			return empty, fmt.Errorf("%w: published image changed", errs.ErrConflict)
		}
		accepted.Status = "succeeded"
		accepted.Result.ImageDigest = options.Digest
		state.Phase = "complete"
	} else {
		count, err := catalog.CountImportIntent(ctx, options.Digest)
		if err != nil {
			return empty, err
		}
		if count != 0 {
			return empty, fmt.Errorf("%w: image digest retains another import intent", errs.ErrConflict)
		}
		reserved, err := responsibilities.ReservedBytes(ctx)
		if err != nil {
			return empty, err
		}
		available, err := loom.FreeBytes(i.directory)
		if err != nil {
			return empty, err
		}
		if reserved > available || i.safetyBytes > available-reserved || input.Reservation > available-reserved-i.safetyBytes {
			return empty, fmt.Errorf("%w: insufficient image import free space", errs.ErrExhausted)
		}
	}
	payload := operation.Encode(input)
	operations := operation.New(tx)
	if err = operations.Insert(ctx, operation.Admission{
		ID:        accepted.ID,
		ScopeKind: "node",
		ScopeID:   i.nodeID,
		Key:       key,
		Hash:      hash,
		Request:   frozen,
		Status:    accepted.Status,
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "image_import",
		Action:              "image_import",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: options.Digest,
		ExpectedInstance:    "",
		Status:              accepted.Status,
		Body:                operation.Encode(state),
	})); err != nil {
		return empty, err
	}
	if input.Existing == nil {
		if input.Plan.Options.Materialization == "layered-erofs-v1" {
			for _, layer := range input.Plan.Layers {
				if err = imageCleanupFence(ctx, tx, "image_cache", input.Plan.Builder+":"+layer.DiffID); err != nil {
					return empty, err
				}
			}
		}
		if err = store.RequireOne(catalog.InsertImportIntent(ctx, dbgen.InsertImportIntentParams{
			Digest:      options.Digest,
			OperationID: accepted.ID,
			Layout:      options.Materialization,
			Body:        payload,
		})); err != nil {
			return empty, err
		}
		if err = responsibilities.Reserve(ctx, quota.Reservation{
			Owner: "import:" + accepted.ID,
			Bytes: input.Reservation,
		}); err != nil {
			return empty, err
		}
	}
	if err = i.fault("image_import_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = i.checkpoint(); err != nil {
		return accepted, err
	}
	if !operation.Terminal(accepted.Status) {
		i.dispatch(accepted.ID)
	}
	return accepted, nil
}
