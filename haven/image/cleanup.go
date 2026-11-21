package image

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type (
	ImageCleanupRequest struct {
		ImportOperationID string `json:"import_operation_id,omitempty"`
		CacheBuilder      string `json:"cache_builder,omitempty"`
		CacheDiffID       string `json:"cache_diff_id,omitempty"`
	}
	imageCleanupInput struct {
		RequestTenantID string           `json:"request_tenant_id"`
		Kind            string           `json:"kind"`
		ResourceID      string           `json:"resource_id"`
		Source          *imageImportStep `json:"source,omitempty"`
		Cache           *Cache           `json:"cache,omitempty"`
	}
	imageCleanupState struct {
		Phase   string                `json:"phase"`
		Intents []loom.DeletionIntent `json:"intents,omitempty"`
	}
	imageCleanupStep struct {
		OperationID string
		Hash        string
		Status      string
		Input       imageCleanupInput
		State       imageCleanupState
	}
)

func loadImageCleanup(ctx context.Context, q store.DBTX, id string) (imageCleanupStep, error) {
	step := imageCleanupStep{
		OperationID: id,
	}
	record, err := dbgen.New(q).GetStep(ctx, dbgen.GetStepParams{
		OperationID: id,
		StepKey:     "image_cleanup",
	})
	if err != nil {
		return step, err
	}
	payload, body := record.Input, record.Body
	action, candidate, expected := record.Action, record.CandidateResourceID, record.ExpectedInstance
	step.Hash, step.Status = record.InputHash, record.Status
	if operation.Digest(payload) != step.Hash {
		return step, fmt.Errorf("image cleanup input changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "image_cleanup" || candidate != step.Input.ResourceID || expected != "" || ((step.Input.Source == nil) == (step.Input.Cache == nil)) {
		return step, fmt.Errorf("image cleanup authority identity changed")
	}
	return step, nil
}

func imageCleanupFence(ctx context.Context, q store.DBTX, kind, id string) error {
	count, err := dbgen.New(q).CountPendingCleanupResource(ctx, dbgen.CountPendingCleanupResourceParams{
		ResourceKind: kind,
		ResourceID:   id,
	})
	if err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("%w: image artifact has an independent collector", errs.ErrConflict)
	}
	return nil
}

func (i *Images) AdmitCleanup(ctx context.Context, principal authorization.Principal, key string, request ImageCleanupRequest) (operation.State, error) {
	var empty operation.State
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	work := request.ImportOperationID != ""
	cache := request.CacheBuilder != "" && request.CacheDiffID != ""
	if key == "" || len(key) > 256 || work == cache || (work && (request.CacheBuilder != "" || request.CacheDiffID != "")) {
		return empty, fmt.Errorf("%w: image cleanup requires one exact import or cache", errs.ErrInvalid)
	}
	guard := i.guards.ForNodeIdempotencyKey(i.nodeID, key)
	guard.Lock()
	defer guard.Unlock()
	frozen := operation.Encode(struct {
		Action  string
		Request ImageCleanupRequest
	}{"image_cleanup", request})
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
	if !i.guards.ForImageArtifacts().TryLock() {
		return empty, fmt.Errorf("%w: artifact producer or collector active", errs.ErrConflict)
	}
	defer i.guards.ForImageArtifacts().Unlock()
	input := imageCleanupInput{
		RequestTenantID: principal.TenantID,
	}
	if request.ImportOperationID != "" {
		source, err := loadImageImport(ctx, i.database, request.ImportOperationID)
		if err != nil {
			return empty, err
		}
		if err = imageWorkCleanupEligible(source); err != nil {
			return empty, err
		}
		input.Kind = "import_work"
		input.ResourceID = source.OperationID
		input.Source = &source
	} else {
		cache, err := loadImageCache(ctx, i.database, request.CacheBuilder, request.CacheDiffID)
		if err != nil {
			return empty, err
		}
		input.Kind = "image_cache"
		input.ResourceID = request.CacheBuilder + ":" + request.CacheDiffID
		input.Cache = &cache
	}
	i.admission.Lock()
	defer i.admission.Unlock()
	if err := i.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := i.database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = imageCleanupFence(ctx, tx, input.Kind, input.ResourceID); err != nil {
		return empty, err
	}
	if input.Source != nil {
		current, err := loadImageImport(ctx, tx, input.Source.OperationID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Source) {
			return empty, fmt.Errorf("%w: original image import changed", errs.ErrConflict)
		}
		count, err := dbgen.New(tx).CountPendingOwnedCleanup(ctx, dbgen.CountPendingOwnedCleanupParams{
			ID:           "image_import:" + current.OperationID,
			OperationID:  current.OperationID,
			ResourceKind: "image_import",
		})
		if err != nil {
			return empty, err
		}
		if count != 1 {
			return empty, fmt.Errorf("%w: image import lacks pending cleanup responsibility", errs.ErrConflict)
		}
	} else {
		current, err := loadImageCache(ctx, tx, request.CacheBuilder, request.CacheDiffID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Cache) {
			return empty, fmt.Errorf("%w: cache ownership changed", errs.ErrConflict)
		}
		live, err := imageCacheLive(ctx, tx, current.Witness)
		if err != nil {
			return empty, err
		}
		if live {
			return empty, fmt.Errorf("%w: image cache has live consumers", errs.ErrConflict)
		}
	}
	accepted := operation.State{
		ID:     operation.NewID("op-"),
		Key:    key,
		Status: "pending",
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
		Status:    "pending",
		Body:      operation.Encode(accepted.Result),
	}); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         accepted.ID,
		StepKey:             "image_cleanup",
		Action:              "image_cleanup",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: input.ResourceID,
		ExpectedInstance:    "",
		Status:              "pending",
		Body: operation.Encode(imageCleanupState{
			Phase: "admitted",
		}),
	})); err != nil {
		return empty, err
	}
	if err = store.RequireOne(dbgen.New(tx).InsertCleanupIntent(ctx, dbgen.InsertCleanupIntentParams{
		ID:           "image_cleanup:" + accepted.ID,
		OperationID:  accepted.ID,
		ResourceKind: input.Kind,
		ResourceID:   input.ResourceID,
		Body:         payload,
	})); err != nil {
		return empty, err
	}
	if err = i.fault("image_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = i.checkpoint(); err != nil {
		return accepted, err
	}
	i.dispatch(accepted.ID)
	return accepted, nil
}
