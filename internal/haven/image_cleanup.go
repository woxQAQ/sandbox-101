package haven

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type (
	ImageCleanupRequest struct {
		ImportOperationID string `json:"import_operation_id,omitempty"`
		CacheBuilder      string `json:"cache_builder,omitempty"`
		CacheDiffID       string `json:"cache_diff_id,omitempty"`
	}
	imageCleanupInput struct {
		RequestTenantID string            `json:"request_tenant_id"`
		Kind            string            `json:"kind"`
		ResourceID      string            `json:"resource_id"`
		Source          *imageImportStep  `json:"source,omitempty"`
		Cache           *imageCacheRecord `json:"cache,omitempty"`
	}
	imageCleanupState struct {
		Phase   string                `json:"phase"`
		Intents []loom.DeletionIntent `json:"intents,omitempty"`
	}
	imageCleanupStep struct {
		OperationID, Hash, Status string
		Input                     imageCleanupInput
		State                     imageCleanupState
	}
)

func loadImageCleanup(ctx context.Context, q queryer, id string) (imageCleanupStep, error) {
	step := imageCleanupStep{OperationID: id}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='image_cleanup'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
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

func imageCleanupFence(ctx context.Context, q queryer, kind, id string) error {
	var count int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE resource_kind=? AND resource_id=? AND status='pending'", kind, id).Scan(&count)
	if err != nil {
		return err
	}
	if count != 0 {
		return loom.Fail(loom.Conflict, "image artifact has an independent collector")
	}
	return nil
}

func (service *Service) AdmitImageCleanup(ctx context.Context, principal Principal, key string, request ImageCleanupRequest) (Operation, error) {
	var empty Operation
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return empty, loom.Fail(loom.NotFound, "not found")
	}
	work := request.ImportOperationID != ""
	cache := request.CacheBuilder != "" && request.CacheDiffID != ""
	if key == "" || len(key) > 256 || work == cache || (work && (request.CacheBuilder != "" || request.CacheDiffID != "")) {
		return empty, loom.Fail(loom.Invalid, "image cleanup requires one exact import or cache")
	}
	guard := service.vmLock("node-request:" + string(encode([]string{service.store.identity.Identity, key})))
	guard.Lock()
	defer guard.Unlock()
	frozen := encode(struct {
		Action  string
		Request ImageCleanupRequest
	}{"image_cleanup", request})
	hash := digest(frozen)
	if cached, found, err := cachedScopedOperation(ctx, service.store.db, "node", service.store.identity.Identity, key, hash); found || err != nil {
		if found && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	if !service.artifacts.TryLock() {
		return empty, loom.Fail(loom.Conflict, "artifact producer or collector active")
	}
	defer service.artifacts.Unlock()
	input := imageCleanupInput{RequestTenantID: principal.TenantID}
	if request.ImportOperationID != "" {
		source, err := loadImageImport(ctx, service.store.db, request.ImportOperationID)
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
		cache, err := loadImageCache(ctx, service.store.db, request.CacheBuilder, request.CacheDiffID)
		if err != nil {
			return empty, err
		}
		input.Kind = "image_cache"
		input.ResourceID = request.CacheBuilder + ":" + request.CacheDiffID
		input.Cache = &cache
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = imageCleanupFence(ctx, tx, input.Kind, input.ResourceID); err != nil {
		return empty, err
	}
	if input.Source != nil {
		current, err := loadImageImport(ctx, tx, input.Source.OperationID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Source) {
			return empty, loom.Fail(loom.Conflict, "original image import changed")
		}
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cleanup_intents WHERE id=? AND operation_id=? AND resource_kind='image_import' AND status='pending'", "image_import:"+current.OperationID, current.OperationID).Scan(&count); err != nil {
			return empty, err
		}
		if count != 1 {
			return empty, loom.Fail(loom.Conflict, "image import lacks pending cleanup responsibility")
		}
	} else {
		current, err := loadImageCache(ctx, tx, request.CacheBuilder, request.CacheDiffID)
		if err != nil {
			return empty, err
		}
		if !reflect.DeepEqual(current, *input.Cache) {
			return empty, loom.Fail(loom.Conflict, "cache ownership changed")
		}
		live, err := imageCacheLive(ctx, tx, current.Witness)
		if err != nil {
			return empty, err
		}
		if live {
			return empty, loom.Fail(loom.Conflict, "image cache has live consumers")
		}
	}
	operation := Operation{ID: newID("op-"), Key: key, Status: "pending"}
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{operation.ID, "node", service.store.identity.Identity, key, hash, frozen, "pending", encode(operation.Result)}},
		{"INSERT INTO operation_steps VALUES(?,'image_cleanup','image_cleanup',?,?,?,?,'pending',?)", []any{operation.ID, digest(payload), payload, input.ResourceID, "", encode(imageCleanupState{Phase: "admitted"})}},
		{"INSERT INTO cleanup_intents VALUES(?,?,?,?,'pending',?)", []any{"image_cleanup:" + operation.ID, operation.ID, input.Kind, input.ResourceID, payload}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("image_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}
