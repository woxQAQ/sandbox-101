package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

// AdmitCold freezes a tenant-owned template and all resource identities with its
// slot, disk budget and source reference. Delivery remains separately fenced.
func (service *Service) AdmitCold(ctx context.Context, request ColdRequest) (Operation, error) {
	var empty Operation
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.TemplateID == "" {
		return empty, loom.Fail(loom.Invalid, "tenant, template and a key of 1..256 bytes are required")
	}
	frozen := encode(struct {
		Action  string
		Request ColdRequest
	}{"claim", request})
	hash := digest(frozen)
	if cached, found, err := cachedOperation(ctx, service.store.db, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	template, err := loadColdTemplate(ctx, service.store.db, request.TenantID, request.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&template.Config); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, template.Config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	return service.admitProvision(ctx, request.TenantID, request.Key, frozen, hash, template, image, nil, nil, nil)
}

func (service *Service) admitProvision(ctx context.Context, tenantID, key string, frozen []byte, hash string, template templateRecord, image host.ImageManifest, restore *restoreSource, claim *claimAdmission, target *provisionTarget) (Operation, error) {
	var empty Operation
	free, err := loom.FreeBytes(service.store.directory)
	if err != nil {
		return empty, err
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
	if claim != nil {
		var previous string
		lookup := tx.QueryRowContext(ctx, "SELECT c.request_mac FROM operations o JOIN operation_credentials c ON c.operation_id=o.id WHERE o.scope_kind='tenant' AND o.scope_id=? AND o.idem_key=?", tenantID, key).Scan(&previous)
		if lookup == nil && previous != claim.MAC {
			return empty, loom.Fail(loom.Conflict, "idempotency key has different parameters")
		}
		if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return empty, lookup
		}
	}
	if cached, found, err := cachedOperation(ctx, tx, tenantID, key, hash); found || err != nil {
		if claim != nil && found && err == nil && !terminal(cached.Status) {
			service.mu.Lock()
			service.opSecrets[cached.ID] = claim.Secrets
			service.mu.Unlock()
		}
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	operation, err := service.admitProvisionIn(ctx, tx, tenantID, key, frozen, hash, template, image, restore, claim, target, free)
	if err != nil {
		return empty, err
	}
	if err = service.fault("admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if claim != nil {
		service.mu.Lock()
		service.opSecrets[operation.ID] = claim.Secrets
		service.mu.Unlock()
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	if err = service.fault("admission_committed"); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func cachedOperation(ctx context.Context, q queryer, tenant, key, hash string) (Operation, bool, error) {
	return cachedScopedOperation(ctx, q, "tenant", tenant, key, hash)
}

func cachedScopedOperation(ctx context.Context, q queryer, kind, scope, key, hash string) (Operation, bool, error) {
	var operation Operation
	var request, body []byte
	var storedHash string
	err := q.QueryRowContext(ctx, "SELECT id,idem_key,status,request_hash,request,body FROM operations WHERE scope_kind=? AND scope_id=? AND idem_key=?", kind, scope, key).Scan(&operation.ID, &operation.Key, &operation.Status, &storedHash, &request, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return operation, false, nil
	}
	if err != nil {
		return operation, false, err
	}
	if digest(request) != storedHash {
		return operation, true, fmt.Errorf("operation request identity corrupted")
	}
	if err = json.Unmarshal(body, &operation.Result); err != nil {
		return operation, true, err
	}
	if hash != storedHash {
		return operation, true, loom.Fail(loom.Conflict, "key belongs to a different request")
	}
	return operation, true, nil
}

func loadOwnedTemplate(ctx context.Context, q queryer, tenant, id string) (templateRecord, error) {
	var template templateRecord
	var body []byte
	var owner, scope, kind, status, publication string
	err := q.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=? AND tenant=?", id, tenant).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return template, loom.Fail(loom.NotFound, "template not found")
	}
	if err != nil {
		return template, err
	}
	if err = json.Unmarshal(body, &template); err != nil {
		return template, err
	}
	err = q.QueryRowContext(ctx, `SELECT i.tenant_id,o.scope_kind,o.scope_id,o.status,i.operation_id FROM template_intents i JOIN operations o ON o.id=i.operation_id WHERE i.template_id=?`, id).Scan(&owner, &kind, &scope, &status, &publication)
	if errors.Is(err, sql.ErrNoRows) {
		return template, loom.Fail(loom.ManualRecovery, "template permanent ownership proof is missing")
	}
	if err != nil {
		return template, err
	}
	step, err := loadSupply(ctx, q, publication)
	if err != nil {
		return template, loom.Fail(loom.ManualRecovery, "template immutable publication step is missing")
	}
	if owner != tenant || kind != "tenant" || scope != step.Input.RequestTenantID || (!step.Input.RequesterRoot && scope != tenant) || step.Input.Template.ID != id || step.Input.Template.TenantID != tenant || (step.Input.Command.Action != "template_put" && step.Input.Command.Action != "template_build") {
		return template, loom.Fail(loom.ManualRecovery, "template publisher authorization is inconsistent")
	}
	if (status == "pending" || status == "running") && !terminal(step.Status) {
		return template, loom.Fail(loom.Conflict, "template publication is in progress")
	}
	expected := step.Input.Template
	expected.State = "ready"
	expected.SnapshotID = step.State.SnapshotID
	if template.ID != id || template.TenantID != tenant || template.Version != publication || status != "succeeded" || step.Status != "succeeded" || step.State.Phase != "complete" || !reflect.DeepEqual(template, expected) {
		return template, loom.Fail(loom.ManualRecovery, "template immutable publication differs from catalog")
	}
	return template, nil
}

func loadColdTemplate(ctx context.Context, q queryer, tenant, id string) (templateRecord, error) {
	template, err := loadOwnedTemplate(ctx, q, tenant, id)
	if err != nil {
		return template, err
	}
	if template.SnapshotID != "" || template.TargetWarm != 0 {
		return template, loom.Fail(loom.Incompatible, "cold workflow requires a cold template without a warm pool")
	}
	return template, nil
}

func verifyImageFiles(manifest host.ImageManifest, directory string) error {
	var disk host.ImageManifest
	if err := loom.ReadJSON(filepath.Join(directory, "image.json"), &disk); err != nil {
		return err
	}
	if !reflect.DeepEqual(disk, manifest) {
		return fmt.Errorf("image file manifest differs from frozen catalog")
	}
	return manifest.Verify(directory, manifest.Digest)
}

func loadImage(ctx context.Context, q queryer, digest string) (host.ImageManifest, error) {
	var manifest host.ImageManifest
	var body []byte
	if err := q.QueryRowContext(ctx, "SELECT body FROM images WHERE digest=?", digest).Scan(&body); err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(body, &manifest); err != nil {
		return manifest, err
	}
	if manifest.Digest != digest {
		return manifest, fmt.Errorf("image catalog identity inconsistent")
	}
	return manifest, nil
}
