package sandbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

// AdmitCold freezes a tenant-owned template and all resource identities with its
// slot, disk budget and source reference. Delivery remains separately fenced.
func (service *VMProvisioning) AdmitCold(ctx context.Context, request machine.ColdRequest) (operation.Record, error) {
	var empty operation.Record
	if request.Key == "" || len(request.Key) > 256 || request.TenantID == "" || request.TemplateID == "" {
		return empty, fmt.Errorf("%w: tenant, template and a key of 1..256 bytes are required", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.ColdRequest
	}{"claim", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(service.store.Database).Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	selected, err := supply.NewTemplateCatalog(service.store.Database).Cold(ctx, request.TenantID, request.TemplateID)
	if err != nil {
		return empty, err
	}
	if err = host.CheckConfig(&selected.Config); err != nil {
		return empty, err
	}
	if err = service.backend.Check(ctx, selected.Config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, selected.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	return service.Admit(ctx, request.TenantID, request.Key, frozen, hash, selected, manifest, nil, nil, nil)
}

func (service *VMProvisioning) AdmitClone(ctx context.Context, request machine.CloneRequest) (operation.Record, error) {
	var empty operation.Record
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, fmt.Errorf("%w: clone requires tenant, snapshot and key", errs.ErrInvalid)
	}
	frozen := operation.Encode(struct {
		Action  string
		Request machine.CloneRequest
	}{"clone", request})
	hash := operation.Digest(frozen)
	if cached, found, err := operation.New(service.store.Database).Cached(ctx, "tenant", request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := service.proofs.Snapshots().LoadRestoreSource(ctx, service.store.Database, request.TenantID, request.SnapshotID)
	if err == sql.ErrNoRows {
		return empty, fmt.Errorf("%w: snapshot not found", errs.ErrNotFound)
	}
	if err != nil {
		return empty, err
	}
	config := source.Snapshot.Manifest.Config
	if err = host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err = service.backend.Check(ctx, config); err != nil {
		return empty, err
	}
	manifest, err := image.LoadManifest(ctx, service.store.Database, config.Image)
	if err != nil {
		return empty, err
	}
	var frozenImage host.ImageManifest
	if err = json.Unmarshal(source.Snapshot.Manifest.Image, &frozenImage); err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(manifest, frozenImage) {
		return empty, fmt.Errorf("clone image publication changed")
	}
	if err = image.VerifyFiles(manifest, filepath.Join(service.store.Directory, "images", manifest.Digest[7:])); err != nil {
		return empty, err
	}
	return service.Admit(ctx, request.TenantID, request.Key, frozen, hash, supply.TemplateRecord{
		TenantID: request.TenantID,
		Config:   config,
	}, manifest, &source, nil, nil)
}

func (service *VMProvisioning) Admit(ctx context.Context, tenantID, key string, frozen []byte, hash string, selected supply.TemplateRecord, manifest host.ImageManifest, restore *machine.Source, claim *ClaimAdmission, target *ProvisionTarget) (operation.Record, error) {
	var empty operation.Record
	free, err := loom.FreeBytes(service.store.Directory)
	if err != nil {
		return empty, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	if err = service.gate.Check(); err != nil {
		return empty, err
	}
	tx, err := service.store.Database.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if claim != nil {
		previous, lookup := operation.New(tx).TenantCredentialMAC(ctx, tenantID, key)
		if lookup == nil && previous != claim.MAC {
			return empty, fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
		}
		if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
			return empty, lookup
		}
	}
	if cached, found, err := operation.New(tx).Cached(ctx, "tenant", tenantID, key, hash); found || err != nil {
		if claim != nil && found && err == nil && !operation.Terminal(cached.Status) {
			service.access.SetOperationSecrets(cached.ID, claim.Secrets)
		}
		if found && err == nil && !operation.Terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	accepted, err := service.AdmitIn(ctx, tx, tenantID, key, frozen, hash, selected, manifest, restore, claim, target, free)
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
		service.access.SetOperationSecrets(accepted.ID, claim.Secrets)
	}
	if err = service.store.Checkpoint(); err != nil {
		return accepted, err
	}
	if err = service.fault("admission_committed"); err != nil {
		return accepted, err
	}
	service.dispatch(accepted.ID)
	return accepted, nil
}

func (service *VMProvisioning) dispatch(id string) {
	if service.store.Reviewed {
		return
	}
	service.workers.Start(id, func() error {
		if handled, err := service.policies.Resume(service.ctx, id); handled || err != nil {
			return err
		}
		return service.Continue(id)
	})
}
