package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

type CloneRequest struct {
	TenantID   string `json:"tenant_id"`
	Key        string `json:"key"`
	SnapshotID string `json:"snapshot_id"`
}

// restoreSource freezes delivery facts and names the immutable capture history.
// References avoid recursively copying ancestry into every clone generation.
// Neither source catalog nor source files are needed after backing transfer.
type restoreSource struct {
	Snapshot         snapshotRecord    `json:"snapshot"`
	CaptureOperation string            `json:"capture_operation"`
	CaptureHash      string            `json:"capture_hash"`
	Publication      loom.FileIdentity `json:"publication"`
}

func (service *Service) loadRestoreSource(ctx context.Context, query queryer, tenant, id string) (restoreSource, error) {
	var source restoreSource
	var body []byte
	var operation, kind string
	var published, deleting int
	err := query.QueryRowContext(ctx, "SELECT s.published,s.deleting,s.body,o.source_operation_id,o.source_kind FROM snapshots s JOIN snapshot_owners o ON o.snapshot_id=s.id WHERE s.id=? AND o.tenant_id=?", id, tenant).Scan(&published, &deleting, &body, &operation, &kind)
	if err != nil {
		return source, err
	}
	if err = json.Unmarshal(body, &source.Snapshot); err != nil {
		return source, err
	}
	if published != 1 || deleting != 0 || kind != "capture" || source.Snapshot.ID != id || source.Snapshot.TenantID != tenant || source.Snapshot.OperationID != operation || source.Snapshot.Manifest == nil {
		return source, loom.Fail(loom.ManualRecovery, "snapshot publication/provenance incomplete")
	}
	capture, err := loadCapture(ctx, query, operation)
	if err != nil {
		return source, err
	}
	if capture.State.Boundary == nil {
		return source, fmt.Errorf("capture publication identity missing")
	}
	source.CaptureOperation = operation
	source.CaptureHash = capture.Hash
	source.Publication = capture.State.Boundary.Publication
	if err = service.validateRestoreSource(source, tenant, capture); err != nil {
		return source, err
	}
	return source, nil
}

func (service *Service) validateSnapshotPublication(source restoreSource, tenant string, capture captureStep) error {
	snapshot := source.Snapshot
	if capture.Hash != source.CaptureHash || capture.OperationID != source.CaptureOperation || capture.State.Boundary == nil || capture.State.Boundary.Publication != source.Publication {
		return fmt.Errorf("restore publication history identity changed")
	}
	if snapshot.TenantID != tenant || snapshot.Manifest == nil || snapshot.OperationID != capture.OperationID || capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired || !capture.State.Captured || !capture.State.Materialized || !capture.State.ArtifactsSynced || capture.State.Boundary == nil || len(capture.State.Files) != 4 || capture.Input.Request.TenantID != tenant {
		return fmt.Errorf("restore historical publication proof missing")
	}
	expected := capture.Input.Snapshot
	expected.Manifest = snapshot.Manifest
	if !reflect.DeepEqual(expected, snapshot) || !reflect.DeepEqual(capture.State.Files, snapshot.Manifest.Files) || snapshot.Dir != filepath.Join(service.store.directory, "snapshots", snapshot.ID) {
		return fmt.Errorf("restore snapshot differs from original capture")
	}
	manifest := snapshot.Manifest
	if manifest.Format != 1 || manifest.Backend != capture.Input.Backend || manifest.CPU != capture.Input.CPU || manifest.Architecture != capture.Input.Architecture || manifest.MemoryMode != "CopyOnWrite" || manifest.PublicationMode != "IndependentInodes" || !reflect.DeepEqual(manifest.Config, capture.Input.Source.Input.Resource.Config) {
		return loom.Fail(loom.Incompatible, "restore backend/CPU/shape/publication mismatch")
	}
	var image host.ImageManifest
	if err := json.Unmarshal(manifest.Image, &image); err != nil {
		return err
	}
	if !reflect.DeepEqual(image, capture.Input.Source.Input.Image) {
		return fmt.Errorf("restore image differs from historical capture")
	}
	return nil
}

func (service *Service) validateRestoreSource(source restoreSource, tenant string, capture captureStep) error {
	if err := service.validateSnapshotPublication(source, tenant, capture); err != nil {
		return err
	}
	manifest := source.Snapshot.Manifest
	if manifest.Backend != service.opts.Backend.Compatibility() || manifest.CPU != host.CPUIdentity() || manifest.Architecture != runtime.GOARCH {
		return loom.Fail(loom.Incompatible, "restore backend/CPU mismatch")
	}
	return nil
}

func verifyRestoreSourceFiles(source restoreSource) error {
	directory, err := loom.OpenCaptureDir(source.Snapshot.Dir)
	if err != nil {
		return err
	}
	info, err := directory.Stat()
	closeErr := directory.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if loom.IdentityOf(info) != source.Publication {
		return fmt.Errorf("restore source directory identity changed")
	}
	if err = loom.VerifyFiles(source.Snapshot.Dir, source.Snapshot.Manifest.Files); err != nil {
		return err
	}
	return loom.ValidateCOWTopology(filepath.Join(source.Snapshot.Dir, "ch"), uint64(source.Snapshot.Manifest.Config.Shape.MemoryMiB)<<20)
}

func (service *Service) AdmitClone(ctx context.Context, request CloneRequest) (Operation, error) {
	var empty Operation
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.SnapshotID, "snap-") {
		return empty, loom.Fail(loom.Invalid, "clone requires tenant, snapshot and key")
	}
	frozen := encode(struct {
		Action  string
		Request CloneRequest
	}{"clone", request})
	hash := digest(frozen)
	if cached, found, err := cachedOperation(ctx, service.store.db, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	source, err := service.loadRestoreSource(ctx, service.store.db, request.TenantID, request.SnapshotID)
	if err == sql.ErrNoRows {
		return empty, loom.Fail(loom.NotFound, "snapshot not found")
	}
	if err != nil {
		return empty, err
	}
	config := source.Snapshot.Manifest.Config
	if err = host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, config.Image)
	if err != nil {
		return empty, err
	}
	var frozenImage host.ImageManifest
	if err = json.Unmarshal(source.Snapshot.Manifest.Image, &frozenImage); err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(image, frozenImage) {
		return empty, fmt.Errorf("clone image publication changed")
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	return service.admitProvision(ctx, request.TenantID, request.Key, frozen, hash, templateRecord{
		TenantID: request.TenantID,
		Config:   config,
	}, image, &source, nil, nil)
}
