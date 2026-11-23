package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// SnapshotProofs verifies frozen publication history using the caller's database
// snapshot. Compatibility is an observation; this component performs no effects.
type SnapshotProofs struct {
	directory string
	backend   loom.SnapshotCompatibility
}

func NewSnapshotProofs(directory string, backend loom.SnapshotCompatibility) SnapshotProofs {
	return SnapshotProofs{
		directory: directory,
		backend:   backend,
	}
}

func (proofs SnapshotProofs) ValidateResource(step ResourceStep) error {
	return ValidateResourceStep(proofs.directory, step)
}

func (proofs SnapshotProofs) LoadSnapshotSource(ctx context.Context, query store.DBTX, tenant, id string) (SnapshotSource, error) {
	var source SnapshotSource
	row, err := dbgen.New(query).GetOwnedPublication(ctx, dbgen.GetOwnedPublicationParams{
		ID:       id,
		TenantID: tenant,
	})
	if err != nil {
		return source, err
	}
	if err = json.Unmarshal(row.Body, &source.Snapshot); err != nil {
		return source, err
	}
	if row.Published != 1 || row.Deleting != 0 || row.SourceKind != "capture" || source.Snapshot.ID != id || source.Snapshot.TenantID != tenant || source.Snapshot.OperationID != row.SourceOperationID || source.Snapshot.Manifest == nil {
		return source, fmt.Errorf("%w: snapshot publication/provenance incomplete", errs.ErrManualRecovery)
	}
	capture, err := LoadCapture(ctx, query, row.SourceOperationID)
	if errors.Is(err, sql.ErrNoRows) {
		return source, fmt.Errorf("%w: snapshot publication lacks its capture step", errs.ErrManualRecovery)
	}
	if err != nil {
		return source, err
	}
	if capture.State.Boundary == nil {
		return source, fmt.Errorf("capture publication identity missing")
	}
	source.CaptureOperation = row.SourceOperationID
	source.CaptureHash = capture.Hash
	source.Publication = capture.State.Boundary.Publication
	if err = proofs.ValidateRestoreSource(source, tenant, capture); err != nil {
		return source, err
	}
	return source, nil
}

func (proofs SnapshotProofs) ValidateSnapshotPublication(source SnapshotSource, tenant string, capture CaptureStep) error {
	publication := source.Snapshot
	if capture.Hash != source.CaptureHash || capture.OperationID != source.CaptureOperation || capture.State.Boundary == nil || capture.State.Boundary.Publication != source.Publication {
		return fmt.Errorf("restore publication history identity changed")
	}
	if publication.TenantID != tenant || capture.Input.Request.TenantID != tenant || publication.OperationID != capture.OperationID {
		return fmt.Errorf("snapshot publication tenant or operation differs from capture")
	}
	if capture.Status != "succeeded" || capture.State.Phase != "complete" || capture.State.ResumeRequired {
		return fmt.Errorf("snapshot capture has not completed")
	}
	if !capture.State.Captured || !capture.State.Materialized || !capture.State.ArtifactsSynced {
		return fmt.Errorf("snapshot capture lacks synchronized artifacts")
	}
	if publication.Manifest == nil || len(capture.State.Files) != 4 {
		return fmt.Errorf("snapshot publication lacks manifest or capture files")
	}
	expected := capture.Input.Snapshot
	expected.Manifest = publication.Manifest
	if !reflect.DeepEqual(expected, publication) || !reflect.DeepEqual(capture.State.Files, publication.Manifest.Files) || publication.Dir != filepath.Join(proofs.directory, "snapshots", publication.ID) {
		return fmt.Errorf("restore snapshot differs from original capture")
	}
	manifest := publication.Manifest
	if manifest.Format != 1 || manifest.Backend != capture.Input.Backend || manifest.CPU != capture.Input.CPU || manifest.Architecture != capture.Input.Architecture || manifest.MemoryMode != "CopyOnWrite" || manifest.PublicationMode != "IndependentInodes" || !reflect.DeepEqual(manifest.Config, capture.Input.Source.Input.Resource.Config) {
		return fmt.Errorf("%w: restore backend/CPU/shape/publication mismatch", errs.ErrIncompatible)
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

func (proofs SnapshotProofs) ValidateRestoreSource(source SnapshotSource, tenant string, capture CaptureStep) error {
	if err := proofs.ValidateSnapshotPublication(source, tenant, capture); err != nil {
		return err
	}
	manifest := source.Snapshot.Manifest
	if manifest.Backend != proofs.backend.Compatibility() || manifest.CPU != host.CPUIdentity() || manifest.Architecture != runtime.GOARCH {
		return fmt.Errorf("%w: restore backend/CPU mismatch", errs.ErrIncompatible)
	}
	return nil
}

func (proofs SnapshotProofs) SnapshotPublication(ctx context.Context, query store.DBTX, tenant, id string) (SnapshotSource, error) {
	var source SnapshotSource
	row, err := dbgen.New(query).GetOwnedPublication(ctx, dbgen.GetOwnedPublicationParams{
		ID:       id,
		TenantID: tenant,
	})
	if err != nil {
		return source, err
	}
	if err = json.Unmarshal(row.Body, &source.Snapshot); err != nil {
		return source, err
	}
	capture, err := LoadCapture(ctx, query, row.SourceOperationID)
	if err != nil {
		return source, err
	}
	if row.Published != 1 || row.Deleting != 0 || row.SourceKind != "capture" || capture.State.Boundary == nil || source.Snapshot.ID != id {
		return source, fmt.Errorf("snapshot delete requires unmarked published capture")
	}
	source.CaptureOperation = row.SourceOperationID
	source.CaptureHash = capture.Hash
	source.Publication = capture.State.Boundary.Publication
	return source, proofs.ValidateSnapshotPublication(source, tenant, capture)
}
