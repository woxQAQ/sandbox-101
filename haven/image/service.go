package image

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// ImageGuards exposes the existing lock domains used by image producers and collectors.
type ImageGuards interface {
	// ForStoreMutation returns the node-wide lock for durable admission, state publication and final store close.
	ForStoreMutation() execution.Guard
	// ForImageArtifacts returns the lock shared by image publication, cleanup and GC inventory.
	ForImageArtifacts() execution.Guard
	// ForNodeIdempotencyKey returns the lock shared by admissions using this node's key.
	ForNodeIdempotencyKey(nodeID, key string) execution.Guard
	// ForImageImport returns the lock for advancing one image import operation.
	ForImageImport(operationID string) execution.Guard
	// ForImageCleanup returns the lock for advancing one image cleanup operation.
	ForImageCleanup(operationID string) execution.Guard
}

type ImageOptions struct {
	SafetyBytes int64
	Fault       func(string) error
}

// Images coordinates import, immutable publication, cache funding and collection.
// It borrows the shared store and execution owners; it does not close either.
type Images struct {
	database    *store.Database
	directory   string
	nodeID      string
	reviewed    bool
	safetyBytes int64
	checkpoint  func() error
	faultPoint  func(string) error
	ctx         context.Context
	guards      ImageGuards
	admission   execution.Guard
	gate        execution.AdmissionGate
	workers     *execution.Workers
}

type ImageDependencies struct {
	Guards  ImageGuards
	Gate    execution.AdmissionGate
	Workers *execution.Workers
}

func NewImages(state *store.State, dependencies ImageDependencies, options ImageOptions) *Images {
	return &Images{
		database:    state.Database(),
		directory:   state.Directory(),
		nodeID:      state.Identity().Identity,
		reviewed:    state.Reviewed(),
		safetyBytes: options.SafetyBytes,
		checkpoint:  state.Checkpoint,
		faultPoint:  options.Fault,
		ctx:         dependencies.Workers.Context(),
		guards:      dependencies.Guards,
		admission:   dependencies.Guards.ForStoreMutation(),
		gate:        dependencies.Gate,
		workers:     dependencies.Workers,
	}
}

func (i *Images) fault(point string) error {
	if i.faultPoint != nil {
		return i.faultPoint(point)
	}
	return nil
}

func (i *Images) dispatch(id string) {
	if i.reviewed {
		return
	}
	i.workers.Start(id, func() error {
		handled, err := i.Resume(id)
		if err != nil {
			return err
		}
		if !handled {
			return fmt.Errorf("image operation lacks matching immutable step")
		}
		return nil
	})
}

// Resume selects existing frozen steps, preserving cleanup-before-import order.
// An absent image step lets the application's dispatcher try its next use case.
func (i *Images) Resume(id string) (bool, error) {
	if i.reviewed {
		return false, fmt.Errorf("%w: reviewed rollback cannot resume image effects", errs.ErrManualRecovery)
	}
	cleanup, err := loadImageCleanup(i.ctx, i.database, id)
	if err == nil {
		guard := i.guards.ForImageCleanup(id)
		guard.Lock()
		defer guard.Unlock()
		cleanup, err = loadImageCleanup(i.ctx, i.database, id)
		if err != nil {
			return true, err
		}
		if operation.Terminal(cleanup.Status) {
			return true, nil
		}
		i.guards.ForImageArtifacts().Lock()
		defer i.guards.ForImageArtifacts().Unlock()
		return true, i.runImageCleanup(cleanup)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	step, err := loadImageImport(i.ctx, i.database, id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	guard := i.guards.ForImageImport(id)
	guard.Lock()
	defer guard.Unlock()
	step, err = loadImageImport(i.ctx, i.database, id)
	if err != nil {
		return true, err
	}
	if operation.Terminal(step.Status) {
		return true, nil
	}
	i.guards.ForImageArtifacts().Lock()
	defer i.guards.ForImageArtifacts().Unlock()
	return true, i.runImageImport(step)
}

func ValidateImageImport(ctx context.Context, database store.DBTX, id string) error {
	_, err := loadImageImport(ctx, database, id)
	return err
}

func ValidateImageCleanup(ctx context.Context, database store.DBTX, id string) error {
	_, err := loadImageCleanup(ctx, database, id)
	return err
}

// CollectCandidate rechecks the artifact proof frozen by the GC parent before
// admitting a separately owned collector. Admission repeats current custody checks.
func (i *Images) CollectCandidate(ctx context.Context, principal authorization.Principal, key string, item types.GCItem) (operation.State, error) {
	if !principal.Root || principal.TenantID == "" || principal.SandboxID != "" {
		return operation.State{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	request := ImageCleanupRequest{
		ImportOperationID: item.ID,
	}
	switch item.Kind {
	case "image_cache":
		record, err := loadImageCache(ctx, i.database, item.CacheBuilder, item.CacheDiffID)
		if err != nil {
			return operation.State{}, err
		}
		if operation.Digest(operation.Encode(record)) != item.Proof {
			return operation.State{}, fmt.Errorf("%w: GC frozen cache ownership changed", errs.ErrManualRecovery)
		}
		request = ImageCleanupRequest{
			CacheBuilder: item.CacheBuilder,
			CacheDiffID:  item.CacheDiffID,
		}
	case "image_work":
		source, err := loadImageImport(ctx, i.database, item.ID)
		if err != nil {
			return operation.State{}, err
		}
		if source.Hash != item.Proof {
			return operation.State{}, fmt.Errorf("%w: GC frozen import ownership changed", errs.ErrManualRecovery)
		}
	default:
		return operation.State{}, fmt.Errorf("%w: unknown image collection kind", errs.ErrInvalid)
	}
	return i.AdmitCleanup(ctx, principal, key, request)
}
