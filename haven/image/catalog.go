package image

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/store"
)

// LoadManifest reads the immutable publication in the caller's snapshot and
// verifies that its embedded identity matches the requested digest.
func LoadManifest(ctx context.Context, query store.DBTX, digest string) (host.ImageManifest, error) {
	var manifest host.ImageManifest
	body, err := dbgen.New(query).GetManifestBody(ctx, digest)
	if err != nil {
		return manifest, err
	}
	if err = json.Unmarshal(body, &manifest); err != nil {
		return manifest, err
	}
	if manifest.Digest != digest {
		return manifest, fmt.Errorf("image catalog identity inconsistent")
	}
	return manifest, nil
}

// PublishedDigests enumerates the roots retained by immutable publications.
func PublishedDigests(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListManifestDigests(ctx)
}

func loadManifests(ctx context.Context, query store.DBTX) ([]host.ImageManifest, error) {
	bodies, err := dbgen.New(query).ListManifestBodies(ctx)
	if err != nil {
		return nil, err
	}
	var manifests []host.ImageManifest
	for _, body := range bodies {
		var manifest host.ImageManifest
		if err = json.Unmarshal(body, &manifest); err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

// LoadCacheRecord decodes the returned witness and original funding identity.
// Import history and the independent physical charge are checked separately.
func LoadCacheRecord(ctx context.Context, query store.DBTX, key CacheKey) (Cache, error) {
	var cache Cache
	body, err := dbgen.New(query).GetCacheBody(ctx, dbgen.GetCacheBodyParams{
		Builder: key.Builder,
		DiffID:  key.DiffID,
	})
	if err != nil {
		return cache, err
	}
	err = json.Unmarshal(body, &cache)
	return cache, err
}

func fundedCaches(ctx context.Context, query store.DBTX, operationID string) ([]Cache, error) {
	bodies, err := dbgen.New(query).ListFundedCacheBodies(ctx, operationID)
	if err != nil {
		return nil, err
	}
	var caches []Cache
	for _, body := range bodies {
		var cache Cache
		if err = json.Unmarshal(body, &cache); err != nil {
			return nil, err
		}
		caches = append(caches, cache)
	}
	return caches, nil
}

// Cache preserves the returned physical witness and its original funding operation.
type Cache struct {
	Witness         imageimport.ImageCacheWitness `json:"witness"`
	SourceOperation string                        `json:"source_operation"`
}

type CacheKey struct {
	Builder string
	DiffID  string
}

// VerifyFiles binds the immutable catalog manifest to the sealed on-disk image.
func VerifyFiles(manifest host.ImageManifest, directory string) error {
	var disk host.ImageManifest
	if err := loom.ReadJSON(filepath.Join(directory, "image.json"), &disk); err != nil {
		return err
	}
	if !reflect.DeepEqual(disk, manifest) {
		return fmt.Errorf("image file manifest differs from frozen catalog")
	}
	return manifest.Verify(directory, manifest.Digest)
}
