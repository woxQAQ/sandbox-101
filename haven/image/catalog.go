package image

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

// LoadManifest reads the immutable publication in the caller's snapshot and
// verifies that its embedded identity matches the requested digest.
func LoadManifest(ctx context.Context, query store.DBTX, digest string) (host.ImageManifest, error) {
	var manifest host.ImageManifest
	err := store.ReadSnapshot(ctx, query, func(tx *sql.Tx) error {
		q := dbgen.New(tx)
		hash, err := q.PublishedManifestHash(ctx, digest)
		if err != nil {
			return err
		}
		if err = readManifest(ctx, q, hash, &manifest); err != nil {
			return err
		}
		if manifest.Digest != digest {
			return fmt.Errorf("image publication identity changed")
		}
		return nil
	})
	return manifest, err
}

func readManifest(ctx context.Context, q *dbgen.Queries, hash string, manifest *host.ImageManifest) error {
	row, err := q.GetManifest(ctx, hash)
	if err != nil {
		return err
	}
	*manifest = host.ImageManifest{
		Digest:       row.Digest,
		Format:       int(row.Format),
		Layout:       row.Layout,
		Architecture: row.Architecture,
		Builder:      row.Builder,
		Files:        map[string]string{},
	}
	layers, err := q.ListImageLayers(ctx, hash)
	if err != nil {
		return err
	}
	if row.LayersPresent {
		manifest.Layers = make([]string, 0, len(layers))
		manifest.DiffIDs = make([]string, 0, len(layers))
	}
	for _, l := range layers {
		manifest.Layers = append(manifest.Layers, l.LayerDigest)
		manifest.DiffIDs = append(manifest.DiffIDs, l.DiffID)
	}
	tools, err := q.ListImageTools(ctx, hash)
	if err != nil {
		return err
	}
	if row.ToolsPresent || len(tools) > 0 {
		manifest.Tools = make(map[string]string, len(tools))
	}
	for _, tool := range tools {
		manifest.Tools[tool.Name] = tool.Version
	}
	files, err := q.ListImageFiles(ctx, hash)
	if err != nil {
		return err
	}
	for _, f := range files {
		manifest.Files[f.Path] = f.Sha256
	}
	artifacts, err := q.ListImageArtifacts(ctx, hash)
	if err != nil {
		return err
	}
	for _, a := range artifacts {
		artifact, err := artifactValue(a)
		if err != nil {
			return err
		}
		manifest.Artifacts = append(manifest.Artifacts, artifact)
	}
	if operation.Digest(operation.Encode(manifest)) != row.ManifestHash {
		return fmt.Errorf("image manifest changed for %s", hash)
	}
	return nil
}

// PublishManifest saves the sealed image's ordered devices, layers, tools and file digests in the caller's transaction.
func PublishManifest(ctx context.Context, tx *sql.Tx, m host.ImageManifest) error {
	q := dbgen.New(tx)
	hash := operation.Digest(operation.Encode(m))
	if err := retainManifest(ctx, q, hash, m); err != nil {
		return err
	}
	return store.RequireOne(q.PublishImage(ctx, dbgen.PublishImageParams{
		Digest:       m.Digest,
		ManifestHash: hash,
	}))
}

func retainManifest(ctx context.Context, q *dbgen.Queries, hash string, m host.ImageManifest) error {
	if _, err := q.GetManifest(ctx, hash); err == nil {
		var retained host.ImageManifest
		return readManifest(ctx, q, hash, &retained)
	} else if err != sql.ErrNoRows {
		return err
	}
	if len(m.Layers) != len(m.DiffIDs) || (m.Layers == nil) != (m.DiffIDs == nil) || m.Files == nil {
		return fmt.Errorf("image manifest lacks paired layer identities or file inventory")
	}
	if err := store.RequireOne(q.InsertManifest(ctx, dbgen.InsertManifestParams{
		Digest:        m.Digest,
		Format:        int64(m.Format),
		Layout:        m.Layout,
		Architecture:  m.Architecture,
		Builder:       m.Builder,
		ManifestHash:  hash,
		LayersPresent: m.Layers != nil,
		ToolsPresent:  m.Tools != nil,
	})); err != nil {
		return err
	}
	for index, digest := range m.Layers {
		if err := store.RequireOne(q.InsertLayer(ctx, dbgen.InsertLayerParams{
			ManifestHash: hash,
			Position:     int64(index),
			LayerDigest:  digest,
			DiffID:       m.DiffIDs[index],
		})); err != nil {
			return err
		}
	}
	for name, version := range m.Tools {
		if err := store.RequireOne(q.InsertTool(ctx, dbgen.InsertToolParams{
			ManifestHash: hash,
			Name:         name,
			Version:      version,
		})); err != nil {
			return err
		}
	}
	for path, checksum := range m.Files {
		if err := store.RequireOne(q.InsertFile(ctx, dbgen.InsertFileParams{
			ManifestHash: hash,
			Path:         path,
			Sha256:       checksum,
		})); err != nil {
			return err
		}
	}
	for index, a := range m.Artifacts {
		artifactHash, err := retainArtifact(ctx, q, a)
		if err != nil {
			return err
		}
		if err := store.RequireOne(q.InsertArtifact(ctx, dbgen.InsertArtifactParams{
			ManifestHash: hash,
			Position:     int64(index),
			ArtifactHash: artifactHash,
		})); err != nil {
			return err
		}
	}
	return nil
}

// PublishedDigests enumerates the roots retained by immutable publications.
func PublishedDigests(ctx context.Context, query store.DBTX) ([]string, error) {
	return dbgen.New(query).ListManifestDigests(ctx)
}

func loadManifests(ctx context.Context, query store.DBTX) ([]host.ImageManifest, error) {
	digests, err := PublishedDigests(ctx, query)
	if err != nil {
		return nil, err
	}
	var manifests []host.ImageManifest
	for _, digest := range digests {
		manifest, err := LoadManifest(ctx, query, digest)
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

// LoadCacheRecord decodes the returned witness and original funding identity.
// Import history and the independent physical charge are checked separately.
func LoadCacheRecord(ctx context.Context, query store.DBTX, key CacheKey) (Cache, error) {
	row, err := dbgen.New(query).GetCache(ctx, dbgen.GetCacheParams{
		Builder: key.Builder,
		DiffID:  key.DiffID,
	})
	if err != nil {
		return Cache{}, err
	}
	witness, err := cacheWitness(row.ImageCacheWitness)
	return Cache{
		SourceOperation: row.SourceOperationID,
		Witness:         witness,
	}, err
}

func fundedCaches(ctx context.Context, query store.DBTX, operationID string) ([]Cache, error) {
	rows, err := dbgen.New(query).ListFundedCaches(ctx, operationID)
	if err != nil {
		return nil, err
	}
	var caches []Cache
	for _, row := range rows {
		witness, err := cacheWitness(row.ImageCacheWitness)
		if err != nil {
			return nil, err
		}
		caches = append(caches, Cache{
			SourceOperation: row.SourceOperationID,
			Witness:         witness,
		})
	}
	return caches, nil
}

func cacheWitness(row dbgen.ImageCacheWitness) (imageimport.ImageCacheWitness, error) {
	w := imageimport.ImageCacheWitness{
		PhysicalBytes: row.PhysicalBytes,
		Path:          row.Path,
		ReceiptSHA256: row.ReceiptSha256,
		Directory: loom.FileIdentity{
			Device: uint64(row.DirectoryDevice),
			Inode:  uint64(row.DirectoryInode),
			Mode:   uint32(row.DirectoryMode),
		},
		Data: loom.FileIdentity{
			Device: uint64(row.DataDevice),
			Inode:  uint64(row.DataInode),
			Mode:   uint32(row.DataMode),
		},
		Receipt: loom.FileIdentity{
			Device: uint64(row.ReceiptDevice),
			Inode:  uint64(row.ReceiptInode),
			Mode:   uint32(row.ReceiptMode),
		},
		Cache: host.LayerCache{
			Builder: row.Builder,
			DiffID:  row.DiffID,
			Format:  int(row.Format),
			Profile: row.Profile,
			SHA256:  row.Sha256,
			Bytes:   row.TarBytes,
			Entries: row.Entries,
		},
	}
	if operation.Digest(operation.Encode(w)) != row.WitnessHash {
		return w, fmt.Errorf("cache witness changed")
	}
	return w, nil
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
