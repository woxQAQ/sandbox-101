package imageimport

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

type (
	ImageLayerProof struct {
		Bytes    int64               `json:"bytes"`
		Entries  int64               `json:"entries"`
		Cache    *ImageCacheWitness  `json:"cache,omitempty"`
		Artifact *host.ImageArtifact `json:"artifact,omitempty"`
	}
	ImageImportProof struct {
		Manifest       host.ImageManifest `json:"manifest"`
		ManifestSHA256 string             `json:"manifest_sha256"`
		Synced         bool               `json:"synced"`
	}
)

func StageImageImport(ctx context.Context, state, operation string, plan ImageImportPlan, boundary ImageImportBoundary) error {
	if err := VerifyImageImportBoundary(state, operation, boundary, true); err != nil {
		return err
	}
	stage := func(digest string, limit, exact int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		source, err := host.Blob(plan.Options.Layout, digest)
		if err != nil {
			return err
		}
		info, err := os.Lstat(source)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > limit || (exact >= 0 && info.Size() != exact) {
			return errors.New("OCI blob size or type changed")
		}
		name, err := host.SHAName(digest)
		if err != nil {
			return err
		}
		target := filepath.Join(boundary.Path, "input", "blobs", "sha256", name)
		if _, err = os.Lstat(target); os.IsNotExist(err) {
			if err = loom.CopyFile(source, target, 0o400); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		actual, err := loom.DigestFileContext(ctx, target)
		if err != nil {
			return err
		}
		if actual != digest {
			return errors.New("staged OCI digest mismatch")
		}
		return nil
	}
	if err := stage(plan.Options.Digest, 16<<20, -1); err != nil {
		return err
	}
	if err := stage(plan.ConfigDigest, 16<<20, plan.ConfigSize); err != nil {
		return err
	}
	for _, layer := range plan.Layers {
		if plan.Options.Materialization == host.MergedOCIEROFSLayout || layer.Cached == nil {
			if err := stage(layer.Digest, plan.Options.MaxBytes*2, layer.Size); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

func MaterializeImageLayer(ctx context.Context, state, operation string, plan ImageImportPlan, boundary ImageImportBoundary, index int, bytesLeft, filesLeft int64) (ImageLayerProof, error) {
	var proof ImageLayerProof
	if err := VerifyImageImportBoundary(state, operation, boundary, true); err != nil {
		return proof, err
	}
	if index < 0 || index >= len(plan.Layers) {
		return proof, errors.New("invalid layer index")
	}
	layer := plan.Layers[index]
	if plan.Options.Materialization == host.MergedOCIEROFSLayout {
		file := filepath.Join(boundary.Path, "input", "blobs", "sha256", layer.Digest[7:])
		n, c, err := host.ValidateLayerFor(file, layer.MediaType, layer.DiffID, bytesLeft, filesLeft, true)
		if err != nil {
			return proof, err
		}
		return ImageLayerProof{
			Bytes:   n,
			Entries: c,
		}, nil
	}
	var cache host.LayerCache
	if layer.Cached != nil {
		current, err := host.LoadLayerCache(state, plan.Builder, layer.DiffID)
		if err != nil {
			return proof, err
		}
		if !reflect.DeepEqual(current, *layer.Cached) {
			return proof, errors.New("frozen cache receipt changed")
		}
		cache = current
	} else {
		file := filepath.Join(boundary.Path, "input", "blobs", "sha256", layer.Digest[7:])
		n, c, err := host.ValidateLayer(file, layer.MediaType, layer.DiffID, bytesLeft, filesLeft)
		if err != nil {
			return proof, err
		}
		cache, err = host.BuildSharedLayerCache(ctx, state, file, layer.MediaType, plan.Builder, layer.DiffID, n, c)
		if err != nil {
			return proof, err
		}
	}
	if cache.Bytes > bytesLeft || cache.Entries > filesLeft {
		return proof, errors.New("cached OCI layer exceeds import bounds")
	}
	artifact, err := cache.Artifact(state, index)
	if err != nil {
		return proof, err
	}
	directory := filepath.Join(state, filepath.Dir(artifact.Path))
	info, err := os.Lstat(directory)
	if err != nil {
		return proof, err
	}
	witness := ImageCacheWitness{
		Path:      directory,
		Directory: loom.IdentityOf(info),
		Cache:     cache,
	}
	info, err = os.Lstat(filepath.Join(directory, "layer.erofs"))
	if err != nil {
		return proof, err
	}
	witness.Data = loom.IdentityOf(info)
	witness.PhysicalBytes = info.Size()
	info, err = os.Lstat(filepath.Join(directory, "layer.json"))
	if err != nil {
		return proof, err
	}
	witness.Receipt = loom.IdentityOf(info)
	witness.PhysicalBytes += info.Size()
	witness.ReceiptSHA256, err = loom.DigestFileContext(ctx, filepath.Join(directory, "layer.json"))
	if err != nil {
		return proof, err
	}
	return ImageLayerProof{
		Bytes:    cache.Bytes,
		Entries:  cache.Entries,
		Cache:    &witness,
		Artifact: &artifact,
	}, nil
}

func BuildImageImportOutput(ctx context.Context, state, operation string, plan ImageImportPlan, boundary ImageImportBoundary, layers []ImageLayerProof) (ImageImportProof, error) {
	var proof ImageImportProof
	if err := VerifyImageImportBoundary(state, operation, boundary, true); err != nil {
		return proof, err
	}
	if len(layers) != len(plan.Layers) {
		return proof, errors.New("image layer proof set incomplete")
	}
	output := filepath.Join(boundary.Path, "output")
	var artifacts []host.ImageArtifact
	for _, layer := range layers {
		if layer.Artifact != nil {
			artifacts = append(artifacts, *layer.Artifact)
		}
	}
	var err error
	if plan.Options.Materialization == host.MergedOCIEROFSLayout {
		err = host.MaterializeMergedOCI(ctx, filepath.Join(boundary.Path, "input"), output, plan.Options.Digest)
	} else {
		err = host.MaterializeSharedFsmeta(ctx, state, output, artifacts, plan.Builder)
	}
	if err != nil {
		return proof, err
	}
	hashes, err := loom.HashTree(output)
	if err != nil {
		return proof, err
	}
	name, err := host.SHAName(plan.Options.Digest)
	if err != nil {
		return proof, err
	}
	manifest := host.ImageManifest{
		Format:       2,
		Layout:       host.LayeredEROFSLayout,
		Digest:       plan.Options.Digest,
		Architecture: runtime.GOARCH,
		Tools:        plan.Tools,
		Builder:      plan.Builder,
		Files:        hashes,
	}
	for _, layer := range plan.Layers {
		manifest.Layers = append(manifest.Layers, layer.Digest)
		manifest.DiffIDs = append(manifest.DiffIDs, layer.DiffID)
	}
	manifest.Artifacts = append([]host.ImageArtifact{
		{
			Role:      "fsmeta",
			Path:      filepath.Join("images", name, "fsmeta.erofs"),
			SHA256:    hashes["fsmeta.erofs"],
			GuestPath: "/sandbox/ro/fsmeta.erofs",
			Serial:    "sandbox-root",
		},
	}, artifacts...)
	if plan.Options.Materialization == host.MergedOCIEROFSLayout {
		manifest.Format = 3
		manifest.Layout = host.MergedOCIEROFSLayout
		manifest.Artifacts = nil
	}
	if err = loom.WriteJSON(filepath.Join(output, "image.json"), manifest); err != nil {
		return proof, err
	}
	if err = loom.SealTree(output); err != nil {
		return proof, err
	}
	if manifest.Layout == host.MergedOCIEROFSLayout {
		err = manifest.VerifyMergedOCIArtifacts(output)
	} else {
		err = manifest.VerifyLayeredArtifacts(state, output)
	}
	if err != nil {
		return proof, err
	}
	if err = loom.SyncTree(output); err != nil {
		return proof, err
	}
	hash, err := loom.DigestFileContext(ctx, filepath.Join(output, "image.json"))
	if err != nil {
		return proof, err
	}
	return ImageImportProof{
		Manifest:       manifest,
		ManifestSHA256: hash,
		Synced:         true,
	}, ctx.Err()
}

func VerifyImportedImage(ctx context.Context, state, path string, boundary ImageImportBoundary, proof ImageImportProof) error {
	if !proof.Synced || proof.ManifestSHA256 == "" || boundary.Output.Inode == 0 {
		return errors.New("image publication lacks fixed synced proof")
	}
	directory, err := loom.OpenCaptureDir(path)
	if err != nil {
		return err
	}
	info, err := directory.Stat()
	_ = directory.Close()
	if err != nil {
		return err
	}
	if loom.IdentityOf(info) != boundary.Output {
		return errors.New("image publication directory replaced")
	}
	hash, err := loom.DigestFileContext(ctx, filepath.Join(path, "image.json"))
	if err != nil {
		return err
	}
	if hash != proof.ManifestSHA256 {
		return errors.New("image publication manifest bytes changed")
	}
	if proof.Manifest.Layout == host.MergedOCIEROFSLayout {
		return proof.Manifest.VerifyMergedOCIArtifacts(path)
	}
	return proof.Manifest.VerifyLayeredArtifacts(state, path)
}

func PublishImportedImage(ctx context.Context, state, operation string, boundary ImageImportBoundary, proof ImageImportProof) error {
	if err := VerifyImageImportBoundary(state, operation, boundary, false); err != nil {
		return err
	}
	name, err := host.SHAName(proof.Manifest.Digest)
	if err != nil {
		return err
	}
	parent := filepath.Join(state, "images")
	if err = os.Mkdir(parent, 0o700); err != nil && !os.IsExist(err) {
		return err
	}
	directory, err := loom.OpenCaptureDir(parent)
	if err != nil {
		return err
	}
	_ = directory.Close()
	target := filepath.Join(parent, name)
	if _, err = os.Lstat(target); err == nil {
		if err = VerifyImportedImage(ctx, state, target, boundary, proof); err != nil {
			return err
		}
		return errors.Join(loom.SyncDir(parent), loom.SyncDir(boundary.Path), ctx.Err())
	} else if !os.IsNotExist(err) {
		return err
	}
	output := filepath.Join(boundary.Path, "output")
	if err = VerifyImportedImage(ctx, state, output, boundary, proof); err != nil {
		return err
	}
	if err = os.Rename(output, target); err != nil {
		return err
	}
	return errors.Join(loom.SyncDir(parent), loom.SyncDir(boundary.Path), ctx.Err())
}
