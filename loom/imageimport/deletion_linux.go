package imageimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func InspectWorkDeletion(ctx context.Context, state, operation string, plan ImageImportPlan, boundary ImageImportBoundary, published bool) (loom.DeletionIntent, error) {
	var empty loom.DeletionIntent
	if err := VerifyImageImportBoundary(state, operation, boundary, false); err != nil {
		return empty, err
	}
	directories := map[string]bool{".": true, "input": true, "input/blobs": true, "input/blobs/sha256": true}
	fixed := map[string]loom.FileIdentity{".": boundary.Work}
	files := map[string]os.FileMode{}
	if plan.Options.Materialization == host.MergedOCIEROFSLayout {
		files["input/oci-layout"] = 0
		files["input/index.json"] = 0
	}
	for _, digest := range []string{plan.Options.Digest, plan.ConfigDigest} {
		name, err := host.SHAName(digest)
		if err != nil {
			return empty, err
		}
		files[filepath.Join("input", "blobs", "sha256", name)] = 0
	}
	for _, layer := range plan.Layers {
		files[filepath.Join("input", "blobs", "sha256", layer.Digest[7:])] = 0
	}
	if !published {
		directories["output"] = true
		fixed["output"] = boundary.Output
		names := []string{"kernel", "initramfs", "fsmeta.erofs", "fsmeta.map.json", "image.json"}
		if plan.Options.Materialization == host.MergedOCIEROFSLayout {
			names = []string{"kernel", "initramfs", "root.erofs", "image.json"}
		}
		for _, name := range names {
			files[filepath.Join("output", name)] = 0
		}
	}
	return loom.InspectOwnedArtifactTopology(ctx, state, boundary.Path, directories, files, fixed)
}

func InspectUnpublishedImageDeletion(ctx context.Context, state, path string, boundary ImageImportBoundary, proof ImageImportProof) (loom.DeletionIntent, error) {
	var empty loom.DeletionIntent
	if path != filepath.Join(state, "images", proof.Manifest.Digest[7:]) {
		return empty, fmt.Errorf("unpublished image deletion path changed")
	}
	if err := VerifyImportedImage(ctx, state, path, boundary, proof); err != nil {
		return empty, err
	}
	files := map[string]os.FileMode{"image.json": 0}
	for name := range proof.Manifest.Files {
		files[name] = 0
	}
	return loom.InspectOwnedArtifactTopology(ctx, state, path, map[string]bool{".": true}, files, map[string]loom.FileIdentity{".": boundary.Output})
}

func VerifyCacheWitness(ctx context.Context, state string, witness ImageCacheWitness) error {
	cache, err := host.LoadLayerCache(state, witness.Cache.Builder, witness.Cache.DiffID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(cache, witness.Cache) {
		return fmt.Errorf("cache receipt changed")
	}
	expected := filepath.Join(state, "layer-cache", cache.Builder[7:], cache.DiffID[7:])
	if witness.Path != expected || witness.PhysicalBytes <= 0 {
		return fmt.Errorf("cache ownership path or charge changed")
	}
	var bytes int64
	for path, identity := range map[string]loom.FileIdentity{expected: witness.Directory, filepath.Join(expected, "layer.erofs"): witness.Data, filepath.Join(expected, "layer.json"): witness.Receipt} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if loom.IdentityOf(info) != identity {
			return fmt.Errorf("cache ownership inode changed")
		}
		if info.Mode().IsRegular() {
			bytes += info.Size()
		}
	}
	if bytes != witness.PhysicalBytes {
		return fmt.Errorf("cache physical size changed")
	}
	hash, err := loom.DigestFileContext(ctx, filepath.Join(expected, "layer.json"))
	if err != nil {
		return err
	}
	if hash != witness.ReceiptSHA256 {
		return fmt.Errorf("cache receipt bytes changed")
	}
	return nil
}

func InspectCacheDeletion(ctx context.Context, state string, witness ImageCacheWitness) (loom.DeletionIntent, error) {
	var empty loom.DeletionIntent
	if err := VerifyCacheWitness(ctx, state, witness); err != nil {
		return empty, err
	}
	return loom.InspectOwnedArtifactTopology(ctx, state, witness.Path, map[string]bool{".": true}, map[string]os.FileMode{"layer.erofs": 0, "layer.json": 0}, map[string]loom.FileIdentity{".": witness.Directory, "layer.erofs": witness.Data, "layer.json": witness.Receipt})
}
