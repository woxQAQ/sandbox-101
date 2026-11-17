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

// inputDeletionTopology fixes the original staged inputs. Output custody is
// selected by separate entrypoints after the caller verifies publication state.
func (work ImportWork) inputDeletionTopology() (loom.ArtifactTopology, error) {
	var empty loom.ArtifactTopology
	state, operation, plan, boundary := work.StateRoot, work.OperationID, work.Plan, work.Boundary
	if err := VerifyImageImportBoundary(state, operation, boundary, false); err != nil {
		return empty, err
	}
	topology := loom.ArtifactTopology{
		Directories: map[string]bool{
			".":                  true,
			"input":              true,
			"input/blobs":        true,
			"input/blobs/sha256": true,
		},
		Files: map[string]os.FileMode{},
		Fixed: map[string]loom.FileIdentity{
			".": boundary.Work,
		},
	}
	if plan.Options.Materialization == host.MergedOCIEROFSLayout {
		topology.Files["input/oci-layout"] = 0
		topology.Files["input/index.json"] = 0
	}
	for _, digest := range []string{
		plan.Options.Digest,
		plan.ConfigDigest,
	} {
		name, err := host.SHAName(digest)
		if err != nil {
			return empty, err
		}
		topology.Files[filepath.Join("input", "blobs", "sha256", name)] = 0
	}
	for _, layer := range plan.Layers {
		topology.Files[filepath.Join("input", "blobs", "sha256", layer.Digest[7:])] = 0
	}
	return topology, nil
}

func (work ImportWork) InspectPublishedWorkDeletion(ctx context.Context) (loom.DeletionIntent, error) {
	topology, err := work.inputDeletionTopology()
	if err != nil {
		return loom.DeletionIntent{}, err
	}
	return loom.InspectOwnedArtifactTopology(ctx, work.StateRoot, work.Boundary.Path, topology)
}

func (work ImportWork) InspectUnpublishedWorkDeletion(ctx context.Context) (loom.DeletionIntent, error) {
	topology, err := work.inputDeletionTopology()
	if err != nil {
		return loom.DeletionIntent{}, err
	}
	topology.Directories["output"] = true
	topology.Fixed["output"] = work.Boundary.Output
	names := []string{
		"kernel",
		"initramfs",
		"fsmeta.erofs",
		"fsmeta.map.json",
		"image.json",
	}
	if work.Plan.Options.Materialization == host.MergedOCIEROFSLayout {
		names = []string{
			"kernel",
			"initramfs",
			"root.erofs",
			"image.json",
		}
	}
	for _, name := range names {
		topology.Files[filepath.Join("output", name)] = 0
	}
	return loom.InspectOwnedArtifactTopology(ctx, work.StateRoot, work.Boundary.Path, topology)
}

func (proof ImageImportProof) InspectUnpublishedDeletion(ctx context.Context, state, path string, boundary ImageImportBoundary) (loom.DeletionIntent, error) {
	var empty loom.DeletionIntent
	if path != filepath.Join(state, "images", proof.Manifest.Digest[7:]) {
		return empty, fmt.Errorf("unpublished image deletion path changed")
	}
	if err := proof.Verify(ctx, state, path, boundary); err != nil {
		return empty, err
	}
	files := map[string]os.FileMode{
		"image.json": 0,
	}
	for name := range proof.Manifest.Files {
		files[name] = 0
	}
	return loom.InspectOwnedArtifactTopology(ctx, state, path, loom.ArtifactTopology{
		Directories: map[string]bool{
			".": true,
		},
		Files: files,
		Fixed: map[string]loom.FileIdentity{
			".": boundary.Output,
		},
	})
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
	for path, identity := range map[string]loom.FileIdentity{
		expected:                               witness.Directory,
		filepath.Join(expected, "layer.erofs"): witness.Data,
		filepath.Join(expected, "layer.json"):  witness.Receipt,
	} {
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
	return loom.InspectOwnedArtifactTopology(ctx, state, witness.Path, loom.ArtifactTopology{
		Directories: map[string]bool{
			".": true,
		},
		Files: map[string]os.FileMode{
			"layer.erofs": 0,
			"layer.json":  0,
		},
		Fixed: map[string]loom.FileIdentity{
			".":           witness.Directory,
			"layer.erofs": witness.Data,
			"layer.json":  witness.Receipt,
		},
	})
}
