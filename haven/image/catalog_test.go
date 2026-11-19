package image_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/woxQAQ/haven/internal/testfixture"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/tests/fixtures"
)

func TestActualLayeredManifestVerification(t *testing.T) {
	t.Parallel()
	e, _, _, original := fixtures.ImportFixture(t)
	dir := filepath.Join(e.Opts.StateDir, "images", original.Digest[7:])
	for name, mutate := range map[string]func(*host.ImageManifest){
		"format": func(m *host.ImageManifest) {
			m.Format = 1
		},
		"layout": func(m *host.ImageManifest) {
			m.Layout = "merged-erofs-v1"
		},
		"builder": func(m *host.ImageManifest) {
			m.Builder = "sha256:" + strings.Repeat("0", 64)
		},
		"missing artifact": func(m *host.ImageManifest) {
			m.Artifacts = m.Artifacts[:2]
		},
		"wrong order": func(m *host.ImageManifest) {
			m.Artifacts[1], m.Artifacts[2] = m.Artifacts[2], m.Artifacts[1]
		},
		"wrong serial": func(m *host.ImageManifest) {
			m.Artifacts[1].Serial = "sandbox-data-0001"
		},
		"wrong hash": func(m *host.ImageManifest) {
			m.Artifacts[1].SHA256 = "sha256:" + strings.Repeat("0", 64)
		},
		"extra file": func(m *host.ImageManifest) {
			m.Files["ignored"] = m.Files["kernel"]
		},
		"wrong receipt": func(m *host.ImageManifest) {
			m.Artifacts[1].CacheSHA256 = m.Files["kernel"]
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := cloneImageManifest(t, original)
			mutate(&m)
			if err := m.Verify(dir, original.Digest); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
	m := cloneImageManifest(t, original)
	m.DiffIDs[0], m.DiffIDs[1] = m.DiffIDs[1], m.DiffIDs[0]
	m.Layers[0], m.Layers[1] = m.Layers[1], m.Layers[0]
	for i := range 2 {
		a := original.Artifacts[2-i]
		m.Artifacts[i+1] = host.DataArtifact(host.LayerArtifactIdentity{
			Builder:     m.Builder,
			DiffID:      m.DiffIDs[i],
			SHA256:      a.SHA256,
			CacheSHA256: a.CacheSHA256,
		}, i)
	}
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("manifest reordered without rebuilding device_table accepted")
	}
	meta := filepath.Join(dir, "fsmeta.erofs")
	if err := os.Chmod(meta, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(meta, make([]byte, 4096), 0o444); err != nil {
		t.Fatal(err)
	}
	os.Chmod(meta, 0o444)
	m = cloneImageManifest(t, original)
	m.Files["fsmeta.erofs"], _ = loom.DigestFile(meta)
	m.Artifacts[0].SHA256 = m.Files["fsmeta.erofs"]
	if err := m.Verify(dir, original.Digest); err == nil {
		t.Fatal("meaningless fsmeta accepted with a matching content hash")
	}
}

func TestActualMissingAndUnsafeArtifacts(t *testing.T) {
	t.Parallel()
	e, layout, layers, m := fixtures.ImportFixture(t)
	dir := filepath.Join(e.Opts.StateDir, "images", m.Digest[7:])
	for _, rel := range []string{
		m.Artifacts[0].Path,
		m.Artifacts[1].Path,
		filepath.Join(filepath.Dir(m.Artifacts[1].Path), "layer.json"),
	} {
		file := filepath.Join(e.Opts.StateDir, rel)
		moved := file + ".saved"
		if err := os.Rename(file, moved); err != nil {
			t.Fatal(err)
		}
		if err := m.Verify(dir, m.Digest); err == nil {
			t.Fatalf("missing artifact accepted: %s", rel)
		}
		if err := os.Symlink(moved, file); err != nil {
			t.Fatal(err)
		}
		if err := m.Verify(dir, m.Digest); err == nil {
			t.Fatalf("symlink artifact accepted: %s", rel)
		}
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(moved, file); err != nil {
			t.Fatal(err)
		}
	}
	cacheDir := filepath.Dir(filepath.Join(e.Opts.StateDir, m.Artifacts[1].Path))
	if err := os.Rename(cacheDir, cacheDir+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(cacheDir+".saved", cacheDir); err != nil {
		t.Fatal(err)
	}
	if err := m.Verify(dir, m.Digest); err == nil {
		t.Fatal("cache directory symlink accepted")
	}
	os.Remove(cacheDir)
	if err := os.Rename(cacheDir+".saved", cacheDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cacheDir, "layer.json")); err != nil {
		t.Fatal(err)
	}
	image := fixtures.PutOCIManifest(t, layout, "missing receipt", layers)
	if _, err := e.ImportOCI(
		context.Background(),
		imageimport.ImageImportOptions{
			Layout:   layout,
			Digest:   image,
			MaxBytes: 4 << 20,
			MaxFiles: 1000,
		},
	); err == nil {
		t.Fatal("existing cache with missing receipt treated as a cache miss")
	}
}

func TestActualLegacyNativeLayeredCatalogStillVerifies(t *testing.T) {
	t.Parallel()
	fixtures.RequireEROFS(t)
	ctx := context.Background()
	tools, err := host.ImageTools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delete(tools, "go-erofs")
	builder := host.ImageBuilder(tools)
	layout := t.TempDir()
	layer := fixtures.PutOCILayer(t, layout, fixtures.BootLayerEntries(), false)
	image := fixtures.PutOCIManifest(t, layout, "legacy-native", []fixtures.OCILayerFixture{
		layer,
	})
	e := fixtures.NewNativeImageFixture(t)
	defer e.Close()
	input := filepath.Join(layout, "blobs", "sha256", layer.Descriptor.Digest.Encoded())
	n, count, err := host.ValidateLayer(host.LayerInput{
		Path:      input,
		MediaType: layer.Descriptor.MediaType,
		DiffID:    layer.Diff.String(),
	}, host.LayerLimits{
		MaxBytes: 4 << 20,
		MaxFiles: 1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	cache, err := host.BuildLayerCache(ctx, e.Opts.StateDir, host.LayerCacheRequest{
		Input: host.LayerInput{
			Path:      input,
			MediaType: layer.Descriptor.MediaType,
			DiffID:    layer.Diff.String(),
		},
		Builder: builder,
		Bytes:   n,
		Entries: count,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := cache.Artifact(e.Opts.StateDir, 0)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(e.Opts.StateDir, "images", image[7:])
	if err = os.MkdirAll(out, 0o700); err != nil {
		t.Fatal(err)
	}
	if err = host.MaterializeFsmeta(ctx, host.FsmetaLayers{
		StateRoot: e.Opts.StateDir,
		Output:    out,
		Artifacts: []host.ImageArtifact{
			artifact,
		},
	}, image); err != nil {
		t.Fatal(err)
	}
	hashes, err := loom.HashTree(out)
	if err != nil {
		t.Fatal(err)
	}
	meta := host.ImageArtifact{
		Role:      "fsmeta",
		Path:      filepath.Join("images", image[7:], "fsmeta.erofs"),
		SHA256:    hashes["fsmeta.erofs"],
		GuestPath: "/sandbox/ro/fsmeta.erofs",
		Serial:    "sandbox-root",
	}
	m := host.ImageManifest{
		Format:       2,
		Layout:       host.LayeredEROFSLayout,
		Digest:       image,
		Architecture: runtime.GOARCH,
		Layers: []string{
			layer.Descriptor.Digest.String(),
		},
		DiffIDs: []string{
			layer.Diff.String(),
		},
		Tools:   tools,
		Files:   hashes,
		Builder: builder,
		Artifacts: []host.ImageArtifact{
			meta,
			artifact,
		},
	}
	if err = loom.WriteJSON(filepath.Join(out, "image.json"), m); err != nil {
		t.Fatal(err)
	}
	if err = loom.SealTree(out); err != nil {
		t.Fatal(err)
	}
	if _, err = e.DB.Exec("INSERT INTO images VALUES(?,?)", image, testfixture.Encode(m)); err != nil {
		t.Fatal(err)
	}
	if err = m.Verify(out, image); err != nil {
		t.Fatal(err)
	}
	reimported, err := e.ImportOCI(ctx, imageimport.ImageImportOptions{
		Layout: layout,
		Digest: image,
	})
	if err != nil || !reflect.DeepEqual(m, reimported) {
		t.Fatalf("old catalog identity changed: %v", err)
	}
	root := fixtures.ExtractManifest(t, e, reimported)
	if got, err := os.ReadFile(filepath.Join(root, "boot/vmlinux")); err != nil || string(got) != "kernel-fixture" {
		t.Fatalf("old catalog lost native contents: %v", err)
	}
}

func cloneImageManifest(t *testing.T, m host.ImageManifest) host.ImageManifest {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var cloned host.ImageManifest
	if err = json.Unmarshal(b, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}
