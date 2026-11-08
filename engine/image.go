package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/sandbox-engine/api"
)

// ImageManifest fixes the materializer identity and the complete ordered set of
// immutable block devices. Native layered rebuild and OCI merged compatibility
// have separate immutable layout and builder identities.
type ImageManifest struct {
	Format       int               `json:"format"`
	Layout       string            `json:"layout"`
	Digest       string            `json:"digest"`
	Architecture string            `json:"architecture"`
	Layers       []string          `json:"layers"`
	DiffIDs      []string          `json:"diff_ids"`
	Tools        map[string]string `json:"tools"`
	Files        map[string]string `json:"files"`
	Builder      string            `json:"builder,omitempty"`
	Artifacts    []ImageArtifact   `json:"artifacts,omitempty"`
}

func (m ImageManifest) Verify(dir, digest string) error {
	if m.Layout == LayeredEROFSLayout {
		return m.verifyLayered(dir, digest)
	}
	if m.Layout == MergedOCIEROFSLayout {
		return m.verifyMergedOCI(dir, digest)
	}
	if m.Format != 1 || m.Layout != "merged-erofs-v1" || m.Digest != digest ||
		m.Architecture != runtime.GOARCH ||
		m.Builder != "" ||
		len(m.Artifacts) != 0 {
		return api.Fail(api.Incompatible, "image format/platform mismatch")
	}
	for _, n := range []string{"kernel", "initramfs", "root.erofs"} {
		if _, ok := m.Files[n]; !ok {
			return api.Fail(api.Incompatible, "image missing %s", n)
		}
	}
	return verifyFiles(dir, m.Files)
}

type ImportOptions struct {
	Layout          string // Source OCI image-layout directory.
	Digest          string
	Materialization string // Empty selects LayeredEROFSLayout; compatibility is explicit.
	MaxBytes        int64
	MaxFiles        int64
}

// ImportOCI is an offline administrative operation: the caller must hold this
// Engine's single-writer lock; the CLI opens it itself, so it cannot race a
// running daemon. Registry download and authentication are intentionally absent.
func (e *Engine) ImportOCI(
	ctx context.Context,
	opts ImportOptions,
) (ImageManifest, error) {
	// Publication and cache collection share a guard for the entire read/build
	// interval, including cache hits before the catalog transaction.
	e.artifacts.Lock()
	defer e.artifacts.Unlock()
	var output ImageManifest
	e.mu.Lock()
	closed := e.closing
	e.mu.Unlock()
	if closed {
		return output, api.Fail(api.Conflict, "engine shutting down")
	}
	if opts.Materialization == "" {
		opts.Materialization = LayeredEROFSLayout
	}
	if opts.Materialization != LayeredEROFSLayout &&
		opts.Materialization != MergedOCIEROFSLayout {
		return output, api.Fail(
			api.Invalid,
			"unknown image materialization %q",
			opts.Materialization,
		)
	}
	merged := opts.Materialization == MergedOCIEROFSLayout
	name, err := shaName(opts.Digest)
	if err != nil {
		return output, err
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 8 << 30
	}
	if opts.MaxFiles == 0 {
		opts.MaxFiles = 1000000
	}
	if opts.MaxBytes <= 0 || opts.MaxBytes > 1<<40 || opts.MaxFiles <= 0 ||
		opts.MaxFiles > 1<<30 {
		return output, api.Fail(api.Invalid, "invalid import bounds")
	}
	dest := filepath.Join(e.opts.StateDir, "images", name)
	var published string
	if err = e.db.QueryRow("SELECT value FROM meta WHERE key=?", "image:"+opts.Digest).
		Scan(&published); err == nil {
		if err = json.Unmarshal([]byte(published), &output); err != nil {
			return output, err
		}
		if output.Layout != opts.Materialization {
			return output, api.Fail(
				api.Conflict,
				"image digest already published with materialization %s",
				output.Layout,
			)
		}
		return output, output.Verify(dest, opts.Digest)
	}
	tools, err := imageTools(ctx)
	if err != nil {
		return output, err
	}
	builder := imageBuilder(tools)
	if merged {
		tools, err = mergedOCITools(ctx, tools)
		if err != nil {
			return output, err
		}
		builder = mergedOCIBuilder(tools)
	}
	source, err := filepath.Abs(opts.Layout)
	if err != nil {
		return output, err
	}
	manifestPath, err := blob(source, opts.Digest)
	if err != nil {
		return output, err
	}
	var manifest v1.Manifest
	if err = readBlobJSON(manifestPath, opts.Digest, &manifest); err != nil {
		return output, err
	}
	if manifest.SchemaVersion != 2 || len(manifest.Layers) == 0 ||
		len(manifest.Layers) > MaxImageDataDevices {
		return output, api.Fail(
			api.Incompatible,
			"image requires 1..%d layer data backends",
			MaxImageDataDevices,
		)
	}
	configPath, err := blob(source, manifest.Config.Digest.String())
	if err != nil {
		return output, err
	}
	configStat, err := os.Stat(configPath)
	if err != nil {
		return output, err
	}
	if manifest.Config.Size < 0 || configStat.Size() != manifest.Config.Size {
		return output, api.Fail(
			api.Invalid,
			"OCI config descriptor size mismatch",
		)
	}
	var config v1.Image
	if err = readBlobJSON(
		configPath,
		manifest.Config.Digest.String(),
		&config,
	); err != nil {
		return output, err
	}
	if config.OS != "linux" || config.Architecture != runtime.GOARCH ||
		config.RootFS.Type != "layers" ||
		len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return output, api.Fail(
			api.Incompatible,
			"OCI platform/rootfs mismatch",
		)
	}
	work := filepath.Join(e.opts.StateDir, "imports", name)
	owner := "import:" + name
	e.admission.Lock()
	tx, err := e.db.Begin()
	if err == nil {
		// Flat per-file block alignment and metadata can exceed tar payload size.
		err = e.reserve(
			tx,
			owner,
			opts.MaxBytes*4+opts.MaxFiles*8192+(32<<20),
			0,
			0,
		)
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}
	e.admission.Unlock()
	if err != nil {
		return output, err
	}
	if err = os.MkdirAll(
		filepath.Join(work, "input", "blobs", "sha256"),
		0o700,
	); err != nil {
		return output, err
	}
	if err = writeJSON(filepath.Join(work, "intent.json"), opts); err != nil {
		return output, err
	}
	input := filepath.Join(work, "input")
	stage := func(digest string, maxSize int64) error {
		src, x := blob(source, digest)
		if x != nil {
			return x
		}
		st, x := os.Stat(src)
		if x != nil {
			return x
		}
		if st.Size() > maxSize {
			return api.Fail(
				api.Exhausted,
				"OCI input blob exceeds staging bound",
			)
		}
		n, _ := shaName(digest)
		dst := filepath.Join(input, "blobs", "sha256", n)
		if _, x = os.Stat(dst); os.IsNotExist(x) {
			if x = copyFile(src, dst, 0o400); x != nil {
				return x
			}
		}
		got, x := digestFile(dst)
		if x != nil {
			return x
		}
		if got != digest {
			return api.Fail(api.Invalid, "staged OCI digest mismatch")
		}
		return nil
	}
	if err = stage(opts.Digest, 16<<20); err != nil {
		return output, err
	}
	if err = stage(manifest.Config.Digest.String(), 16<<20); err != nil {
		return output, err
	}
	var used, files, compressedBytes int64
	var layerDigests, diffs []string
	var artifacts []ImageArtifact
	for i, layer := range manifest.Layers {
		if layer.Size < 0 || !supportedLayerMedia(layer.MediaType) {
			return output, api.Fail(
				api.Incompatible,
				"only uncompressed/gzip OCI layers with nonnegative sizes are supported",
			)
		}
		if layer.Size > opts.MaxBytes*2-compressedBytes {
			return output, api.Fail(
				api.Exhausted,
				"OCI compressed input size exceeds staging bound",
			)
		}
		compressedBytes += layer.Size
		diff := config.RootFS.DiffIDs[i].String()
		if _, err = shaName(layer.Digest.String()); err != nil {
			return output, err
		}
		if _, err = shaName(diff); err != nil {
			return output, err
		}
		if merged {
			if err = stage(layer.Digest.String(), opts.MaxBytes*2); err != nil {
				return output, err
			}
			file := filepath.Join(
				input,
				"blobs",
				"sha256",
				layer.Digest.Encoded(),
			)
			st, x := os.Stat(file)
			if x != nil {
				return output, x
			}
			if st.Size() != layer.Size {
				return output, api.Fail(
					api.Invalid,
					"OCI layer descriptor size mismatch",
				)
			}
			n, c, x := validateLayerFor(
				file,
				layer.MediaType,
				diff,
				opts.MaxBytes-used,
				opts.MaxFiles-files,
				true,
			)
			if x != nil {
				return output, x
			}
			used += n
			files += c
			layerDigests = append(layerDigests, layer.Digest.String())
			diffs = append(diffs, diff)
			continue
		}
		// A verified cache receipt proves the input diff_id was preflighted.
		// Rebuilding fsmeta from published layers needs no source layer tar.
		cached, x := loadLayerCache(e.opts.StateDir, builder, diff)
		if os.IsNotExist(x) {
			if err = stage(layer.Digest.String(), opts.MaxBytes*2); err != nil {
				return output, err
			}
			file := filepath.Join(
				input,
				"blobs",
				"sha256",
				layer.Digest.Encoded(),
			)
			st, statErr := os.Stat(file)
			if statErr != nil {
				return output, statErr
			}
			if st.Size() != layer.Size {
				return output, api.Fail(
					api.Invalid,
					"OCI layer descriptor size mismatch",
				)
			}
			var n, c int64
			n, c, x = validateLayer(
				file,
				layer.MediaType,
				diff,
				opts.MaxBytes-used,
				opts.MaxFiles-files,
			)
			if x == nil {
				cached, x = buildSharedLayerCache(
					ctx,
					e.opts.StateDir,
					file,
					layer.MediaType,
					builder,
					diff,
					n,
					c,
				)
			}
		}
		if x != nil {
			return output, x
		}
		if cached.Bytes > opts.MaxBytes-used ||
			cached.Entries > opts.MaxFiles-files {
			return output, api.Fail(
				api.Exhausted,
				"cached OCI layer exceeds import bounds",
			)
		}
		used += cached.Bytes
		files += cached.Entries
		layerDigests = append(layerDigests, layer.Digest.String())
		diffs = append(diffs, diff)
		a, x := cached.artifact(e.opts.StateDir, i)
		if x != nil {
			return output, x
		}
		artifacts = append(artifacts, a)
	}
	out := filepath.Join(work, "output")
	if err = os.RemoveAll(out); err != nil {
		return output, err
	}
	if err = os.Mkdir(out, 0o700); err != nil {
		return output, err
	}
	if merged {
		err = materializeMergedOCI(
			ctx,
			input,
			out,
			opts.Digest,
			opts.MaxBytes+opts.MaxFiles*4096,
		)
	} else {
		err = materializeSharedFsmeta(
			ctx,
			e.opts.StateDir,
			out,
			artifacts,
			builder,
		)
	}
	if err != nil {
		return output, err
	}
	hashes, err := hashTree(out)
	if err != nil {
		return output, err
	}
	meta := ImageArtifact{
		Role:      "fsmeta",
		Path:      filepath.Join("images", name, "fsmeta.erofs"),
		SHA256:    hashes["fsmeta.erofs"],
		GuestPath: "/sandbox/ro/fsmeta.erofs",
		Serial:    "sandbox-root",
	}
	output = ImageManifest{
		Format:       2,
		Layout:       LayeredEROFSLayout,
		Digest:       opts.Digest,
		Architecture: runtime.GOARCH,
		Layers:       layerDigests,
		DiffIDs:      diffs,
		Tools:        tools,
		Files:        hashes,
		Builder:      builder,
		Artifacts:    append([]ImageArtifact{meta}, artifacts...),
	}
	if merged {
		output.Format, output.Layout, output.Artifacts = 3, MergedOCIEROFSLayout, nil
	}
	if err = writeJSON(filepath.Join(out, "image.json"), output); err != nil {
		return output, err
	}
	if err = sealTree(out); err != nil {
		return output, err
	}
	if merged {
		err = output.verifyMergedOCIArtifacts(out)
	} else {
		err = output.verifyLayeredArtifacts(e.opts.StateDir, out)
	}
	if err != nil {
		return output, err
	}
	if err = syncTree(out); err != nil {
		return output, err
	}
	if _, x := os.Lstat(dest); x == nil {
		var previous ImageManifest
		if err = readJSON(
			filepath.Join(dest, "image.json"),
			&previous,
		); err != nil {
			return output, api.Fail(
				api.ManualRecovery,
				"unpublished image destination cannot be verified",
			)
		}
		if err = previous.Verify(dest, opts.Digest); err != nil {
			return output, err
		}
		if previous.Layout != opts.Materialization {
			return output, api.Fail(
				api.Conflict,
				"image digest already materialized with layout %s",
				previous.Layout,
			)
		}
		output = previous
	} else if !os.IsNotExist(x) {
		return output, x
	} else {
		if err = os.Rename(out, dest); err != nil {
			return output, err
		}
		if err = syncDir(filepath.Dir(dest)); err != nil {
			return output, err
		}
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	tx, err = e.db.Begin()
	if err != nil {
		return output, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.Exec(
		"INSERT OR REPLACE INTO meta VALUES(?,?)",
		"image:"+opts.Digest,
		string(encode(output)),
	); err != nil {
		return output, err
	}
	if _, err = tx.Exec(
		"UPDATE reservations SET owner=? WHERE owner=?",
		"image:"+name,
		owner,
	); err != nil {
		return output, err
	}
	if err = tx.Commit(); err != nil {
		return output, err
	}
	return output, nil
}
