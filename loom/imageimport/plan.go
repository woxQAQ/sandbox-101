package imageimport

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	v1 "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

type ImageImportOptions struct {
	Layout          string `json:"layout"`
	Digest          string `json:"digest"`
	Materialization string `json:"materialization"`
	MaxBytes        int64  `json:"max_bytes"`
	MaxFiles        int64  `json:"max_files"`
}
type (
	ImageImportLayer struct {
		Digest    string           `json:"digest"`
		DiffID    string           `json:"diff_id"`
		MediaType string           `json:"media_type"`
		Size      int64            `json:"size"`
		Cached    *host.LayerCache `json:"cached,omitempty"`
	}
	ImageImportPlan struct {
		Options      ImageImportOptions `json:"options"`
		ConfigDigest string             `json:"config_digest"`
		ConfigSize   int64              `json:"config_size"`
		Tools        map[string]string  `json:"tools"`
		Builder      string             `json:"builder"`
		Layers       []ImageImportLayer `json:"layers"`
	}
)

func NormalizeImageImportOptions(options ImageImportOptions) (ImageImportOptions, error) {
	if options.Materialization == "" {
		options.Materialization = host.LayeredEROFSLayout
	}
	if options.Materialization != host.LayeredEROFSLayout && options.Materialization != host.MergedOCIEROFSLayout {
		return options, loom.Fail(loom.Invalid, "unknown image materialization")
	}
	if _, err := host.SHAName(options.Digest); err != nil {
		return options, err
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = 8 << 30
	}
	if options.MaxFiles == 0 {
		options.MaxFiles = 1000000
	}
	if options.MaxBytes <= 0 || options.MaxBytes > 1<<40 || options.MaxFiles <= 0 || options.MaxFiles > 1<<30 {
		return options, loom.Fail(loom.Invalid, "invalid import bounds")
	}
	source, err := filepath.Abs(options.Layout)
	if err != nil {
		return options, err
	}
	options.Layout = source
	return options, nil
}

func ReadImageImportPlan(ctx context.Context, state string, options ImageImportOptions) (ImageImportPlan, error) {
	var plan ImageImportPlan
	options, err := NormalizeImageImportOptions(options)
	if err != nil {
		return plan, err
	}
	source := options.Layout
	manifestPath, err := host.Blob(source, options.Digest)
	if err != nil {
		return plan, err
	}
	info, err := os.Stat(manifestPath)
	if err != nil {
		return plan, err
	}
	if info.Size() > 16<<20 {
		return plan, loom.Fail(loom.Exhausted, "OCI manifest exceeds metadata bound")
	}
	var manifest v1.Manifest
	if err = host.ReadBlobJSON(manifestPath, options.Digest, &manifest); err != nil {
		return plan, err
	}
	if manifest.SchemaVersion != 2 || len(manifest.Layers) == 0 || len(manifest.Layers) > host.MaxImageDataDevices {
		return plan, loom.Fail(loom.Incompatible, "OCI image requires 1..%d layers", host.MaxImageDataDevices)
	}
	configPath, err := host.Blob(source, manifest.Config.Digest.String())
	if err != nil {
		return plan, err
	}
	info, err = os.Stat(configPath)
	if err != nil {
		return plan, err
	}
	if manifest.Config.Size < 0 || info.Size() != manifest.Config.Size || info.Size() > 16<<20 {
		return plan, loom.Fail(loom.Invalid, "OCI config descriptor size mismatch or metadata bound")
	}
	var config v1.Image
	if err = host.ReadBlobJSON(configPath, manifest.Config.Digest.String(), &config); err != nil {
		return plan, err
	}
	if config.OS != "linux" || config.Architecture != runtime.GOARCH || config.RootFS.Type != "layers" || len(config.RootFS.DiffIDs) != len(manifest.Layers) {
		return plan, loom.Fail(loom.Incompatible, "OCI platform/rootfs mismatch")
	}
	tools, err := host.ImageTools(ctx)
	if err != nil {
		return plan, err
	}
	builder := host.ImageBuilder(tools)
	if options.Materialization == host.MergedOCIEROFSLayout {
		tools, err = host.MergedOCITools(ctx, tools)
		if err != nil {
			return plan, err
		}
		builder = host.MergedOCIBuilder(tools)
	}
	plan = ImageImportPlan{Options: options, ConfigDigest: manifest.Config.Digest.String(), ConfigSize: manifest.Config.Size, Tools: tools, Builder: builder}
	var compressed int64
	for index, layer := range manifest.Layers {
		if layer.Size < 0 || !host.SupportedLayerMedia(layer.MediaType) {
			return plan, loom.Fail(loom.Incompatible, "unsupported OCI layer media or size")
		}
		if layer.Size > options.MaxBytes*2-compressed {
			return plan, loom.Fail(loom.Exhausted, "OCI compressed staging bound exceeded")
		}
		compressed += layer.Size
		diff := config.RootFS.DiffIDs[index].String()
		if _, err = host.SHAName(layer.Digest.String()); err != nil {
			return plan, err
		}
		if _, err = host.SHAName(diff); err != nil {
			return plan, err
		}
		entry := ImageImportLayer{Digest: layer.Digest.String(), DiffID: diff, MediaType: layer.MediaType, Size: layer.Size}
		if options.Materialization == host.LayeredEROFSLayout {
			cache, err := host.LoadLayerCache(state, builder, diff)
			if err == nil {
				entry.Cached = &cache
			} else if !os.IsNotExist(err) {
				return plan, err
			}
		}
		plan.Layers = append(plan.Layers, entry)
	}
	return plan, ctx.Err()
}

type (
	ImageImportBoundary struct {
		Path   string            `json:"path"`
		Work   loom.FileIdentity `json:"work"`
		Output loom.FileIdentity `json:"output"`
	}
	ImageCacheWitness struct {
		PhysicalBytes int64             `json:"physical_bytes"`
		Path          string            `json:"path"`
		Directory     loom.FileIdentity `json:"directory"`
		Data          loom.FileIdentity `json:"data"`
		Receipt       loom.FileIdentity `json:"receipt"`
		Cache         host.LayerCache   `json:"cache"`
		ReceiptSHA256 string            `json:"receipt_sha256"`
	}
)

func PrepareImageImport(state, operation string) (ImageImportBoundary, error) {
	var boundary ImageImportBoundary
	if !loom.ValidResourceID(operation, "op-") {
		return boundary, loom.Fail(loom.Invalid, "invalid import operation")
	}
	parent := filepath.Join(state, "imports")
	if err := os.Mkdir(parent, 0o700); err != nil && !os.IsExist(err) {
		return boundary, err
	}
	directory, err := loom.OpenCaptureDir(parent)
	if err != nil {
		return boundary, err
	}
	_ = directory.Close()
	work := filepath.Join(parent, operation)
	if err = os.Mkdir(work, 0o700); err != nil {
		return boundary, loom.Fail(loom.ManualRecovery, "import work cannot be adopted without preparation receipt: %v", err)
	}
	boundary.Path = work
	info, err := os.Lstat(work)
	if err != nil {
		return boundary, err
	}
	boundary.Work = loom.IdentityOf(info)
	for _, relative := range []string{"input", "input/blobs", "input/blobs/sha256", "output"} {
		if err = os.Mkdir(filepath.Join(work, relative), 0o700); err != nil {
			return boundary, err
		}
	}
	info, err = os.Lstat(filepath.Join(work, "output"))
	if err != nil {
		return boundary, err
	}
	boundary.Output = loom.IdentityOf(info)
	if err = loom.SyncDir(work); err != nil {
		return boundary, err
	}
	return boundary, loom.SyncDir(parent)
}

func VerifyImageImportBoundary(state, operation string, boundary ImageImportBoundary, output bool) error {
	if boundary.Path != filepath.Join(state, "imports", operation) || !loom.ValidResourceID(operation, "op-") {
		return loom.Fail(loom.Invalid, "import boundary path changed")
	}
	for path, want := range map[string]loom.FileIdentity{boundary.Path: boundary.Work} {
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || loom.IdentityOf(info) != want {
			return fmt.Errorf("import work identity changed")
		}
	}
	if output {
		info, err := os.Lstat(filepath.Join(boundary.Path, "output"))
		if err != nil {
			return err
		}
		if !info.IsDir() || loom.IdentityOf(info) != boundary.Output {
			return fmt.Errorf("import output identity changed")
		}
	}
	return nil
}
