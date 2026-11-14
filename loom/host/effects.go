package host

import (
	"archive/tar"
	"context"
	"io/fs"
)

// These operations accept explicit files, directories and frozen format inputs.
// They neither consult nor publish a control-plane catalog.
const (
	LayerProfile          = layerProfile
	SharedLayerProfile    = sharedLayerProfile
	SharedEROFSVersion    = sharedEROFSVersion
	MergedOCIProfile      = mergedOCIProfile
	CloudHypervisorCommit = cloudHypervisorCommit
)

type (
	LayerCache      = layerCache
	CachedHeader    = cachedHeader
	HeaderAttribute = headerAttribute
)

func (m ImageManifest) SharedLayered() bool {
	return m.sharedLayered()
}

func (m ImageManifest) VerifyLayeredArtifacts(state, directory string) error {
	return m.verifyLayeredArtifacts(state, directory)
}

func (m ImageManifest) VerifyMergedOCIArtifacts(directory string) error {
	return m.verifyMergedOCIArtifacts(directory)
}

func (c layerCache) Artifact(state string, index int) (ImageArtifact, error) {
	return c.artifact(state, index)
}

func SHAName(digest string) (string, error) {
	return shaName(digest)
}

func ReadBlobJSON(file, digest string, value any) error {
	return readBlobJSON(file, digest, value)
}

func Blob(directory, digest string) (string, error) {
	return blob(directory, digest)
}

func ImageTools(ctx context.Context) (map[string]string, error) {
	return imageTools(ctx)
}

func ImageBuilder(tools map[string]string) string {
	return imageBuilder(tools)
}

func MergedOCITools(ctx context.Context, tools map[string]string) (map[string]string, error) {
	return mergedOCITools(ctx, tools)
}

func MergedOCIBuilder(tools map[string]string) string {
	return mergedOCIBuilder(tools)
}

func SupportedLayerMedia(media string) bool {
	return supportedLayerMedia(media)
}

func LayerRelativePath(builder, diff string) string {
	return layerRelativePath(builder, diff)
}

func LayerUUID(diff string) string {
	return layerUUID(diff)
}

func LoadLayerCache(state, builder, diff string) (LayerCache, error) {
	return loadLayerCache(state, builder, diff)
}

func BuildLayerCache(ctx context.Context, state, input, media, builder, diff string, size, entries int64) (LayerCache, error) {
	return buildLayerCache(ctx, state, input, media, builder, diff, size, entries)
}

func BuildSharedLayerCache(ctx context.Context, state, input, media, builder, diff string, size, entries int64) (LayerCache, error) {
	return buildSharedLayerCache(ctx, state, input, media, builder, diff, size, entries)
}

func ValidateLayer(file, media, diff string, maxBytes, maxFiles int64) (int64, int64, error) {
	return validateLayer(file, media, diff, maxBytes, maxFiles)
}

func ValidateLayerFor(file, media, diff string, maxBytes, maxFiles int64, merged bool) (int64, int64, error) {
	return validateLayerFor(file, media, diff, maxBytes, maxFiles, merged)
}

func MaterializeMergedOCI(ctx context.Context, input, output, digest string, limit int64) error {
	return materializeMergedOCI(ctx, input, output, digest, limit)
}

func MaterializeSharedFsmeta(ctx context.Context, state, output string, artifacts []ImageArtifact, builder string) error {
	return materializeSharedFsmeta(ctx, state, output, artifacts, builder)
}

func MaterializeFsmeta(ctx context.Context, state, output string, artifacts []ImageArtifact, digest string) error {
	return materializeFsmeta(ctx, state, output, artifacts, digest)
}

func ImageToolSandbox(work string, reads []ImageMount, mapped bool) ([]string, error) {
	return imageToolSandbox(work, reads, mapped)
}

func RunImageTool(ctx context.Context, work string, reads []ImageMount, tool string, args ...string) error {
	return runImageTool(ctx, work, reads, tool, args...)
}

func RunMappedImageTool(ctx context.Context, args []string) error {
	return runMappedImageTool(ctx, args)
}

func VerifyImageDeviceTable(meta string, layers, diffs []string, shared bool) error {
	return verifyImageDeviceTable(meta, layers, diffs, shared)
}

func EROFSSuper(file string) ([]byte, int64, error) {
	return erofsSuper(file)
}

func SharedHeaders(image fs.FS, receipt LayerCache) ([]tar.Header, error) {
	return sharedHeaders(image, receipt)
}

func CacheHeader(header tar.Header) CachedHeader {
	return cacheHeader(header)
}

func DataArtifact(builder, diff, hash, receipt string, index int) ImageArtifact {
	return dataArtifact(builder, diff, hash, receipt, index)
}

func ValidateLayerEntry(header *tar.Header, entries map[string]byte, whiteouts map[string]bool) error {
	return validateLayerEntry(header, entries, whiteouts)
}
