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

func MaterializeMergedOCI(ctx context.Context, input, output, digest string) error {
	return materializeMergedOCI(ctx, input, output, digest)
}

func RunImageTool(ctx context.Context, work, tool string, args ...string) error {
	return runImageTool(ctx, work, tool, args...)
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

func ValidateLayerEntry(header *tar.Header, entries map[string]byte, whiteouts map[string]bool) error {
	return validateLayerEntry(header, entries, whiteouts)
}
