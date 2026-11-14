package host

import (
	"errors"
	// ImageManifest fixes the materializer identity and the complete ordered set of
	// immutable block devices. Catalog publication remains the caller's decision.
	"fmt"
	"runtime"
)

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
	if m.Format != 1 || m.Layout != "merged-erofs-v1" || m.Digest != digest || m.Architecture != runtime.GOARCH || m.Builder != "" || len(m.Artifacts) != 0 {
		return errors.New("image format/platform mismatch")
	}
	for _, name := range []string{
		"kernel",
		"initramfs",
		"root.erofs",
	} {
		if _, ok := m.Files[name]; !ok {
			return fmt.Errorf("image missing %s", name)
		}
	}
	return verifyFiles(dir, m.Files)
}
