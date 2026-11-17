package image

import (
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

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
