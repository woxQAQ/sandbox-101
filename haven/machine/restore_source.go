package machine

import (
	"fmt"
	"path/filepath"

	"github.com/woxQAQ/haven/loom"
)

func VerifyRestoreSourceFiles(source SnapshotSource) error {
	directory, err := loom.OpenCaptureDir(source.Snapshot.Dir)
	if err != nil {
		return err
	}
	info, err := directory.Stat()
	closeErr := directory.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if loom.IdentityOf(info) != source.Publication {
		return fmt.Errorf("restore source directory identity changed")
	}
	if err = loom.VerifyFiles(source.Snapshot.Dir, source.Snapshot.Manifest.Files); err != nil {
		return err
	}
	return loom.ValidateCOWTopology(filepath.Join(source.Snapshot.Dir, "ch"), uint64(source.Snapshot.Manifest.Config.Shape.MemoryMiB)<<20)
}
