package host

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"

	api "github.com/woxQAQ/haven/loom"
)

func validateCOWTopology(dir string, memoryBytes uint64) error {
	return api.ValidateCOWTopology(dir, memoryBytes)
}

func verifyCOWFileMappings(maps io.Reader, backing *unix.Stat_t, size uint64) error {
	return api.VerifyCOWFileMappings(maps, backing, size)
}

func (b *CloudHypervisor) verifyCOWMappings(r Resource) error {
	process, alive, err := b.process(r)
	if err != nil {
		return err
	}
	if !alive {
		return errors.New("restored VMM exited before COW verification")
	}
	var backing unix.Stat_t
	if err = unix.Stat(filepath.Join(r.RestoreDir, "memory-ranges"), &backing); err != nil {
		return err
	}
	maps, err := os.Open(fmt.Sprintf("/proc/%d/maps", process.PID))
	if err != nil {
		return err
	}
	defer func() {
		_ = maps.Close()
	}()
	if err = verifyCOWFileMappings(maps, &backing, uint64(backing.Size)); err != nil {
		return err
	}
	start, _, err := procStart(process.PID)
	if err != nil || start != process.StartTime {
		return errors.New("VMM identity changed during COW verification")
	}
	return nil
}
