package loom

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// InspectWritableDisk fixes the independently writable inode after preparation.
// This is an identity proof; the evolving disk is never hashed into a new proof.
func InspectWritableDisk(resource Resource) (FileIdentity, error) {
	parent, err := OpenCaptureDir(filepath.Dir(resource.Disk))
	if err != nil {
		return FileIdentity{}, err
	}
	defer func() { _ = parent.Close() }()
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(resource.Disk), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return FileIdentity{}, err
	}
	file := os.NewFile(uintptr(fd), resource.Disk)
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return FileIdentity{}, err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		return FileIdentity{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != resource.Config.Shape.DiskMiB<<20 || stat.Nlink != 1 {
		return FileIdentity{}, fmt.Errorf("writable disk is not an independent full-size inode")
	}
	return IdentityOf(info), nil
}

func VerifyWritableDisk(resource Resource, expected FileIdentity) error {
	actual, err := InspectWritableDisk(resource)
	if err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("writable disk inode changed")
	}
	return nil
}
