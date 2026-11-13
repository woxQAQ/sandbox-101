package loom

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"golang.org/x/sys/unix"
)

// RestoreMaterials records delivery bytes, not the evolving writable disk.
// Complete is set and committed by the control plane after SyncMaterials.
type RestoreMaterials struct {
	Resource Resource                    `json:"resource"`
	Version  FileIdentity                `json:"version"`
	Restore  FileIdentity                `json:"restore"`
	Files    map[string]RestoreFileProof `json:"files"`
	Complete bool                        `json:"complete"`
}

type RestoreFileProof struct {
	Source    string       `json:"source"`
	Candidate string       `json:"candidate"`
	Size      int64        `json:"size"`
	Identity  FileIdentity `json:"identity"`
}

var restoreNames = []string{"config.json", "state.json", "disk.raw", "memory-ranges"}

func RestoreFileNames() []string { return slices.Clone(restoreNames) }

func restorePath(resource Resource, name string) string {
	if name == "disk.raw" {
		return resource.Disk
	}
	return filepath.Join(resource.RestoreDir, name)
}

func restoreSource(directory, name string) string {
	if name == "disk.raw" {
		return filepath.Join(directory, name)
	}
	return filepath.Join(directory, "ch", name)
}

// All checks and syncing refer to the same no-follow descriptor.
func inspectRestoreFile(path, want string, size int64, synchronize bool) (RestoreFileProof, error) {
	parent, err := OpenCaptureDir(filepath.Dir(path))
	if err != nil {
		return RestoreFileProof{}, err
	}
	defer func() { _ = parent.Close() }()
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return RestoreFileProof{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return RestoreFileProof{}, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || size >= 0 && info.Size() != size {
		return RestoreFileProof{}, fmt.Errorf("candidate regular-file/size mismatch: %s", path)
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return RestoreFileProof{}, err
	}
	got := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	if got != want {
		return RestoreFileProof{}, fmt.Errorf("candidate checksum mismatch: %s", path)
	}
	if synchronize {
		if err = file.Sync(); err != nil {
			return RestoreFileProof{}, err
		}
	}
	return RestoreFileProof{Source: want, Candidate: got, Size: info.Size(), Identity: IdentityOf(info)}, nil
}

// RestoreBoundary keeps the candidate directory descriptors pinned for one
// preparation call. Closing it releases descriptors and never deletes materials.
type RestoreBoundary struct {
	Materials   *RestoreMaterials
	source      string
	directories []*os.File
}

func (boundary *RestoreBoundary) Close() error {
	var result error
	for _, directory := range boundary.directories {
		result = errors.Join(result, directory.Close())
	}
	boundary.directories = nil
	return result
}

// PrepareRestore creates only the explicit candidate and returns its observed
// identities and fixed source checksums. Existing unproved versions are refused.
// The caller commits this proof before invoking any CopyFile operation below.
func PrepareRestore(resource Resource, operation, source string, digests map[string]string, createVM bool) (*RestoreBoundary, error) {
	versionPath := filepath.Join(resource.Dir, "versions", operation)
	if !ValidResourceID(operation, "op-") || resource.Disk != filepath.Join(versionPath, "disk.raw") || resource.RestoreDir != filepath.Join(versionPath, "restore") {
		return nil, Fail(ManualRecovery, "restore candidate ownership mismatch")
	}
	boundary := &RestoreBoundary{source: source, Materials: &RestoreMaterials{Resource: resource, Files: map[string]RestoreFileProof{}}}
	complete := false
	defer func() {
		if !complete {
			_ = boundary.Close()
		}
	}()
	root, err := OpenCaptureDir(resource.Dir)
	if os.IsNotExist(err) && createVM {
		if err = os.Mkdir(resource.Dir, 0o700); err == nil {
			root, err = OpenCaptureDir(resource.Dir)
		}
	}
	if err != nil {
		return nil, err
	}
	boundary.directories = append(boundary.directories, root)
	versions, err := CaptureChild(root, "versions", 0o700)
	if err != nil {
		return nil, err
	}
	boundary.directories = append(boundary.directories, versions)
	if err = unix.Mkdirat(int(versions.Fd()), operation, 0o700); err != nil {
		return nil, err
	}
	version, err := OpenCaptureDir(versionPath)
	if err != nil {
		return nil, err
	}
	boundary.directories = append(boundary.directories, version)
	restore, err := CaptureChild(version, "restore", 0o700)
	if err != nil {
		return nil, err
	}
	boundary.directories = append(boundary.directories, restore)
	for _, name := range restoreNames {
		key := "ch/" + name
		if name == "disk.raw" {
			key = name
		}
		proof, err := inspectRestoreFile(restoreSource(source, name), digests[key], -1, false)
		if err != nil {
			return nil, err
		}
		proof.Candidate = ""
		boundary.Materials.Files[name] = proof
	}
	versionInfo, err := version.Stat()
	if err != nil {
		return nil, err
	}
	restoreInfo, err := restore.Stat()
	if err != nil {
		return nil, err
	}
	boundary.Materials.Version = IdentityOf(versionInfo)
	boundary.Materials.Restore = IdentityOf(restoreInfo)
	if err = boundary.syncDirectories(); err != nil {
		return nil, err
	}
	if err = SyncDir(filepath.Dir(resource.Dir)); err != nil {
		return nil, err
	}
	complete = true
	return boundary, nil
}

func (boundary *RestoreBoundary) syncDirectories() error {
	for _, v := range slices.Backward(boundary.directories) {
		if err := v.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// CopyFile creates a private pending copy, or the immutable RAM hardlink. Each
// call has a separate control-plane checkpoint before the next physical effect.
func (boundary *RestoreBoundary) CopyFile(name string) error {
	if _, exists := boundary.Materials.Files[name]; !exists {
		return Fail(Invalid, "unknown restore file")
	}
	destination := restorePath(boundary.Materials.Resource, name)
	source := restoreSource(boundary.source, name)
	if name == "memory-ranges" {
		return os.Link(source, destination)
	}
	mode := os.FileMode(0o444)
	if name == "disk.raw" {
		mode = 0o600
	}
	return CopyFile(source, destination+".pending", mode)
}

func (boundary *RestoreBoundary) PublishFile(name string) error {
	proof, exists := boundary.Materials.Files[name]
	if !exists || name == "memory-ranges" {
		return Fail(Invalid, "unknown or immutable restore file")
	}
	destination := restorePath(boundary.Materials.Resource, name)
	if _, err := inspectRestoreFile(destination+".pending", proof.Source, proof.Size, true); err != nil {
		return err
	}
	return os.Rename(destination+".pending", destination)
}

// SyncMaterials verifies every delivered inode and syncs the retained directory
// descriptors. It does not set Complete or publish the candidate to a catalog.
func (boundary *RestoreBoundary) SyncMaterials() error {
	for _, name := range restoreNames {
		proof := boundary.Materials.Files[name]
		file, err := inspectRestoreFile(restorePath(boundary.Materials.Resource, name), proof.Source, proof.Size, true)
		if err != nil {
			return err
		}
		if name == "memory-ranges" && file.Identity != proof.Identity {
			return Fail(ManualRecovery, "candidate backing inode mismatch")
		}
		boundary.Materials.Files[name] = file
	}
	return boundary.syncDirectories()
}

// VerifyRestoreMaterials checks the fixed completion proof. It never rehashes
// incomplete materials into new authority or verifies a live disk's old digest.
func VerifyRestoreMaterials(proof *RestoreMaterials, expected Resource, digests map[string]string, disk bool) error {
	if proof == nil || !proof.Complete || !reflect.DeepEqual(proof.Resource, expected) || len(proof.Files) != len(restoreNames) {
		return Fail(ManualRecovery, "restore completion proof missing or inconsistent; legacy handoff requires manual recovery")
	}
	for path, identity := range map[string]FileIdentity{filepath.Dir(expected.RestoreDir): proof.Version, expected.RestoreDir: proof.Restore} {
		file, err := OpenCaptureDir(path)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		err = errors.Join(err, file.Close())
		if err != nil {
			return err
		}
		if IdentityOf(info) != identity {
			return Fail(ManualRecovery, "restore directory identity changed")
		}
	}
	for _, name := range restoreNames {
		file := proof.Files[name]
		key := "ch/" + name
		if name == "disk.raw" {
			key = name
		}
		if file.Source != digests[key] || file.Candidate != file.Source || file.Size <= 0 {
			return Fail(Incompatible, "restore fixed source proof mismatch")
		}
		if name == "disk.raw" && !disk {
			continue
		}
		got, err := inspectRestoreFile(restorePath(expected, name), file.Source, file.Size, false)
		if err != nil {
			return err
		}
		if got.Identity != file.Identity {
			return Fail(ManualRecovery, "restore artifact inode mismatch")
		}
	}
	return nil
}
