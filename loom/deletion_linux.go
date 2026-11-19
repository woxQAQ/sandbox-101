package loom

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// DeletionIntent is an exact inode receipt. Its existence does not grant
// deletion authority; the control plane commits it after checking reachability.
type DeletionIntent struct {
	Format int                     `json:"format"`
	Path   string                  `json:"path"`
	Dir    bool                    `json:"directory"`
	Nodes  map[string]FileIdentity `json:"nodes"`
}

// ValidatePath rejects symlinks at every component, including the private root.
func ValidatePath(ctx context.Context, stateRoot, path string, directory bool) error {
	relative, err := filepath.Rel(stateRoot, path)
	if err != nil || filepath.Clean(path) != path || relative == ".." || strings.HasPrefix(relative, "../") {
		return fmt.Errorf("GC path outside state: %s", path)
	}
	current := stateRoot
	var parts []string
	if relative != "." {
		parts = strings.Split(relative, string(filepath.Separator))
	}
	for index := -1; index < len(parts); index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if index >= 0 {
			current = filepath.Join(current, parts[index])
		}
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if index < len(parts)-1 || directory {
			if !info.IsDir() {
				return fmt.Errorf("GC symlink/nondirectory: %s", current)
			}
		} else if !info.Mode().IsRegular() {
			return fmt.Errorf("GC nonregular resource: %s", current)
		}
	}
	return nil
}

type contextReader struct {
	io.Reader

	ctx context.Context
}

func (reader contextReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.Reader.Read(data)
}

func DigestFileContext(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = file.Close()
	}()
	hash := sha256.New()
	if _, err = io.Copy(hash, contextReader{
		Reader: file,
		ctx:    ctx,
	}); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// OwnedDirectoryFiles checks only the supplied directory and exact allowlist.
// Unknown names, special files or mounts retain the entire object.
func OwnedDirectoryFiles(ctx context.Context, stateRoot, path string, allowed map[string]string) ([]string, error) {
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil, nil
	}
	if err := ValidatePath(ctx, stateRoot, path, true); err != nil {
		return nil, err
	}
	directories := map[string]bool{
		".": true,
	}
	for relative := range allowed {
		if filepath.IsAbs(relative) || filepath.Clean(relative) != relative || relative == ".." || strings.HasPrefix(relative, "../") {
			return nil, fmt.Errorf("unsafe deletion allowlist")
		}
		for parent := filepath.Dir(relative); parent != "."; parent = filepath.Dir(parent) {
			directories[parent] = true
		}
	}
	var paths []string
	err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		relative, _ := filepath.Rel(path, current)
		if entry.IsDir() {
			if !directories[relative] {
				return fmt.Errorf("unknown directory retained: %s", current)
			}
			return nil
		}
		want, known := allowed[relative]
		if !known || !entry.Type().IsRegular() {
			return fmt.Errorf("unknown artifact retained: %s", current)
		}
		if want != "" {
			got, err := DigestFileContext(ctx, current)
			if err != nil || got != want {
				return fmt.Errorf("artifact identity unconfirmed: %s: %v", current, err)
			}
		}
		paths = append(paths, current)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = rejectDeletionMounts(path); err != nil {
		return nil, err
	}
	return paths, nil
}

func rejectDeletionMounts(path string) error {
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	for line := range strings.SplitSeq(string(mounts), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 5 {
			mount := unescape.Replace(fields[4])
			if mount == path || strings.HasPrefix(mount, path+"/") {
				return fmt.Errorf("mounted artifact retained: %s", mount)
			}
		}
	}
	return nil
}

func CheckLocalReaders(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	readers := map[string]string{}
	watched := map[string]bool{}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		key := fmt.Sprintf("%x:%x:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)), stat.Ino)
		// Live paths pin their inode identity, so any descriptor reaching the
		// same inode is a reader even when it arrived through another name.
		readers[key] = ""
		watched[strings.ReplaceAll(path, "\n", `\012`)] = true
	}
	return scanLocalReaders(readers, watched)
}

// scanLocalReaders walks the engine process memory maps and open descriptors.
// An identity match counts as a reader when the receipt carries no path (a
// live artifact accepts any name reaching its inode) or when the current path
// still matches the receipt. A path match alone stays authoritative because a
// mapping can outlive its recorded device identity (overlayfs).
func scanLocalReaders(readers map[string]string, paths map[string]bool) error {
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(string(maps), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		mapped := ""
		if len(fields) > 5 {
			mapped = strings.TrimSuffix(strings.Join(fields[5:], " "), " (deleted)")
		}
		if paths[mapped] {
			return fmt.Errorf("artifact still mapped by engine process")
		}
		device := strings.Split(fields[3], ":")
		if len(device) != 2 {
			return fmt.Errorf("unrecognized mapping identity")
		}
		major, majorErr := strconv.ParseUint(device[0], 16, 32)
		minor, minorErr := strconv.ParseUint(device[1], 16, 32)
		if majorErr != nil || minorErr != nil {
			return fmt.Errorf("unrecognized mapping device")
		}
		if path, watched := readers[fmt.Sprintf("%x:%x:%s", major, minor, fields[4])]; watched && (path == "" || path == mapped) {
			return fmt.Errorf("artifact still mapped by engine process")
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return err
	}
	for _, fd := range fds {
		name := filepath.Join("/proc/self/fd", fd.Name())
		info, err := os.Stat(name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		stat := info.Sys().(*syscall.Stat_t)
		path, watched := readers[fmt.Sprintf("%x:%x:%d", unix.Major(uint64(stat.Dev)), unix.Minor(uint64(stat.Dev)), stat.Ino)]
		if !watched {
			continue
		}
		if path != "" {
			target, err := os.Readlink(name)
			if err != nil {
				// The descriptor raced closed between listing and resolution.
				continue
			}
			if strings.TrimSuffix(target, " (deleted)") != path {
				// The receipt inode was recycled by an unrelated open file.
				continue
			}
		}
		return fmt.Errorf("artifact still open by engine process")
	}
	return nil
}

// CheckLocalReaderIdentities also checks unlinked files against a persisted
// inode receipt. Physical names disappearing never proves that an open reader
// exited. Once a name is gone, though, the bare inode identity is ambiguous:
// the filesystem is free to recycle it for an unrelated file. A descriptor or
// mapping therefore only counts as a reader while its current path still
// matches the receipt.
func CheckLocalReaderIdentities(intent DeletionIntent) error {
	readers := map[string]string{}
	paths := map[string]bool{}
	for relative, identity := range intent.Nodes {
		key := fmt.Sprintf("%x:%x:%d", unix.Major(identity.Device), unix.Minor(identity.Device), identity.Inode)
		path := filepath.Join(intent.Path, relative)
		readers[key] = path
		paths[path] = true
	}
	if len(readers) == 0 {
		return nil
	}
	return scanLocalReaders(readers, paths)
}

func InspectDeletion(ctx context.Context, path string, files []string, directory bool) (DeletionIntent, error) {
	intent := DeletionIntent{
		Format: 1,
		Path:   path,
		Dir:    directory,
		Nodes:  map[string]FileIdentity{},
	}
	if directory {
		err := filepath.WalkDir(path, func(current string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			relative, _ := filepath.Rel(path, current)
			intent.Nodes[relative] = IdentityOf(info)
			return nil
		})
		return intent, err
	}
	for _, file := range files {
		info, err := os.Lstat(file)
		if err != nil {
			return intent, err
		}
		intent.Nodes["."] = IdentityOf(info)
	}
	return intent, nil
}

func ResumeDeletion(ctx context.Context, stateRoot string, intent DeletionIntent) ([]string, error) {
	if _, err := os.Lstat(intent.Path); os.IsNotExist(err) {
		return nil, nil
	}
	if err := ValidatePath(ctx, stateRoot, intent.Path, intent.Dir); err != nil {
		return nil, err
	}
	if intent.Dir {
		if err := rejectDeletionMounts(intent.Path); err != nil {
			return nil, err
		}
	}
	var paths []string
	err := filepath.WalkDir(intent.Path, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = verifyDeletionNode(intent, path); err != nil {
			return err
		}
		if !entry.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})
	return paths, err
}

func verifyDeletionNode(intent DeletionIntent, path string) error {
	relative, err := filepath.Rel(intent.Path, path)
	if err != nil {
		return err
	}
	if filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, "../") || filepath.Clean(path) != path {
		return fmt.Errorf("deletion node escapes its receipt")
	}
	want, exists := intent.Nodes[relative]
	if !exists || intent.Format != 1 {
		return fmt.Errorf("GC deletion inode replaced or unknown: %s", path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if IdentityOf(info) != want {
		return fmt.Errorf("GC deletion inode replaced or unknown: %s", path)
	}
	return nil
}

// RemoveDeletionFile repeats the exact inode check immediately before unlink.
// The caller already holds the durable intent and keeps responsibility on error.
func RemoveDeletionFile(ctx context.Context, stateRoot string, intent DeletionIntent, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidatePath(ctx, stateRoot, filepath.Dir(path), true); err != nil {
		return err
	}
	if err := verifyDeletionNode(intent, path); err != nil {
		return err
	}
	relativeNode, _ := filepath.Rel(intent.Path, path)
	mode := os.FileMode(intent.Nodes[relativeNode].Mode)
	if !mode.IsRegular() && mode&os.ModeType != os.ModeSocket {
		return fmt.Errorf("unsupported deletion node type")
	}
	root, err := os.OpenRoot(stateRoot)
	if err != nil {
		return err
	}
	defer func() {
		_ = root.Close()
	}()
	relative, _ := filepath.Rel(stateRoot, path)
	if err = root.Remove(relative); err != nil && !os.IsNotExist(err) {
		return err
	}
	return SyncDir(filepath.Dir(path))
}

func RemoveDeletionDirectories(ctx context.Context, stateRoot string, intent DeletionIntent) error {
	if intent.Dir {
		var directories []string
		if err := filepath.WalkDir(intent.Path, func(path string, entry os.DirEntry, err error) error {
			if os.IsNotExist(err) {
				return nil
			}
			if err != nil {
				return err
			}
			if !entry.IsDir() {
				return fmt.Errorf("unexpected artifact appeared during GC: %s", path)
			}
			if err = verifyDeletionNode(intent, path); err != nil {
				return err
			}
			directories = append(directories, path)
			return nil
		}); err != nil {
			return err
		}
		root, err := os.OpenRoot(stateRoot)
		if err != nil {
			return err
		}
		defer func() {
			_ = root.Close()
		}()
		for _, directory := range slices.Backward(directories) {
			if err = ValidatePath(ctx, stateRoot, directory, true); err != nil {
				return err
			}
			if err = verifyDeletionNode(intent, directory); err != nil {
				return err
			}
			relative, _ := filepath.Rel(stateRoot, directory)
			if err = root.Remove(relative); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err = SyncDir(filepath.Dir(directory)); err != nil {
				return err
			}
		}
	}
	parent := filepath.Dir(intent.Path)
	for {
		if err := ValidatePath(ctx, stateRoot, parent, true); err != nil {
			if os.IsNotExist(err) && parent != stateRoot {
				parent = filepath.Dir(parent)
				continue
			}
			return err
		}
		return SyncDir(parent)
	}
}
