package engine

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

func (g *gcGraph) inventory() error {
	for _, root := range []string{"vms", "snapshots", "images", "imports"} {
		dir := filepath.Join(g.e.opts.StateDir, root)
		if err := g.path(dir, true); err != nil {
			return err
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err = g.ctx.Err(); err != nil {
				return err
			}
			p := filepath.Join(dir, entry.Name())
			if !entry.IsDir() {
				return fmt.Errorf("GC unknown resource: %s", p)
			}
			switch root {
			case "vms":
				v, ok := g.vms[entry.Name()]
				if !ok {
					return fmt.Errorf("GC unknown VM directory: %s", p)
				}
				if err = g.vmInventory(v); err != nil {
					return err
				}
			case "snapshots":
				if _, ok := g.snapshots[entry.Name()]; !ok {
					return fmt.Errorf("GC unknown snapshot directory: %s", p)
				}
			case "images":
				if _, err = shaName("sha256:" + entry.Name()); err != nil {
					return err
				}
				if _, ok := g.images["sha256:"+entry.Name()]; !ok {
					// Interrupted publications remain roots even before catalog
					// admission; import is the only way to publish them.
					var m ImageManifest
					if err = gcReadJSON(
						filepath.Join(p, "image.json"),
						&m,
					); err != nil ||
						m.Digest != "sha256:"+entry.Name() {
						return fmt.Errorf(
							"GC unknown unpublished image root: %s",
							p,
						)
					}
					if err = g.verifyImage(m, p); err != nil {
						return err
					}
				}
			case "imports":
				var opts ImportOptions
				if _, pending := g.deletions[p]; pending {
					if _, ok := g.images["sha256:"+entry.Name()]; !ok {
						return fmt.Errorf(
							"GC import deletion catalog root missing: %s",
							p,
						)
					}
					continue
				}
				if err = gcReadJSON(
					filepath.Join(p, "intent.json"),
					&opts,
				); err != nil ||
					opts.Digest != "sha256:"+entry.Name() {
					return fmt.Errorf("GC unknown import ownership: %s", p)
				}
				if _, err = shaName(opts.Digest); err != nil {
					return err
				}
				if _, ok := g.images[opts.Digest]; !ok {
					return fmt.Errorf(
						"GC incomplete import retained as recovery root: %s",
						p,
					)
				}
				g.imports[p] = opts
			}
		}
	}
	for _, s := range g.snapshots {
		found := false
		for _, o := range g.ops {
			if o.Operation.Request.Action == "snapshot" &&
				o.Operation.SnapshotID == s.Snapshot.ID &&
				o.Operation.VMID == s.Snapshot.SourceVMID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf(
				"GC snapshot operation ownership missing: %s",
				s.Snapshot.ID,
			)
		}
	}
	return nil
}

func (g *gcGraph) vmInventory(v vmRecord) error {
	entries, err := os.ReadDir(v.Resource.Dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Name() {
		case "network.json", "network-removing.json", "network-policy.json":
			if !entry.Type().IsRegular() {
				return fmt.Errorf("GC unknown network ownership file")
			}
			continue
		case "versions", "disks", "capture", "runs":
		default:
			return fmt.Errorf(
				"GC unknown VM resource: %s",
				filepath.Join(v.Resource.Dir, entry.Name()),
			)
		}
		p := filepath.Join(v.Resource.Dir, entry.Name())
		if !entry.IsDir() {
			return fmt.Errorf("GC resource traverses non-directory: %s", p)
		}
		children, err := os.ReadDir(p)
		if err != nil {
			return err
		}
		for _, child := range children {
			path := filepath.Join(p, child.Name())
			known := false
			switch entry.Name() {
			case "versions":
				_, known = g.versions[path]
			case "disks":
				_, known = g.versions[strings.TrimSuffix(path, ".pending")]
			case "capture":
				o, ok := g.ops[child.Name()]
				known = ok && o.Operation.VMID == v.VM.ID &&
					o.Operation.Request.Action == "snapshot" &&
					o.Journal.Resource != nil
			case "runs":
				known = v.Resource.RunDir == path
				for _, o := range g.ops {
					for _, r := range []*Resource{o.Journal.Old, o.Journal.Resource} {
						known = known ||
							r != nil && r.VMID == v.VM.ID && r.RunDir == path
					}
				}
			}
			if !known {
				return fmt.Errorf("GC unknown resource ownership: %s", path)
			}
		}
	}
	return nil
}

// path rejects symlinks at every component, including the private state root.
func (g *gcGraph) path(path string, dir bool) error {
	base := g.e.opts.StateDir
	rel, err := filepath.Rel(base, path)
	if err != nil || filepath.Clean(path) != path || rel == ".." ||
		strings.HasPrefix(rel, "../") {
		return fmt.Errorf("GC path outside state: %s", path)
	}
	current := base
	parts := []string{}
	if rel != "." {
		parts = strings.Split(rel, string(filepath.Separator))
	}
	for i := -1; i < len(parts); i++ {
		if err := g.ctx.Err(); err != nil {
			return err
		}
		if i >= 0 {
			current = filepath.Join(current, parts[i])
		}
		st, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if i < len(parts)-1 || dir {
			if !st.IsDir() {
				return fmt.Errorf("GC symlink/nondirectory: %s", current)
			}
		} else if !st.Mode().IsRegular() {
			return fmt.Errorf("GC nonregular resource: %s", current)
		}
	}
	return nil
}

func (g *gcGraph) immutable(root, rel, want string) error {
	if filepath.IsAbs(rel) || filepath.Clean(rel) != rel || rel == "." ||
		strings.HasPrefix(rel, "../") ||
		strings.Contains(rel, "\\") {
		return fmt.Errorf("GC unsafe artifact path")
	}
	path := filepath.Join(root, rel)
	if err := g.path(path, false); err != nil {
		return err
	}
	st, err := os.Stat(path)
	if err != nil {
		return err
	}
	if st.Mode().Perm()&0o222 != 0 || st.Size() == 0 {
		return fmt.Errorf("GC mutable/empty immutable artifact: %s", path)
	}
	got, err := gcDigest(g.ctx, path)
	if err == nil && got != want {
		return fmt.Errorf("GC artifact checksum mismatch: %s", path)
	}
	return err
}

type gcReader struct {
	io.Reader

	ctx context.Context
}

func (r gcReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

func gcDigest(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err = io.Copy(h, gcReader{Reader: f, ctx: ctx}); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

// ownedFiles is an exact allowlist, not recursive deletion authority. A journal
// proves its canonical version path; a verified receipt proves cache contents.
// Unexpected names, symlinks, special files and mounts retain the entire object.
func (g *gcGraph) ownedFiles(
	path string,
	allowed map[string]string,
	dir bool,
) ([]string, error) {
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		return nil, nil // Retry after a crash between unlink and metadata retirement.
	}
	if err := g.path(path, dir); err != nil {
		return nil, err
	}
	if !dir {
		return []string{path}, nil
	}
	dirs := map[string]bool{".": true}
	for rel := range allowed {
		for p := filepath.Dir(rel); p != "."; p = filepath.Dir(p) {
			dirs[p] = true
		}
	}
	paths := []string{}
	err := filepath.WalkDir(
		path,
		func(p string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if err = g.ctx.Err(); err != nil {
				return err
			}
			rel, _ := filepath.Rel(path, p)
			if d.IsDir() {
				if !dirs[rel] {
					return fmt.Errorf("unknown directory retained: %s", p)
				}
				return nil
			}
			want, ok := allowed[rel]
			if !ok || !d.Type().IsRegular() {
				return fmt.Errorf("unknown artifact retained: %s", p)
			}
			if want != "" {
				got, err := gcDigest(g.ctx, p)
				if err != nil || got != want {
					return fmt.Errorf(
						"artifact identity unconfirmed: %s: %v",
						p,
						err,
					)
				}
			}
			paths = append(paths, p)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	// Check mounts even when the tree is empty or all file names are expected.
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	unescape := strings.NewReplacer(
		`\040`,
		" ",
		`\011`,
		"\t",
		`\012`,
		"\n",
		`\134`,
		`\`,
	)
	for line := range strings.SplitSeq(string(mounts), "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 {
			mount := unescape.Replace(f[4])
			if mount == path || strings.HasPrefix(mount, path+"/") {
				return nil, fmt.Errorf("mounted artifact retained: %s", mount)
			}
		}
	}
	return paths, nil
}

func gcLocalReaders(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	inodes := map[string]bool{}
	for _, path := range paths {
		st, err := os.Stat(path)
		if err != nil {
			return err
		}
		s := st.Sys().(*syscall.Stat_t)
		inodes[fmt.Sprintf("%x:%x:%d", unix.Major(uint64(s.Dev)), unix.Minor(uint64(s.Dev)), s.Ino)] = true
	}
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(string(maps), "\n") {
		// Some overlay filesystems expose different stat/proc device identities.
		// A recorded pathname still proves use, and must never be ignored merely
		// because a device/inode comparison cannot establish equivalence.
		for _, path := range paths {
			escaped := strings.ReplaceAll(path, "\n", `\012`)
			if strings.HasSuffix(line, " "+escaped) ||
				strings.HasSuffix(line, " "+escaped+" (deleted)") {
				return fmt.Errorf("artifact still mapped by engine process")
			}
		}
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		dev := strings.Split(f[3], ":")
		if len(dev) != 2 {
			return fmt.Errorf("unrecognized mapping identity")
		}
		major, e1 := strconv.ParseUint(dev[0], 16, 32)
		minor, e2 := strconv.ParseUint(dev[1], 16, 32)
		if e1 != nil || e2 != nil {
			return fmt.Errorf("unrecognized mapping device")
		}
		if inodes[fmt.Sprintf("%x:%x:%s", major, minor, f[4])] {
			return fmt.Errorf("artifact still mapped by engine process")
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return err
	}
	for _, fd := range fds {
		st, err := os.Stat(filepath.Join("/proc/self/fd", fd.Name()))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		s := st.Sys().(*syscall.Stat_t)
		key := fmt.Sprintf(
			"%x:%x:%d",
			unix.Major(uint64(s.Dev)),
			unix.Minor(uint64(s.Dev)),
			s.Ino,
		)
		if inodes[key] {
			return fmt.Errorf("artifact still open by engine process")
		}
	}
	return nil
}

func (g *gcGraph) removeFiles(path string, files []string, dir bool) error {
	// os.Root constrains removal even if an unexpected external rename occurs.
	root, err := os.OpenRoot(g.e.opts.StateDir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	for _, file := range files {
		if err = g.ctx.Err(); err != nil {
			return err
		}
		rel, _ := filepath.Rel(g.e.opts.StateDir, file)
		if err = root.Remove(rel); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err = syncDir(filepath.Dir(file)); err != nil {
			return err
		}
		if g.e.opts.Fault != nil {
			if err = g.e.opts.Fault("gc_unlinked"); err != nil {
				return err
			}
		}
	}
	if dir {
		var dirs []string
		if err = filepath.WalkDir(
			path,
			func(p string, d os.DirEntry, err error) error {
				if os.IsNotExist(err) {
					return nil
				}
				if err != nil {
					return err
				}
				if !d.IsDir() {
					return fmt.Errorf(
						"unexpected artifact appeared during GC: %s",
						p,
					)
				}
				dirs = append(dirs, p)
				return nil
			},
		); err != nil {
			return err
		}
		for _, dir := range slices.Backward(dirs) {
			rel, _ := filepath.Rel(g.e.opts.StateDir, dir)
			if err = root.Remove(rel); err != nil && !os.IsNotExist(err) {
				return err
			}
			if err = syncDir(filepath.Dir(dir)); err != nil {
				return err
			}
		}
	}
	return syncDir(filepath.Dir(path))
}
