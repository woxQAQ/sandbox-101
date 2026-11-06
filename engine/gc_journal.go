package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// The deletion intent retains exact inode ownership across partial unlinks.
// It is committed and checkpointed before touching files; both it and backing
// registrations survive until paths and parent directory entries are synced.
type gcIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Mode   uint32 `json:"mode"`
}

type gcDeletion struct {
	Format int                   `json:"format"`
	Path   string                `json:"path"`
	Dir    bool                  `json:"directory"`
	Nodes  map[string]gcIdentity `json:"nodes"`
}

func gcNode(st os.FileInfo) gcIdentity {
	s := st.Sys().(*syscall.Stat_t)
	return gcIdentity{uint64(s.Dev), s.Ino, uint32(st.Mode())}
}

func (g *gcGraph) loadDeletions() error {
	rows, err := g.e.db.QueryContext(g.ctx, "SELECT path,body FROM gc_deletions")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var path string
		var b []byte
		var d gcDeletion
		if err = rows.Scan(&path, &b); err != nil {
			return err
		}
		if err = gcDecode(b, &d); err != nil {
			return err
		}
		rel, err := filepath.Rel(g.e.opts.StateDir, path)
		parts := strings.Split(rel, string(filepath.Separator))
		valid := len(parts) == 2 && parts[0] == "imports"
		if valid {
			_, err = shaName("sha256:" + parts[1])
			valid = err == nil && d.Dir
		}
		if len(parts) == 3 && parts[0] == "layer-cache" {
			_, a := shaName("sha256:" + parts[1])
			_, b := shaName("sha256:" + parts[2])
			valid = a == nil && b == nil && d.Dir
		}
		if len(parts) == 4 && parts[0] == "vms" && gcID(parts[1], "vm-") {
			valid = parts[2] == "versions" && gcID(parts[3], "op-") && d.Dir || parts[2] == "capture" && gcID(parts[3], "op-") && d.Dir || parts[2] == "disks" && gcID(strings.TrimSuffix(strings.TrimSuffix(parts[3], ".pending"), ".raw"), "op-") && !d.Dir
		}
		if !valid || d.Path != path || filepath.Clean(path) != path || d.Format != 1 || len(d.Nodes) == 0 {
			return fmt.Errorf("GC corrupt deletion ownership: %s", path)
		}
		for rel, n := range d.Nodes {
			if filepath.IsAbs(rel) || filepath.Clean(rel) != rel || strings.HasPrefix(rel, "../") || rel == ".." || strings.Contains(rel, "\\") || n.Inode == 0 || !(os.FileMode(n.Mode).IsDir() || os.FileMode(n.Mode).IsRegular()) {
				return fmt.Errorf("GC corrupt deletion inode record: %s", path)
			}
		}
		g.deletions[path] = d
	}
	return rows.Err()
}

func (g *gcGraph) deletion(path string, files []string, dir bool) (gcDeletion, error) {
	d := gcDeletion{Format: 1, Path: path, Dir: dir, Nodes: map[string]gcIdentity{}}
	if dir {
		err := filepath.WalkDir(path, func(p string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			st, err := entry.Info()
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(path, p)
			d.Nodes[rel] = gcNode(st)
			return nil
		})
		return d, err
	}
	for _, p := range files {
		st, err := os.Lstat(p)
		if err != nil {
			return d, err
		}
		d.Nodes["."] = gcNode(st)
	}
	return d, nil
}

func (g *gcGraph) resumeFiles(d gcDeletion) ([]string, error) {
	if _, err := os.Lstat(d.Path); os.IsNotExist(err) {
		return nil, nil
	}
	if err := g.path(d.Path, d.Dir); err != nil {
		return nil, err
	}
	paths := []string{}
	err := filepath.WalkDir(d.Path, func(p string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = g.ctx.Err(); err != nil {
			return err
		}
		rel, _ := filepath.Rel(d.Path, p)
		st, err := entry.Info()
		if err != nil {
			return err
		}
		if want, ok := d.Nodes[rel]; !ok || gcNode(st) != want {
			return fmt.Errorf("GC deletion inode replaced or unknown: %s", p)
		}
		if !entry.IsDir() {
			paths = append(paths, p)
		}
		return nil
	})
	return paths, err
}

func (g *gcGraph) journalDeletion(d gcDeletion) error {
	if _, err := g.e.db.ExecContext(g.ctx, "INSERT INTO gc_deletions VALUES(?,?)", d.Path, encode(d)); err != nil {
		return err
	}
	g.deletions[d.Path] = d
	if err := g.e.checkpoint(); err != nil {
		return err
	}
	if g.e.opts.Fault != nil {
		return g.e.opts.Fault("gc_delete_intent")
	}
	return nil
}

func (g *gcGraph) finishDeletion(path string) error {
	if _, err := g.e.db.ExecContext(g.ctx, "DELETE FROM gc_deletions WHERE path=?", path); err != nil {
		return err
	}
	delete(g.deletions, path)
	return g.e.checkpoint()
}
