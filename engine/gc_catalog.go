package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

func (g *gcGraph) catalog() error {
	rows, err := g.e.db.QueryContext(g.ctx, "SELECT key,value FROM meta")
	if err != nil {
		return err
	}
	for rows.Next() {
		var key, body string
		if err = rows.Scan(&key, &body); err != nil {
			break
		}
		if strings.HasPrefix(key, "image:") {
			var m ImageManifest
			if err = gcDecode([]byte(body), &m); err != nil {
				break
			}
			if key != "image:"+m.Digest {
				err = fmt.Errorf("GC catalog identity mismatch")
				break
			}
			g.images[m.Digest] = m
		} else {
			switch key {
			case "identity", "epoch", "schema", "backup_point":
			case "recovery_note":
				err = fmt.Errorf(
					"GC fenced by restored history; ownership requires manual review",
				)
			default:
				err = fmt.Errorf("GC unknown metadata root: %s", key)
			}
			if err != nil {
				break
			}
		}
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	for digest, m := range g.images {
		if err = g.verifyImage(
			m,
			filepath.Join(
				g.e.opts.StateDir,
				"images",
				strings.TrimPrefix(digest, "sha256:"),
			),
		); err != nil {
			return err
		}
	}
	for _, v := range g.vms {
		if _, ok := g.images[v.VM.Config.Image]; !ok {
			return fmt.Errorf("GC VM image catalog root missing: %s", v.VM.ID)
		}
	}
	for _, s := range g.snapshots {
		if _, ok := g.images[s.Snapshot.Config.Image]; !ok {
			return fmt.Errorf(
				"GC snapshot image catalog root missing: %s",
				s.Snapshot.ID,
			)
		}
	}
	for _, o := range g.ops {
		if !terminal(o.Operation.Status) && o.Operation.Request.Config != nil {
			if _, ok := g.images[o.Operation.Request.Config.Image]; !ok {
				return fmt.Errorf("GC pending operation image root missing")
			}
		}
	}
	return nil
}

func (g *gcGraph) verifyImage(m ImageManifest, dir string) error {
	if err := g.image(m); err != nil {
		return err
	}
	var disk ImageManifest
	if err := g.path(filepath.Join(dir, "image.json"), false); err != nil {
		return err
	}
	if err := gcReadJSON(
		filepath.Join(dir, "image.json"),
		&disk,
	); err != nil ||
		!reflect.DeepEqual(m, disk) {
		return fmt.Errorf(
			"GC image manifest root missing/corrupt: %s: %v",
			dir,
			err,
		)
	}
	required := []string{"kernel", "initramfs", "root.erofs"}
	if m.Layout == LayeredEROFSLayout {
		required[2] = "fsmeta.erofs"
	}
	for _, name := range required {
		if _, err := shaName(m.Files[name]); err != nil {
			return fmt.Errorf("GC image root missing %s", name)
		}
	}
	for rel, hash := range m.Files {
		if err := g.immutable(dir, rel, hash); err != nil {
			return err
		}
	}
	allowed := map[string]string{"image.json": ""}
	for rel := range m.Files {
		allowed[rel] = ""
	}
	if _, err := g.ownedFiles(dir, allowed, true); err != nil {
		return err
	}
	if m.Layout == LayeredEROFSLayout {
		var layers []string
		for _, a := range m.Artifacts[1:] {
			if err := g.verifyCache(m.Builder, a.DiffID); err != nil {
				return err
			}
			if err := g.immutable(
				g.e.opts.StateDir,
				a.Path,
				a.SHA256,
			); err != nil {
				return err
			}
			if err := g.immutable(
				g.e.opts.StateDir,
				filepath.Join(filepath.Dir(a.Path), "layer.json"),
				a.CacheSHA256,
			); err != nil {
				return err
			}
			layers = append(layers, filepath.Join(g.e.opts.StateDir, a.Path))
		}
		return verifyImageDeviceTable(
			filepath.Join(dir, "fsmeta.erofs"),
			layers,
			m.DiffIDs,
			m.sharedLayered(),
		)
	}
	return nil
}

func (g *gcGraph) verifyCache(builder, diff string) error {
	for _, digest := range []string{builder, diff} {
		if _, err := shaName(digest); err != nil {
			return err
		}
	}
	dir := filepath.Dir(layerRelativePath(builder, diff))
	path := filepath.Join(g.e.opts.StateDir, dir)
	var c layerCache
	if err := g.path(filepath.Join(path, "layer.json"), false); err != nil {
		return err
	}
	if err := gcReadJSON(filepath.Join(path, "layer.json"), &c); err != nil {
		return err
	}
	if c.Format != 1 ||
		(c.Profile != layerProfile && c.Profile != sharedLayerProfile) ||
		c.Builder != builder ||
		c.DiffID != diff ||
		c.Bytes < 0 ||
		c.Entries < 0 {
		return fmt.Errorf("GC unknown layer ownership receipt: %s", path)
	}
	if _, err := shaName(c.SHA256); err != nil {
		return err
	}
	st, err := os.Stat(filepath.Join(path, "layer.json"))
	if err != nil || st.Mode().Perm()&0o222 != 0 {
		return fmt.Errorf(
			"GC cache ownership receipt is not immutable: %s",
			path,
		)
	}
	return g.immutable(
		g.e.opts.StateDir,
		filepath.Join(dir, "layer.erofs"),
		c.SHA256,
	)
}

func (g *gcGraph) importFiles(
	path string,
	opts ImportOptions,
) (map[string]string, error) {
	m := g.images[opts.Digest]
	materialization := opts.Materialization
	if materialization == "" {
		materialization = LayeredEROFSLayout
	}
	if materialization != m.Layout ||
		!g.reservations["image:"+opts.Digest[7:]] {
		return nil, fmt.Errorf(
			"completed import ownership/reservation mismatch",
		)
	}
	files := map[string]string{"intent.json": ""}
	input := filepath.Join(path, "input", "blobs", "sha256")
	if err := g.path(input, true); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(input)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if _, err = shaName("sha256:" + entry.Name()); err != nil {
			return nil, err
		}
		files[filepath.Join("input", "blobs", "sha256", entry.Name())] = "sha256:" + entry.Name()
	}
	out := filepath.Join(path, "output")
	if _, err = os.Lstat(out); err == nil {
		if err = g.verifyImage(m, out); err != nil {
			return nil, err
		}
		files["output/image.json"] = ""
		for rel, hash := range m.Files {
			files[filepath.Join("output", rel)] = hash
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	return files, nil
}
