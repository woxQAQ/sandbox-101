package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func (g *gcGraph) relations() error {
	rows, err := g.e.db.QueryContext(
		g.ctx,
		"SELECT snapshot,operation FROM leases",
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var snapshot, operation string
		if err = rows.Scan(&snapshot, &operation); err != nil {
			break
		}
		o, ok := g.ops[operation]
		if _, exists := g.snapshots[snapshot]; !ok || !exists ||
			!o.Journal.Lease ||
			o.Operation.Request.SnapshotID != snapshot {
			err = fmt.Errorf(
				"GC lease root missing or inconsistent: %s",
				operation,
			)
			break
		}
		g.roots[g.snapshots[snapshot].Dir] = true
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	// A missing lease row is also uncertainty; an operation's boolean alone is
	// not a substitute for the durable admission transaction.
	for id, o := range g.ops {
		if o.Journal.Lease {
			var snapshot string
			if err = g.e.db.QueryRowContext(g.ctx, "SELECT snapshot FROM leases WHERE operation=?", id).
				Scan(&snapshot); err != nil ||
				snapshot != o.Operation.Request.SnapshotID {
				return fmt.Errorf("GC journal lease root missing: %s", id)
			}
		}
	}
	rows, err = g.e.db.QueryContext(g.ctx, "SELECT vm,path,digest FROM backing")
	if err != nil {
		return err
	}
	for rows.Next() {
		var vm, path, digest string
		if err = rows.Scan(&vm, &path, &digest); err != nil {
			break
		}
		v, ok := g.versions[filepath.Dir(filepath.Dir(path))]
		if _, exists := g.vms[vm]; !ok || !exists || v.resource.VMID != vm ||
			path != filepath.Join(v.resource.RestoreDir, "memory-ranges") ||
			v.manifest == nil ||
			v.manifest.Files["ch/memory-ranges"] != digest {
			err = fmt.Errorf("GC backing ownership missing: %s", path)
			break
		}
		g.backing[path] = digest
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	for _, r := range g.resources {
		if r.RestoreDir != "" {
			path := filepath.Join(r.RestoreDir, "memory-ranges")
			// Before handoff the source lease protects a candidate whose backing
			// registration does not exist yet. Every current VM backing must exist.
			if v, ok := g.vms[r.VMID]; ok &&
				v.Resource.RestoreDir == r.RestoreDir {
				if g.backing[path] == "" {
					return fmt.Errorf(
						"GC current VM backing root missing: %s",
						path,
					)
				}
				if err = g.path(path, false); err != nil {
					return err
				}
			}
		}
	}
	rows, err = g.e.db.QueryContext(g.ctx, "SELECT owner FROM reservations")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var owner string
		if err = rows.Scan(&owner); err != nil {
			return err
		}
		g.reservations[owner] = true
	}
	return rows.Err()
}

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

func (g *gcGraph) idle(vm string) error {
	v, ok := g.vms[vm]
	if !ok {
		return fmt.Errorf("VM ownership no longer available")
	}
	resources := []Resource{v.Resource}
	for _, o := range g.ops {
		if o.Operation.VMID != vm {
			continue
		}
		if !terminal(o.Operation.Status) || o.Journal.ResumeRequired ||
			o.Journal.Lease || o.Journal.RestoreBlocked && !o.Journal.RestoreCleaned ||
			v.VM.OperationID == o.Operation.ID {
			return fmt.Errorf(
				"VM has pending or unresolved operation %s",
				o.Operation.ID,
			)
		}
		for _, r := range []*Resource{o.Journal.Old, o.Journal.Resource} {
			if r != nil {
				resources = append(resources, *r)
			}
		}
	}
	seen := map[string]bool{}
	for _, r := range resources {
		if seen[r.Instance] {
			continue
		}
		seen[r.Instance] = true
		obs, err := g.e.opts.Backend.Inspect(g.ctx, r)
		if err != nil {
			return fmt.Errorf(
				"instance %s ownership uncertain: %w",
				r.Instance,
				err,
			)
		}
		if obs.Alive {
			return fmt.Errorf(
				"instance %s still alive; retained until readers exit",
				r.Instance,
			)
		}
	}
	return nil
}

func (g *gcGraph) reachable(path string) bool {
	for root := range g.roots {
		if path == root ||
			strings.HasPrefix(root, path+string(filepath.Separator)) ||
			strings.HasPrefix(path, root+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func (g *gcGraph) sweep(report *GCReport, opts GCOptions) error {
	apply := func(path string, files map[string]string, dir bool, retire func() error) error {
		if len(report.Candidates) >= opts.MaxArtifacts {
			report.Complete = false
			return nil
		}
		if err := g.ctx.Err(); err != nil {
			return err
		}
		deletion, pending := g.deletions[path]
		if _, err := os.Lstat(path); os.IsNotExist(err) && !pending {
			if retire != nil && !opts.DryRun {
				return retire()
			}
			return nil
		}
		var paths []string
		var err error
		if pending {
			paths, err = g.resumeFiles(deletion)
		} else {
			paths, err = g.ownedFiles(path, files, dir)
		}
		if err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{path, err.Error()},
			)
			return nil
		}
		if err = gcLocalReaders(paths); err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{path, err.Error()},
			)
			return nil
		}
		report.Candidates = append(report.Candidates, path)
		if opts.DryRun {
			return nil
		}
		if !pending {
			deletion, err = g.deletion(path, paths, dir)
			if err != nil {
				return err
			}
			if err = g.journalDeletion(deletion); err != nil {
				return err
			}
		}
		if err = g.removeFiles(path, paths, dir); err != nil {
			return err
		}
		if retire != nil {
			if err = retire(); err != nil {
				return err
			}
		}
		if err = g.finishDeletion(path); err != nil {
			return err
		}
		report.Removed = append(report.Removed, path)
		return nil
	}
	resumed := map[string]bool{}
	for _, path := range gcSortedKeys(g.deletions) {
		d := g.deletions[path]
		rel, _ := filepath.Rel(g.e.opts.StateDir, path)
		parts := strings.Split(rel, string(filepath.Separator))
		if g.reachable(path) || g.layers[rel] {
			report.Retained = append(
				report.Retained,
				GCIssue{path, "deletion intent has a durable reader root"},
			)
			continue
		}
		var retire func() error
		switch parts[0] {
		case "vms":
			if _, present := g.vms[parts[1]]; !present {
				deleted := false
				for _, o := range g.ops {
					deleted = deleted ||
						o.Operation.VMID == parts[1] && o.Operation.Request.Action == "delete" &&
							o.Operation.Status == "succeeded"
				}
				if _, err := os.Lstat(path); deleted && os.IsNotExist(err) {
					if !opts.DryRun {
						if err = g.finishDeletion(path); err != nil {
							return err
						}
					}
					resumed[path] = true
					continue
				}
			}
			if err := g.idle(parts[1]); err != nil {
				report.Retained = append(
					report.Retained,
					GCIssue{path, err.Error()},
				)
				continue
			}
			if parts[2] != "capture" {
				v, ok := g.versions[strings.TrimSuffix(path, ".pending")]
				if !ok {
					return fmt.Errorf(
						"GC deletion lost version ownership: %s",
						path,
					)
				}
				retire = func() error { return g.retireVersion(v) }
			} else {
				o, ok := g.ops[parts[3]]
				if !ok || o.Operation.VMID != parts[1] ||
					o.Operation.Request.Action != "snapshot" ||
					!terminal(o.Operation.Status) ||
					o.Journal.ResumeRequired {
					return fmt.Errorf(
						"GC deletion lost capture ownership: %s",
						path,
					)
				}
				boundary := o.Journal.CaptureBoundary
				if boundary == nil || boundary.Path != path ||
					d.Nodes["."] != boundary.Parent {
					report.Retained = append(
						report.Retained,
						GCIssue{
							path,
							"capture deletion boundary unconfirmed; manual recovery required",
						},
					)
					continue
				}
				if stage, present := d.Nodes["publication"]; present &&
					stage != boundary.Publication {
					report.Retained = append(
						report.Retained,
						GCIssue{
							path,
							"capture deletion staging identity unconfirmed",
						},
					)
					continue
				}
			}
		case "imports":
			if _, ok := g.images["sha256:"+parts[1]]; !ok ||
				!g.reservations["image:"+parts[1]] {
				return fmt.Errorf("GC deletion lost import ownership: %s", path)
			}
		case "layer-cache":
		}
		if err := apply(path, nil, d.Dir, retire); err != nil {
			return err
		}
		resumed[path] = true
	}
	for _, path := range gcSortedKeys(g.versions) {
		if resumed[path] {
			continue
		}
		v := g.versions[path]
		if g.reachable(path) {
			continue
		}
		if _, ok := g.vms[v.resource.VMID]; !ok {
			continue // VM deletion owns its directory; never recreate lost authority.
		}
		if err := g.idle(v.resource.VMID); err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{path, err.Error()},
			)
			continue
		}
		files := map[string]string{
			"disk.raw":                    "",
			"disk.raw.pending":            "",
			"restore/config.json":         "",
			"restore/config.json.pending": "",
			"restore/state.json.pending":  "",
			"restore/state.json":          "",
			"restore/memory-ranges":       "",
		}
		dir := v.manifest != nil
		if dir {
			for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
				files["restore/"+name] = v.manifest.Files["ch/"+name]
			}
		} else {
			files = map[string]string{"": ""}
		}
		if err := apply(
			path,
			files,
			dir,
			func() error { return g.retireVersion(v) },
		); err != nil {
			return err
		}
		if !dir {
			if err := apply(
				path+".pending",
				map[string]string{"": ""},
				false,
				nil,
			); err != nil {
				return err
			}
		}
	}
	for _, id := range gcSortedKeys(g.ops) {
		o := g.ops[id]
		if o.Operation.Request.Action != "snapshot" ||
			!terminal(o.Operation.Status) ||
			o.Journal.ResumeRequired ||
			o.Journal.Resource == nil {
			continue
		}
		path := filepath.Join(o.Journal.Resource.Dir, "capture", id)
		if _, err := os.Lstat(path); os.IsNotExist(err) {
			continue
		}
		if o.Journal.CaptureBoundary == nil {
			report.Retained = append(
				report.Retained,
				GCIssue{
					path,
					"legacy capture ownership requires manual recovery",
				},
			)
			continue
		}
		st, statErr := os.Lstat(path)
		if statErr != nil || o.Journal.CaptureBoundary.Path != path ||
			gcNode(st) != o.Journal.CaptureBoundary.Parent {
			report.Retained = append(
				report.Retained,
				GCIssue{path, "capture parent identity changed"},
			)
			continue
		}
		if stage, x := os.Lstat(
			filepath.Join(path, "publication"),
		); x == nil &&
			gcNode(stage) != o.Journal.CaptureBoundary.Publication {
			report.Retained = append(
				report.Retained,
				GCIssue{path, "publication staging identity changed"},
			)
			continue
		} else if x != nil &&
			!os.IsNotExist(x) {
			return x
		}
		if err := g.idle(o.Operation.VMID); err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{path, err.Error()},
			)
			continue
		}
		if err := apply(
			path,
			map[string]string{
				"ch/config.json":               "",
				"ch/state.json":                "",
				"ch/memory-ranges":             "",
				"publication/manifest.json":    "",
				"publication/disk.raw":         "",
				"publication/ch/config.json":   "",
				"publication/ch/state.json":    "",
				"publication/ch/memory-ranges": "",
			},
			true,
			nil,
		); err != nil {
			return err
		}
	}
	for _, path := range gcSortedKeys(g.imports) {
		opts := g.imports[path]
		files, err := g.importFiles(path, opts)
		if err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{path, err.Error()},
			)
			continue
		}
		if err = apply(path, files, true, nil); err != nil {
			return err
		}
	}
	cache := filepath.Join(g.e.opts.StateDir, "layer-cache")
	if _, err := os.Lstat(cache); os.IsNotExist(err) {
		return nil
	}
	if err := g.path(cache, true); err != nil {
		return err
	}
	builders, err := os.ReadDir(cache)
	if err != nil {
		return err
	}
	for _, builder := range builders {
		parent := filepath.Join(cache, builder.Name())
		if !builder.IsDir() {
			report.Retained = append(
				report.Retained,
				GCIssue{parent, "unknown cache owner"},
			)
			continue
		}
		if _, err = shaName("sha256:" + builder.Name()); err != nil {
			report.Retained = append(
				report.Retained,
				GCIssue{parent, "unknown builder identity"},
			)
			continue
		}
		layers, err := os.ReadDir(parent)
		if err != nil {
			return err
		}
		for _, layer := range layers {
			path := filepath.Join(parent, layer.Name())
			rel, _ := filepath.Rel(g.e.opts.StateDir, path)
			if g.layers[rel] || resumed[path] {
				continue
			}
			if err = g.verifyCache(
				"sha256:"+builder.Name(),
				"sha256:"+layer.Name(),
			); err != nil {
				report.Retained = append(
					report.Retained,
					GCIssue{path, err.Error()},
				)
				continue
			}
			if err = apply(
				path,
				map[string]string{"layer.erofs": "", "layer.json": ""},
				true,
				nil,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *gcGraph) retireVersion(v gcVersion) error {
	if v.resource.RestoreDir == "" ||
		g.backing[filepath.Join(v.resource.RestoreDir, "memory-ranges")] == "" &&
			!g.reservations[v.resource.VMID+":"+v.owner] {
		return nil
	}
	tx, err := g.e.db.BeginTx(g.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if v.resource.RestoreDir != "" {
		if _, err = tx.ExecContext(
			g.ctx,
			"DELETE FROM backing WHERE vm=? AND path=?",
			v.resource.VMID,
			filepath.Join(v.resource.RestoreDir, "memory-ranges"),
		); err != nil {
			return err
		}
		if _, err = tx.ExecContext(
			g.ctx,
			"DELETE FROM reservations WHERE owner=?",
			v.resource.VMID+":"+v.owner,
		); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return g.e.checkpoint()
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
