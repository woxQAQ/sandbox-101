package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

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
