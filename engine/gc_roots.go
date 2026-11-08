package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

type gcVersion struct {
	owner    string
	resource Resource
	manifest *Manifest
	path     string
}

type gcGraph struct {
	e            *Engine
	ctx          context.Context
	vms          map[string]vmRecord
	ops          map[string]opRecord
	snapshots    map[string]snapshotRecord
	images       map[string]ImageManifest
	backing      map[string]string
	roots        map[string]bool
	layers       map[string]bool
	versions     map[string]gcVersion
	resources    []Resource
	imports      map[string]ImportOptions
	reservations map[string]bool
	deletions    map[string]gcDeletion
}

func gcDecode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}

func gcReadJSON(path string, v any) error {
	if err := regular(path); err != nil {
		return err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return gcDecode(b, v)
}

func gcID(id, prefix string) bool {
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+32 {
		return false
	}
	_, err := hex.DecodeString(id[len(prefix):])
	return err == nil
}

func (g *gcGraph) records(
	table string,
	visit func(string, []byte) error,
) error {
	rows, err := g.e.db.QueryContext(g.ctx, "SELECT id,body FROM "+table)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		var b []byte
		if err = rows.Scan(&id, &b); err != nil {
			return err
		}
		if err = visit(id, b); err != nil {
			return fmt.Errorf("GC %s %s: %w", table, id, err)
		}
	}
	return rows.Err()
}

func (g *gcGraph) load() error {
	if err := g.loadDeletions(); err != nil {
		return err
	}
	if err := g.records("operations", func(id string, b []byte) error {
		var o opRecord
		if err := gcDecode(b, &o); err != nil {
			return err
		}
		if !gcID(id, "op-") || o.Operation.ID != id || o.Operation.Key == "" {
			return fmt.Errorf("invalid operation identity")
		}
		switch o.Operation.Status {
		case "pending", "running", "succeeded", "failed":
		default:
			return fmt.Errorf("unknown operation status")
		}
		switch o.Operation.Request.Action {
		case "create",
			"start",
			"stop",
			"snapshot",
			"restore",
			"clone",
			"delete",
			"delete_snapshot",
			"network",
			"network_policy":
		default:
			return fmt.Errorf("unknown operation action")
		}
		g.ops[id] = o
		return nil
	}); err != nil {
		return err
	}
	// Check indexed status and idempotency columns too: GC must not turn malformed
	// JSON defaults or a disagreeing SQL status into evidence of completion.
	rows, err := g.e.db.QueryContext(
		g.ctx,
		"SELECT id,key,hash,status FROM operations",
	)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id, key, hash, status string
		if err = rows.Scan(&id, &key, &hash, &status); err != nil {
			break
		}
		o := g.ops[id]
		sum := sha256.Sum256(encode(o.Operation.Request))
		if key != o.Operation.Key || status != o.Operation.Status ||
			hash != hex.EncodeToString(sum[:]) {
			err = fmt.Errorf(
				"GC operation index disagrees with journal: %s",
				id,
			)
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	if err = g.records("vms", func(id string, b []byte) error {
		var v vmRecord
		if err := gcDecode(b, &v); err != nil {
			return err
		}
		if !gcID(id, "vm-") || v.VM.ID != id || v.Resource.VMID != id ||
			!reflect.DeepEqual(v.VM.Config, v.Resource.Config) {
			return fmt.Errorf("invalid VM root")
		}
		if err := g.resource(v.Resource); err != nil {
			return err
		}
		switch v.VM.Phase {
		case "creating", "created", "running", "stopped":
		default:
			return fmt.Errorf("unknown VM phase")
		}
		if v.VM.Instance != v.Resource.Instance {
			return fmt.Errorf("VM running instance root mismatch")
		}
		if v.VM.OperationID != "" {
			if o, ok := g.ops[v.VM.OperationID]; !ok || o.Operation.VMID != id {
				return fmt.Errorf("VM operation root missing")
			}
		}
		if v.VM.Phase != "creating" {
			if err := g.path(v.Resource.Disk, false); err != nil {
				return err
			}
		}
		g.vms[id] = v
		g.rootResource(v.Resource)
		return nil
	}); err != nil {
		return err
	}
	for id, o := range g.ops {
		live := !terminal(o.Operation.Status) || o.Journal.ResumeRequired ||
			o.Journal.Lease || o.Journal.RestoreBlocked && !o.Journal.RestoreCleaned || o.Journal.BlockedRestoreID != "" && g.vms[o.Operation.VMID].VM.ID != ""
		if o.Operation.Error != nil &&
			o.Operation.Error.Code == api.ManualRecovery &&
			!o.Journal.RestoreCleaned &&
			!g.restoreDeleteCleaned(o) {
			return fmt.Errorf("GC operation requires manual recovery: %s", id)
		}
		if v, ok := g.vms[o.Operation.VMID]; ok && v.VM.OperationID == id {
			live = true
		}
		for _, r := range []*Resource{o.Journal.Resource, o.Journal.Old} {
			if r == nil {
				continue
			}
			if r.VMID != o.Operation.VMID {
				return fmt.Errorf("GC journal VM ownership mismatch: %s", id)
			}
			if err = g.resource(*r); err != nil {
				return err
			}
			if live {
				g.rootResource(*r)
			}
		}
		if live && o.Journal.Manifest != nil {
			if err = g.manifest(*o.Journal.Manifest); err != nil {
				return err
			}
		}
		if live && o.Operation.Request.Config != nil {
			if err = checkConfig(o.Operation.Request.Config); err != nil {
				return err
			}
		}
		if !terminal(o.Operation.Status) && o.Operation.VMID != "" {
			if _, ok := g.vms[o.Operation.VMID]; !ok &&
				o.Operation.Request.Action != "delete" {
				return fmt.Errorf(
					"GC pending operation VM root missing: %s",
					id,
				)
			}
		}
		if o.Operation.Request.Action == "create" {
			if v, ok := g.vms[o.Operation.VMID]; ok {
				r := v.Resource
				r.Disk = filepath.Join(r.Dir, "disks", id+".raw")
				r.RestoreDir = ""
				g.versions[r.Disk] = gcVersion{id, r, nil, r.Disk}
			}
		}
		if (o.Operation.Request.Action == "restore" || o.Operation.Request.Action == "clone") &&
			o.Journal.Resource != nil {
			r := *o.Journal.Resource
			p := filepath.Join(r.Dir, "versions", id)
			if r.Disk != filepath.Join(p, "disk.raw") ||
				r.RestoreDir != filepath.Join(p, "restore") ||
				o.Journal.Manifest == nil {
				return fmt.Errorf("GC version ownership mismatch: %s", id)
			}
			g.versions[p] = gcVersion{id, r, o.Journal.Manifest, p}
		}
	}
	if err = g.records("snapshots", func(id string, b []byte) error {
		var s snapshotRecord
		if err := gcDecode(b, &s); err != nil {
			return err
		}
		if !gcID(id, "snap-") || s.Snapshot.ID != id ||
			s.Dir != filepath.Join(g.e.opts.StateDir, "snapshots", id) ||
			checkConfig(&s.Snapshot.Config) != nil {
			return fmt.Errorf("invalid snapshot root")
		}
		g.snapshots[id] = s
		g.roots[s.Dir] = true // Tombstones retain roots until explicit deletion.
		if s.Snapshot.Published {
			var m, disk Manifest
			if err := gcDecode(s.Snapshot.Manifest, &m); err != nil {
				return err
			}
			if err := gcReadJSON(
				filepath.Join(s.Dir, "manifest.json"),
				&disk,
			); err != nil {
				return err
			}
			if !bytes.Equal(encode(m), encode(disk)) ||
				!reflect.DeepEqual(m.Config, s.Snapshot.Config) {
				return fmt.Errorf("snapshot manifest root mismatch")
			}
			if err := g.manifest(m); err != nil {
				return err
			}
			for rel := range m.Files {
				if err := g.path(filepath.Join(s.Dir, rel), false); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err = g.relations(); err != nil {
		return err
	}
	if err = g.catalog(); err != nil {
		return err
	}
	if err = g.inventory(); err != nil {
		return err
	}
	owners := map[string]bool{}
	for id := range g.vms {
		owners[id] = true
	}
	for id := range g.snapshots {
		owners[id] = true
	}
	for digest := range g.images {
		owners["image:"+digest[7:]] = true
	}
	for _, v := range g.versions {
		if _, ok := g.vms[v.resource.VMID]; ok {
			owners[v.resource.VMID+":"+v.owner] = true
		}
	}
	for owner := range g.reservations {
		if !owners[owner] {
			return fmt.Errorf("GC unknown reservation owner: %s", owner)
		}
	}
	return nil
}

// restoreDeleteCleaned derives internal responsibility from the original
// restore's cleanup transaction. Historical failed delete responses stay fixed.
func (g *gcGraph) restoreDeleteCleaned(o opRecord) bool {
	if o.Operation.Request.Action != "delete" ||
		o.Operation.Status != "failed" ||
		o.Operation.Request.VMID != o.Operation.VMID ||
		o.Operation.VMID == "" {
		return false
	}
	original, ok := g.ops[o.Journal.BlockedRestoreID]
	if !ok || original.Operation.ID == o.Operation.ID ||
		original.Operation.VMID != o.Operation.VMID || original.Operation.Status != "failed" ||
		(original.Operation.Request.Action != "restore" && original.Operation.Request.Action != "clone") ||
		!gcID(
			original.Operation.SnapshotID,
			"snap-",
		) || original.Operation.Request.SnapshotID != original.Operation.SnapshotID ||
		!original.Journal.RestoreBlocked || !original.Journal.RestoreCleaned || original.Journal.Lease {
		return false
	}
	if _, exists := g.vms[o.Operation.VMID]; exists {
		return false
	}
	resource, receipt := original.Journal.Resource, original.Journal.RestoreDeletion
	return resource != nil && resource.VMID == o.Operation.VMID &&
		receipt != nil && receipt.Format == 1 && receipt.Dir &&
		receipt.Path == filepath.Join(
			g.e.opts.StateDir,
			"vms",
			o.Operation.VMID,
		)
}

func (g *gcGraph) rootResource(r Resource) {
	g.roots[r.Disk] = true
	if r.RestoreDir != "" {
		g.roots[filepath.Dir(r.RestoreDir)] = true
	}
	g.resources = append(g.resources, r)
}

func (g *gcGraph) resource(r Resource) error {
	if !gcID(r.VMID, "vm-") ||
		r.Dir != filepath.Join(g.e.opts.StateDir, "vms", r.VMID) ||
		checkConfig(&r.Config) != nil ||
		r.ImageDir != filepath.Join(
			g.e.opts.StateDir,
			"images",
			r.Config.Image[7:],
		) {
		return fmt.Errorf("GC invalid resource ownership")
	}
	if !gcID(r.NetworkToken, "net-") || r.Slot < 1 || r.Slot >= 32768 {
		return fmt.Errorf("GC invalid network ownership")
	}
	if r.Instance == "" && r.RunDir != "" ||
		r.Instance != "" &&
			(!gcID(r.Instance, "run-") || r.RunDir != filepath.Join(r.Dir, "runs", r.Instance)) {
		return fmt.Errorf("GC invalid running instance path")
	}
	rel, err := filepath.Rel(r.Dir, r.Disk)
	parts := strings.Split(rel, string(filepath.Separator))
	if err != nil ||
		((len(parts) != 2 || parts[0] != "disks" || !strings.HasSuffix(parts[1], ".raw") || !gcID(strings.TrimSuffix(parts[1], ".raw"), "op-")) && (len(parts) != 3 || parts[0] != "versions" || !gcID(parts[1], "op-") || parts[2] != "disk.raw")) {
		return fmt.Errorf("GC invalid disk version path")
	}
	if r.RestoreDir != "" &&
		r.RestoreDir != filepath.Join(filepath.Dir(r.Disk), "restore") {
		return fmt.Errorf("GC invalid backing path")
	}
	return nil
}

func (g *gcGraph) manifest(m Manifest) error {
	if m.Format != 1 || m.MemoryMode != "CopyOnWrite" ||
		checkConfig(&m.Config) != nil {
		return fmt.Errorf("GC unknown snapshot manifest")
	}
	for _, name := range []string{"ch/config.json", "ch/state.json", "ch/memory-ranges", "disk.raw"} {
		if _, err := shaName(m.Files[name]); err != nil {
			return fmt.Errorf("GC snapshot root missing %s", name)
		}
	}
	for rel, hash := range m.Files {
		if filepath.IsAbs(rel) || filepath.Clean(rel) != rel ||
			strings.HasPrefix(rel, "../") ||
			strings.Contains(rel, "\\") {
			return fmt.Errorf("GC invalid manifest path")
		}
		if _, err := shaName(hash); err != nil {
			return err
		}
	}
	var image ImageManifest
	if err := gcDecode(m.Image, &image); err != nil {
		return err
	}
	if image.Digest != m.Config.Image {
		return fmt.Errorf("GC embedded image root mismatch")
	}
	return g.image(image)
}

func (g *gcGraph) image(m ImageManifest) error {
	if m.Architecture != runtime.GOARCH {
		return fmt.Errorf("GC unknown image architecture")
	}
	if m.Layout == "merged-erofs-v1" {
		if len(m.Files) != 3 {
			return fmt.Errorf("GC unknown legacy image files")
		}
		for _, name := range []string{"kernel", "initramfs", "root.erofs"} {
			if _, err := shaName(m.Files[name]); err != nil {
				return err
			}
		}
	}
	if _, err := shaName(m.Digest); err != nil {
		return err
	}
	if _, err := m.DataSerials(); err != nil {
		return err
	}
	if m.Layout == LayeredEROFSLayout {
		for _, a := range m.Artifacts[1:] {
			g.layers[filepath.Dir(a.Path)] = true
		}
	}
	return nil
}

func gcSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

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
