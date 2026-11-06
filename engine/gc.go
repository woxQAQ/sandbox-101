package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/woxQAQ/sandbox-engine/api"
)

// GCOptions bounds deletions, not retention. Age is never evidence of disuse.
// Callers should also set a context deadline; background passes have two seconds.
type GCOptions struct {
	DryRun       bool `json:"dry_run"`
	MaxArtifacts int  `json:"max_artifacts"`
}

type GCIssue struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type GCReport struct {
	StartedAt  time.Time `json:"started_at"`
	DryRun     bool      `json:"dry_run"`
	Complete   bool      `json:"complete"`
	Candidates []string  `json:"candidates"`
	Removed    []string  `json:"removed"`
	Retained   []GCIssue `json:"retained"`
}

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

func (e *Engine) startGC() {
	interval := e.opts.GCInterval
	if interval == 0 {
		interval = time.Minute
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(e.ctx, 2*time.Second)
				_, _ = e.CollectGarbage(ctx, GCOptions{MaxArtifacts: 16})
				cancel()
			}
		}
	}()
}

// LastGCReport returns the latest background or administrative result.
func (e *Engine) LastGCReport() GCReport {
	e.mu.Lock()
	defer e.mu.Unlock()
	r := e.lastGC
	r.Candidates = append([]string(nil), r.Candidates...)
	r.Removed = append([]string(nil), r.Removed...)
	r.Retained = append([]GCIssue(nil), r.Retained...)
	return r
}

// CollectGarbage holds the import guard and lifecycle locks until every unlink
// and metadata retirement completes. TryLock avoids waiting for lifecycle work
// (whose lock order is VM -> admission). A changing inventory causes a retry,
// never a sweep with an incomplete root set. Operation results are never pruned.
func (e *Engine) CollectGarbage(ctx context.Context, opts GCOptions) (report GCReport, err error) {
	report = GCReport{StartedAt: time.Now().UTC(), DryRun: opts.DryRun, Candidates: []string{}, Removed: []string{}, Retained: []GCIssue{}}
	defer func() {
		if err != nil {
			report.Retained = append(report.Retained, GCIssue{e.opts.StateDir, err.Error()})
		}
		e.mu.Lock()
		e.lastGC = report
		e.lastGC.Candidates = append([]string(nil), report.Candidates...)
		e.lastGC.Removed = append([]string(nil), report.Removed...)
		e.lastGC.Retained = append([]GCIssue(nil), report.Retained...)
		e.mu.Unlock()
	}()
	if opts.MaxArtifacts == 0 {
		opts.MaxArtifacts = 16
	}
	if opts.MaxArtifacts < 1 || opts.MaxArtifacts > 1024 {
		return report, api.Fail(api.Invalid, "GC batch must be 1..1024 artifacts")
	}
	if !e.artifacts.TryLock() {
		return report, api.Fail(api.Conflict, "image import or collection in progress")
	}
	defer e.artifacts.Unlock()
	e.mu.Lock()
	closed := e.closing
	e.mu.Unlock()
	if closed {
		return report, api.Fail(api.Conflict, "engine shutting down")
	}
	ids, err := e.gcLockIDs(ctx)
	if err != nil {
		return report, err
	}
	var locks []*sync.Mutex
	defer func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}()
	for _, id := range ids {
		l := e.vmLock(id)
		if !l.TryLock() {
			return report, api.Fail(api.Conflict, "lifecycle work in progress: %s", id)
		}
		locks = append(locks, l)
	}
	if !e.admission.TryLock() {
		return report, api.Fail(api.Conflict, "admission in progress")
	}
	defer e.admission.Unlock()
	current, err := e.gcLockIDs(ctx)
	if err != nil {
		return report, err
	}
	if !reflect.DeepEqual(ids, current) {
		return report, api.Fail(api.Conflict, "resource inventory changed; retry collection")
	}
	if err = e.gcFence(ctx); err != nil {
		return report, err
	}
	g := gcGraph{e: e, ctx: ctx, vms: map[string]vmRecord{}, ops: map[string]opRecord{}, snapshots: map[string]snapshotRecord{}, images: map[string]ImageManifest{}, backing: map[string]string{}, roots: map[string]bool{}, layers: map[string]bool{}, versions: map[string]gcVersion{}, imports: map[string]ImportOptions{}, reservations: map[string]bool{}, deletions: map[string]gcDeletion{}}
	if err = g.load(); err != nil {
		return report, err
	}
	report.Complete = true
	if err = g.sweep(&report, opts); err != nil {
		report.Complete = false
		return report, err
	}
	return report, nil
}

func (e *Engine) gcLockIDs(ctx context.Context) ([]string, error) {
	rows, err := e.db.QueryContext(ctx, "SELECT id FROM vms UNION SELECT 'snapshot:' || id FROM snapshots ORDER BY 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (e *Engine) gcFence(ctx context.Context) error {
	for _, name := range []string{"RECOVERY_REQUIRED", "RECOVERY_REVIEWED.json"} {
		if _, err := os.Lstat(filepath.Join(e.opts.StateDir, name)); !os.IsNotExist(err) {
			return api.Fail(api.ManualRecovery, "GC fenced by recovery evidence %s: %v", name, err)
		}
	}
	tables, err := e.db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		return err
	}
	for tables.Next() {
		var name string
		if err = tables.Scan(&name); err != nil {
			break
		}
		switch name {
		case "meta", "operations", "vms", "snapshots", "leases", "reservations", "backing", "gc_deletions":
		default:
			err = fmt.Errorf("GC unknown metadata table: %s", name)
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = tables.Err()
	}
	tables.Close()
	if err != nil {
		return err
	}
	var integrity, schema, store string
	var epoch int64
	if err := e.db.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&integrity); err != nil || integrity != "ok" {
		return api.Fail(api.ManualRecovery, "GC database integrity unavailable: %v %s", err, integrity)
	}
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='schema'").Scan(&schema); err != nil || schema != "1" {
		return api.Fail(api.ManualRecovery, "GC schema unavailable")
	}
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='identity'").Scan(&store); err != nil {
		return err
	}
	if err := e.db.QueryRowContext(ctx, "SELECT CAST(value AS INTEGER) FROM meta WHERE key='epoch'").Scan(&epoch); err != nil {
		return err
	}
	var head RecoveryPoint
	if err := gcReadJSON(filepath.Join(e.opts.StateDir, "HEAD.json"), &head); err != nil || head.Store != store || epoch < head.Epoch {
		return api.Fail(api.ManualRecovery, "GC durable HEAD missing, corrupt or ahead of metadata: %v", err)
	}
	return nil
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

func (g *gcGraph) records(table string, visit func(string, []byte) error) error {
	rows, err := g.e.db.QueryContext(g.ctx, "SELECT id,body FROM "+table)
	if err != nil {
		return err
	}
	defer rows.Close()
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
		case "create", "start", "stop", "snapshot", "restore", "clone", "delete", "delete_snapshot", "network", "network_policy":
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
	rows, err := g.e.db.QueryContext(g.ctx, "SELECT id,key,hash,status FROM operations")
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
		if key != o.Operation.Key || status != o.Operation.Status || hash != hex.EncodeToString(sum[:]) {
			err = fmt.Errorf("GC operation index disagrees with journal: %s", id)
			break
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return err
	}
	if err = g.records("vms", func(id string, b []byte) error {
		var v vmRecord
		if err := gcDecode(b, &v); err != nil {
			return err
		}
		if !gcID(id, "vm-") || v.VM.ID != id || v.Resource.VMID != id || !reflect.DeepEqual(v.VM.Config, v.Resource.Config) {
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
		live := !terminal(o.Operation.Status) || o.Journal.ResumeRequired || o.Journal.Lease
		if o.Operation.Error != nil && o.Operation.Error.Code == api.ManualRecovery {
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
			if _, ok := g.vms[o.Operation.VMID]; !ok && o.Operation.Request.Action != "delete" {
				return fmt.Errorf("GC pending operation VM root missing: %s", id)
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
		if (o.Operation.Request.Action == "restore" || o.Operation.Request.Action == "clone") && o.Journal.Resource != nil {
			r := *o.Journal.Resource
			p := filepath.Join(r.Dir, "versions", id)
			if r.Disk != filepath.Join(p, "disk.raw") || r.RestoreDir != filepath.Join(p, "restore") || o.Journal.Manifest == nil {
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
		if !gcID(id, "snap-") || s.Snapshot.ID != id || s.Dir != filepath.Join(g.e.opts.StateDir, "snapshots", id) || checkConfig(&s.Snapshot.Config) != nil {
			return fmt.Errorf("invalid snapshot root")
		}
		g.snapshots[id] = s
		g.roots[s.Dir] = true // Tombstones retain roots until explicit deletion.
		if s.Snapshot.Published {
			var m, disk Manifest
			if err := gcDecode(s.Snapshot.Manifest, &m); err != nil {
				return err
			}
			if err := gcReadJSON(filepath.Join(s.Dir, "manifest.json"), &disk); err != nil {
				return err
			}
			if !bytes.Equal(encode(m), encode(disk)) || !reflect.DeepEqual(m.Config, s.Snapshot.Config) {
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

func (g *gcGraph) rootResource(r Resource) {
	g.roots[r.Disk] = true
	if r.RestoreDir != "" {
		g.roots[filepath.Dir(r.RestoreDir)] = true
	}
	g.resources = append(g.resources, r)
}

func (g *gcGraph) resource(r Resource) error {
	if !gcID(r.VMID, "vm-") || r.Dir != filepath.Join(g.e.opts.StateDir, "vms", r.VMID) || checkConfig(&r.Config) != nil || r.ImageDir != filepath.Join(g.e.opts.StateDir, "images", r.Config.Image[7:]) {
		return fmt.Errorf("GC invalid resource ownership")
	}
	if !gcID(r.NetworkToken, "net-") || r.Slot < 1 || r.Slot >= 32768 {
		return fmt.Errorf("GC invalid network ownership")
	}
	if r.Instance == "" && r.RunDir != "" || r.Instance != "" && (!gcID(r.Instance, "run-") || r.RunDir != filepath.Join(r.Dir, "runs", r.Instance)) {
		return fmt.Errorf("GC invalid running instance path")
	}
	rel, err := filepath.Rel(r.Dir, r.Disk)
	parts := strings.Split(rel, string(filepath.Separator))
	if err != nil || !(len(parts) == 2 && parts[0] == "disks" && strings.HasSuffix(parts[1], ".raw") && gcID(strings.TrimSuffix(parts[1], ".raw"), "op-") || len(parts) == 3 && parts[0] == "versions" && gcID(parts[1], "op-") && parts[2] == "disk.raw") {
		return fmt.Errorf("GC invalid disk version path")
	}
	if r.RestoreDir != "" && r.RestoreDir != filepath.Join(filepath.Dir(r.Disk), "restore") {
		return fmt.Errorf("GC invalid backing path")
	}
	return nil
}

func (g *gcGraph) manifest(m Manifest) error {
	if m.Format != 1 || m.MemoryMode != "CopyOnWrite" || checkConfig(&m.Config) != nil {
		return fmt.Errorf("GC unknown snapshot manifest")
	}
	for _, name := range []string{"ch/config.json", "ch/state.json", "ch/memory-ranges", "disk.raw"} {
		if _, err := shaName(m.Files[name]); err != nil {
			return fmt.Errorf("GC snapshot root missing %s", name)
		}
	}
	for rel, hash := range m.Files {
		if filepath.IsAbs(rel) || filepath.Clean(rel) != rel || strings.HasPrefix(rel, "../") || strings.Contains(rel, "\\") {
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
