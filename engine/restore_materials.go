package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/woxQAQ/sandbox-engine/api"
)

// restoreMaterials proves delivery bytes, not the later writable disk state.
// Source digests are fixed by the verified snapshot manifest before any copying.
type restoreMaterials struct {
	Resource Resource               `json:"resource"`
	Version  gcIdentity             `json:"version"`
	Restore  gcIdentity             `json:"restore"`
	Files    map[string]restoreFile `json:"files"`
	Complete bool                   `json:"complete"`
}

type restoreFile struct {
	Source    string     `json:"source"`
	Candidate string     `json:"candidate"`
	Size      int64      `json:"size"`
	Identity  gcIdentity `json:"identity"`
}

func restorePath(r Resource, name string) string {
	if name == "disk.raw" {
		return r.Disk
	}
	return filepath.Join(r.RestoreDir, name)
}

func restoreSource(dir, name string) string {
	if name == "disk.raw" {
		return filepath.Join(dir, name)
	}
	return filepath.Join(dir, "ch", name)
}

// restoreVMFence reconstructs only the internal fence after an administrative
// recovery acknowledgement cleared VM operation ownership. Public failures stay fixed.
func (e *Engine) restoreVMFence(v *vmRecord) error {
	if v.VM.OperationID != "" {
		return nil
	}
	rows, err := e.db.Query("SELECT body FROM operations WHERE status='failed'")
	if err != nil {
		return err
	}
	var blocked *opRecord
	for rows.Next() {
		var body []byte
		var record opRecord
		if err = rows.Scan(&body); err != nil {
			break
		}
		if err = json.Unmarshal(body, &record); err != nil {
			break
		}
		if record.Operation.VMID != v.VM.ID || record.Journal.RestoreCleaned ||
			(record.Operation.Request.Action != "restore" && record.Operation.Request.Action != "clone") ||
			(!record.Journal.RestoreBlocked && record.Journal.Resource == nil) {
			continue
		}
		if blocked != nil {
			err = api.Fail(
				api.ManualRecovery,
				"multiple unresolved restore fences",
			)
			break
		}
		blocked = &record
	}
	if err == nil {
		err = rows.Err()
	}
	err = errors.Join(err, rows.Close())
	if err != nil {
		return err
	}
	if blocked == nil {
		return nil
	}
	blocked.Journal.RestoreBlocked = true
	if _, err = e.db.Exec(
		"UPDATE operations SET body=? WHERE id=?",
		encode(blocked),
		blocked.Operation.ID,
	); err != nil {
		return err
	}
	v.VM.OperationID = blocked.Operation.ID
	v.VM.BlockedReason = "restore materials unresolved; explicit delete required"
	return e.saveVM(*v)
}

// deleteBlockedRestore is the only path that retires a failed restore fence.
// Its receipt belongs to the original restore so new delete keys resume it.
func (e *Engine) deleteBlockedRestore(o *opRecord, v *vmRecord) error {
	blocked, err := e.loadOp(e.ctx, o.Journal.BlockedRestoreID)
	if err != nil {
		return err
	}
	if blocked.Operation.Status != "failed" ||
		blocked.Operation.VMID != v.VM.ID ||
		!blocked.Journal.RestoreBlocked ||
		blocked.Journal.RestoreCleaned {
		return api.Fail(
			api.ManualRecovery,
			"blocked restore delete ownership mismatch",
		)
	}
	g := gcGraph{
		e:        e,
		ctx:      e.ctx,
		vms:      map[string]vmRecord{v.VM.ID: *v},
		ops:      map[string]opRecord{},
		versions: map[string]gcVersion{},
	}
	resources := []Resource{v.Resource}
	if err = g.records("operations", func(id string, body []byte) error {
		var record opRecord
		if err := gcDecode(body, &record); err != nil {
			return err
		}
		if record.Operation.VMID != v.VM.ID {
			return nil
		}
		g.ops[id] = record
		for _, r := range []*Resource{record.Journal.Resource, record.Journal.Old} {
			if r == nil {
				continue
			}
			if r.VMID != v.VM.ID {
				return api.Fail(
					api.ManualRecovery,
					"delete journal VM mismatch",
				)
			}
			if err := g.resource(*r); err != nil {
				return err
			}
			resources = append(resources, *r)
		}
		return nil
	}); err != nil {
		return err
	}
	seen := map[string]bool{}
	var instances []Resource
	for _, r := range resources {
		if err = g.resource(r); err != nil {
			return err
		}
		if seen[r.Instance] {
			continue
		}
		seen[r.Instance] = true
		if _, err = e.opts.Backend.Inspect(e.ctx, r); err != nil {
			return err
		}
		instances = append(instances, r)
	}
	if err = e.saveOp(o, "restore_delete_stop_intent"); err != nil {
		return err
	}
	for _, r := range instances {
		if err = e.opts.Backend.Network(e.ctx, r, false); err != nil {
			return err
		}
		if err = e.opts.Backend.Stop(e.ctx, r); err != nil {
			return err
		}
		obs, x := e.opts.Backend.Inspect(e.ctx, r)
		if x != nil {
			return x
		}
		if obs.Alive || obs.Network {
			return api.Fail(
				api.ManualRecovery,
				"restore deletion cannot confirm instance/device shutdown",
			)
		}
	}
	for _, r := range instances {
		if err = e.opts.Backend.Remove(e.ctx, r); err != nil {
			return err
		}
	}
	files, err := g.blockedRestoreFiles(v.Resource.Dir)
	if err != nil {
		return err
	}
	if err = gcLocalReaders(files); err != nil {
		return err
	}
	if blocked.Journal.RestoreDeletion == nil {
		receipt, x := g.deletion(v.Resource.Dir, files, true)
		if os.IsNotExist(x) {
			receipt = gcDeletion{
				Format: 1,
				Path:   v.Resource.Dir,
				Dir:    true,
				Nodes:  map[string]gcIdentity{},
			}
		} else if x != nil {
			return x
		}
		blocked.Journal.RestoreDeletion = &receipt
		if _, err = e.db.Exec(
			"UPDATE operations SET body=? WHERE id=?",
			encode(blocked),
			blocked.Operation.ID,
		); err != nil {
			return err
		}
		if err = e.checkpoint(); err != nil {
			return err
		}
	}
	if err = e.saveOp(o, "restore_delete_remove_intent"); err != nil {
		return err
	}
	receipt := blocked.Journal.RestoreDeletion
	if receipt.Format != 1 || !receipt.Dir || receipt.Path != v.Resource.Dir {
		return api.Fail(
			api.ManualRecovery,
			"blocked restore deletion receipt ownership mismatch",
		)
	}
	files, err = g.resumeFiles(*receipt)
	if err != nil {
		return err
	}
	if err = gcLocalReaders(files); err != nil {
		return err
	}
	if err = g.removeFiles(v.Resource.Dir, files, true); err != nil {
		return err
	}
	if err = e.saveOp(o, "restore_delete_removed"); err != nil {
		return err
	}
	e.admission.Lock()
	defer e.admission.Unlock()
	tx, err := e.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{"DELETE FROM backing WHERE vm=?", "DELETE FROM vms WHERE id=?"} {
		if _, err = tx.Exec(query, v.VM.ID); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(
		"DELETE FROM reservations WHERE owner=? OR owner LIKE ?",
		v.VM.ID,
		v.VM.ID+":%",
	); err != nil {
		return err
	}
	if _, err = tx.Exec(
		"DELETE FROM leases WHERE operation=?",
		blocked.Operation.ID,
	); err != nil {
		return err
	}
	blocked.Journal.Lease = false
	blocked.Journal.RestoreCleaned = true
	if _, err = tx.Exec(
		"UPDATE operations SET body=? WHERE id=?",
		encode(blocked),
		blocked.Operation.ID,
	); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return e.saveOp(o, "restore_delete_committed")
}

// The VM ledger proves names, not arbitrary descendants. Unexpected files,
// links, special files and mounts retain the entire blocked VM for diagnosis.
func (g *gcGraph) blockedRestoreFiles(root string) ([]string, error) {
	allowed := map[string]bool{}
	dirs := map[string]bool{
		".":        true,
		"disks":    true,
		"versions": true,
		"capture":  true,
		"runs":     true,
	}
	add := func(path string) {
		rel, _ := filepath.Rel(root, path)
		allowed[rel] = true
		for p := filepath.Dir(rel); p != "."; p = filepath.Dir(p) {
			dirs[p] = true
		}
	}
	for _, name := range []string{"network.json", "network-removing.json", "network-policy.json"} {
		add(filepath.Join(root, name))
	}
	for id, o := range g.ops {
		if o.Operation.Request.Action == "create" {
			add(filepath.Join(root, "disks", id+".raw"))
			add(filepath.Join(root, "disks", id+".raw.pending"))
		}
		if o.Operation.Request.Action == "snapshot" {
			for _, prefix := range []string{"ch", "publication/ch"} {
				for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
					add(filepath.Join(root, "capture", id, prefix, name))
				}
			}
			for _, name := range []string{"disk.raw", "manifest.json"} {
				add(filepath.Join(root, "capture", id, "publication", name))
			}
		}
		for _, r := range []*Resource{o.Journal.Resource, o.Journal.Old} {
			if r == nil {
				continue
			}
			add(r.Disk)
			add(r.Disk + ".pending")
			if r.RestoreDir != "" {
				for _, name := range []string{"config.json", "state.json", "memory-ranges", "config.json.pending", "state.json.pending"} {
					add(filepath.Join(r.RestoreDir, name))
				}
			}
			if r.RunDir != "" {
				for _, name := range []string{"process.json", "launch.json", "console.log", "io/api.sock", "io/vsock"} {
					add(filepath.Join(r.RunDir, name))
				}
				rel, _ := filepath.Rel(root, filepath.Join(r.RunDir, "root"))
				dirs[rel] = true
			}
		}
	}
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
		fields := strings.Fields(line)
		if len(fields) >= 5 {
			mount := unescape.Replace(fields[4])
			if mount == root || strings.HasPrefix(mount, root+"/") {
				return nil, fmt.Errorf(
					"mounted blocked restore retained: %s",
					mount,
				)
			}
		}
	}
	files := []string{}
	err = filepath.WalkDir(
		root,
		func(path string, entry os.DirEntry, err error) error {
			if os.IsNotExist(err) && path == root {
				return nil
			}
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			if entry.IsDir() {
				if !dirs[rel] {
					return fmt.Errorf(
						"unknown blocked restore directory: %s",
						path,
					)
				}
				return nil
			}
			socket := entry.Type()&os.ModeSocket != 0 &&
				(strings.HasSuffix(rel, "/io/api.sock") || strings.HasSuffix(rel, "/io/vsock"))
			if !allowed[rel] || (!entry.Type().IsRegular() && !socket) {
				return fmt.Errorf("unknown blocked restore artifact: %s", path)
			}
			files = append(files, path)
			return nil
		},
	)
	return files, err
}

var restoreNames = []string{
	"config.json",
	"state.json",
	"disk.raw",
	"memory-ranges",
}

// All type, size and digest checks and syncing refer to this same no-follow fd.
func inspectRestoreFile(
	path, want string,
	size int64,
	sync bool,
) (restoreFile, error) {
	parent, err := openCaptureDir(filepath.Dir(path))
	if err != nil {
		return restoreFile{}, err
	}
	defer func() { _ = parent.Close() }()
	fd, err := unix.Openat(
		int(parent.Fd()),
		filepath.Base(path),
		unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC,
		0,
	)
	if err != nil {
		return restoreFile{}, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return restoreFile{}, err
	}
	if !st.Mode().IsRegular() || st.Size() <= 0 ||
		size >= 0 && st.Size() != size {
		return restoreFile{}, fmt.Errorf(
			"candidate regular-file/size mismatch: %s",
			path,
		)
	}
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return restoreFile{}, err
	}
	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != want {
		return restoreFile{}, fmt.Errorf(
			"candidate checksum mismatch: %s",
			path,
		)
	}
	if sync {
		if err = f.Sync(); err != nil {
			return restoreFile{}, err
		}
	}
	return restoreFile{
		Source:    want,
		Candidate: got,
		Size:      st.Size(),
		Identity:  gcNode(st),
	}, nil
}

func (e *Engine) checkRestoreResource(o *opRecord, r Resource) error {
	g := gcGraph{e: e}
	if err := g.resource(r); err != nil {
		return err
	}
	version := filepath.Join(r.Dir, "versions", o.Operation.ID)
	if r.VMID != o.Operation.VMID ||
		r.Disk != filepath.Join(version, "disk.raw") ||
		r.RestoreDir != filepath.Join(version, "restore") {
		return api.Fail(
			api.ManualRecovery,
			"restore candidate ownership mismatch",
		)
	}
	return nil
}

func (e *Engine) prepareRestoreMaterials(
	o *opRecord,
	s snapshotRecord,
	m Manifest,
) error {
	r := *o.Journal.Resource
	if err := e.checkRestoreResource(o, r); err != nil {
		return err
	}
	obs, err := e.opts.Backend.Inspect(e.ctx, r)
	if err != nil {
		return err
	}
	if obs.Alive {
		return api.Fail(
			api.ManualRecovery,
			"unproved restore candidate already in use",
		)
	}
	// A prior incomplete attempt is retained, even if the cached bytes look intact.
	// We need no overwrite/rebuild exception, including for uncertain VMM identity.
	if o.Journal.RestoreMaterials != nil {
		if !o.Journal.RestoreMaterials.Complete {
			return api.Fail(
				api.Incompatible,
				"restore candidate interrupted before completion proof; materials retained",
			)
		}
		return e.verifyRestoreMaterials(o, m, true)
	}
	// Resolve trusted parents with directory descriptors before making entries.
	root, err := openCaptureDir(r.Dir)
	if os.IsNotExist(err) && o.Operation.Request.Action == "clone" {
		if err = os.Mkdir(r.Dir, 0o700); err == nil {
			root, err = openCaptureDir(r.Dir)
		}
	}
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	versions, err := captureChild(root, "versions", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = versions.Close() }()
	// Existing private version entries without a proof are diagnostic evidence.
	if err = unix.Mkdirat(
		int(versions.Fd()),
		o.Operation.ID,
		0o700,
	); err != nil {
		return err
	}
	version, err := openCaptureDir(filepath.Dir(r.RestoreDir))
	if err != nil {
		return err
	}
	defer func() { _ = version.Close() }()
	restore, err := captureChild(version, "restore", 0o700)
	if err != nil {
		return err
	}
	defer func() { _ = restore.Close() }()
	proof := &restoreMaterials{Resource: r, Files: map[string]restoreFile{}}
	for _, name := range restoreNames {
		key := "ch/" + name
		if name == "disk.raw" {
			key = name
		}
		file, x := inspectRestoreFile(
			restoreSource(s.Dir, name),
			m.Files[key],
			-1,
			false,
		)
		if x != nil {
			return x
		}
		file.Candidate = ""
		proof.Files[name] = file
	}
	vi, err := version.Stat()
	if err != nil {
		return err
	}
	ri, err := restore.Stat()
	if err != nil {
		return err
	}
	proof.Version, proof.Restore = gcNode(vi), gcNode(ri)
	for _, f := range []*os.File{restore, version, versions, root} {
		if err = f.Sync(); err != nil {
			return err
		}
	}
	if err = syncDir(filepath.Dir(r.Dir)); err != nil {
		return err
	}
	o.Journal.RestoreMaterials = proof
	if err = e.saveOp(o, "restore_candidate_prepared"); err != nil {
		return err
	}
	for _, name := range restoreNames {
		dst := restorePath(r, name)
		src := restoreSource(s.Dir, name)
		if name == "memory-ranges" {
			if err = os.Link(src, dst); err != nil {
				return err
			}
			continue
		}
		mode := os.FileMode(0o444)
		if name == "disk.raw" {
			mode = 0o600
		}
		tmp := dst + ".pending"
		if err = copyFile(src, tmp, mode); err != nil {
			return err
		}
		if err = e.saveOp(o, "restore_candidate_copied"); err != nil {
			return err
		}
		if _, err = inspectRestoreFile(
			tmp,
			proof.Files[name].Source,
			proof.Files[name].Size,
			true,
		); err != nil {
			return err
		}
		if err = os.Rename(tmp, dst); err != nil {
			return err
		}
		if err = e.saveOp(o, "restore_candidate_renamed"); err != nil {
			return err
		}
	}
	for _, name := range restoreNames {
		file, x := inspectRestoreFile(
			restorePath(r, name),
			proof.Files[name].Source,
			proof.Files[name].Size,
			true,
		)
		if x != nil {
			return x
		}
		if name == "memory-ranges" &&
			file.Identity != proof.Files[name].Identity {
			return api.Fail(
				api.ManualRecovery,
				"candidate backing inode mismatch",
			)
		}
		proof.Files[name] = file
	}
	for _, f := range []*os.File{restore, version, versions, root} {
		if err = f.Sync(); err != nil {
			return err
		}
	}
	if err = e.saveOp(o, "restore_candidate_synced"); err != nil {
		return err
	}
	proof.Complete = true
	return e.saveOp(o, "restore_materials_complete")
}

func (e *Engine) verifyRestoreMaterials(
	o *opRecord,
	m Manifest,
	disk bool,
) error {
	proof := o.Journal.RestoreMaterials
	if proof == nil || !proof.Complete || o.Journal.Resource == nil ||
		!reflect.DeepEqual(proof.Resource, *o.Journal.Resource) ||
		len(proof.Files) != len(restoreNames) {
		return api.Fail(
			api.ManualRecovery,
			"restore completion proof missing or inconsistent; legacy handoff requires manual recovery",
		)
	}
	r := proof.Resource
	if err := e.checkRestoreResource(o, r); err != nil {
		return err
	}
	for path, identity := range map[string]gcIdentity{filepath.Dir(r.RestoreDir): proof.Version, r.RestoreDir: proof.Restore} {
		f, err := openCaptureDir(path)
		if err != nil {
			return err
		}
		st, err := f.Stat()
		err = errors.Join(err, f.Close())
		if err != nil {
			return err
		}
		if gcNode(st) != identity {
			return api.Fail(
				api.ManualRecovery,
				"restore directory identity changed",
			)
		}
	}
	for _, name := range restoreNames {
		file := proof.Files[name]
		key := "ch/" + name
		if name == "disk.raw" {
			key = name
		}
		if file.Source != m.Files[key] || file.Candidate != file.Source ||
			file.Size <= 0 {
			return api.Fail(
				api.Incompatible,
				"restore fixed source proof mismatch",
			)
		}
		if name == "disk.raw" && !disk {
			continue
		}
		got, err := inspectRestoreFile(
			restorePath(r, name),
			file.Source,
			file.Size,
			false,
		)
		if err != nil {
			return err
		}
		if got.Identity != file.Identity {
			return api.Fail(
				api.ManualRecovery,
				"restore artifact inode mismatch",
			)
		}
	}
	return nil
}
