package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"

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
