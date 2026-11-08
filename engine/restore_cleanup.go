package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

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
		instances = append(instances, r)
	}
	if err = e.saveOp(o, "restore_delete_stop_intent"); err != nil {
		return err
	}
	// Stop validates the exact recorded process identity itself. Inspect also
	// rejects live sibling scopes, including other journal-owned instances, so
	// all known instances must exit before the complete inventory can be proved.
	// No network/device mutation or removal is authorized by Stop alone.
	for _, r := range instances {
		if err = e.opts.Backend.Stop(e.ctx, r); err != nil {
			return err
		}
	}
	for _, r := range instances {
		obs, x := e.opts.Backend.Inspect(e.ctx, r)
		if x != nil {
			return x
		}
		if obs.Alive {
			return api.Fail(
				api.ManualRecovery,
				"restore deletion cannot confirm instance shutdown",
			)
		}
	}
	for _, r := range instances {
		if err = e.opts.Backend.Network(e.ctx, r, false); err != nil {
			return err
		}
	}
	for _, r := range instances {
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
