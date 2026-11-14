package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// VMDeletionMember is one frozen resource version. Unpublished permits only its
// prepared private partial files, never execution or publication of those bytes.
type VMDeletionMember struct {
	Resource     Resource
	DiskIdentity *FileIdentity
	Materials    *RestoreMaterials
	Unpublished  bool
	OperationID  string
}

// InspectVMSetDeletion combines only the explicitly supplied version/run plans.
// The control plane proves this is the complete owned set before calling it.
func InspectVMSetDeletion(ctx context.Context, stateRoot string, members []VMDeletionMember, captures []CaptureBoundary) (DeletionIntent, []string, error) {
	var empty DeletionIntent
	if len(members) == 0 {
		return empty, nil, fmt.Errorf("empty VM deletion set")
	}
	first := members[0].Resource
	directories := map[string]bool{
		".":       true,
		"capture": true,
		"runs":    true,
	}
	files := map[string]os.FileMode{
		"network.json":          0,
		"network-policy.json":   0,
		"network-removing.json": 0,
	}
	fixed := map[string]FileIdentity{}
	addFixed := func(path string, identity FileIdentity) error {
		if old, ok := fixed[path]; ok && old != identity {
			return fmt.Errorf("conflicting deletion identities")
		}
		fixed[path] = identity
		return nil
	}
	seen := map[string]bool{}
	for _, member := range members {
		r := member.Resource
		if !ValidResourceID(member.OperationID, "op-") || !ValidResourceID(r.VMID, "vm-") || !ValidResourceID(r.Instance, "run-") || r.Dir != filepath.Join(stateRoot, "vms", r.VMID) || r.VMID != first.VMID || r.Dir != first.Dir || r.NetworkToken != first.NetworkToken || r.Slot != first.Slot || seen[r.Instance] || r.RunDir != filepath.Join(r.Dir, "runs", r.Instance) {
			return empty, nil, fmt.Errorf("invalid explicit VM version set")
		}
		seen[r.Instance] = true
		run := "runs/" + r.Instance
		directories[run] = true
		directories[run+"/root"] = true
		directories[run+"/io"] = true
		for _, name := range []string{
			"launch.json",
			"console.log",
			"process.json",
		} {
			files[run+"/"+name] = 0
		}
		files[run+"/io/api.sock"] = os.ModeSocket
		files[run+"/io/vsock"] = os.ModeSocket
		if r.RestoreDir == "" {
			relative := "disks/" + member.OperationID + ".raw"
			if r.Disk != filepath.Join(r.Dir, relative) || member.Materials != nil {
				return empty, nil, fmt.Errorf("cold version path mismatch")
			}
			if member.DiskIdentity == nil {
				if _, err := os.Lstat(r.Dir); !os.IsNotExist(err) {
					return empty, nil, fmt.Errorf("existing cold candidate lacks inode proof")
				}
			} else if err := addFixed(relative, *member.DiskIdentity); err != nil {
				return empty, nil, err
			}
			directories["disks"] = true
			files[relative] = 0
			continue
		}
		version := "versions/" + member.OperationID
		if r.RestoreDir != filepath.Join(r.Dir, version, "restore") || r.Disk != filepath.Join(r.Dir, version, "disk.raw") {
			return empty, nil, fmt.Errorf("restored version path mismatch")
		}
		proof := member.Materials
		if proof == nil {
			if _, err := os.Lstat(filepath.Join(r.Dir, version)); !os.IsNotExist(err) {
				return empty, nil, fmt.Errorf("existing restore candidate lacks boundary proof")
			}
			if !member.Unpublished {
				return empty, nil, fmt.Errorf("published restore proof missing")
			}
			continue
		}
		if !reflect.DeepEqual(proof.Resource, r) || len(proof.Files) != 4 || proof.Version.Inode == 0 || proof.Restore.Inode == 0 || !member.Unpublished && !proof.Complete {
			return empty, nil, fmt.Errorf("restore version proof incomplete")
		}
		directories["versions"] = true
		directories[version] = true
		directories[version+"/restore"] = true
		if err := addFixed(version, proof.Version); err != nil {
			return empty, nil, err
		}
		if err := addFixed(version+"/restore", proof.Restore); err != nil {
			return empty, nil, err
		}
		for _, name := range RestoreFileNames() {
			relative := version + "/restore/" + name
			if name == "disk.raw" {
				relative = version + "/disk.raw"
			}
			file := proof.Files[name]
			if file.Source == "" || file.Size <= 0 || file.Candidate != "" && file.Candidate != file.Source || proof.Complete && file.Candidate == "" {
				return empty, nil, fmt.Errorf("restore file proof inconsistent")
			}
			files[relative] = 0
			if file.Candidate != "" || name == "memory-ranges" {
				if err := addFixed(relative, file.Identity); err != nil {
					return empty, nil, err
				}
			}
			if member.Unpublished && !proof.Complete && name != "memory-ranges" {
				files[relative+".pending"] = 0
			}
		}
	}
	for _, boundary := range captures {
		operation := filepath.Base(boundary.Path)
		if !ValidResourceID(operation, "op-") || boundary.Path != filepath.Join(first.Dir, "capture", operation) {
			return empty, nil, fmt.Errorf("capture boundary outside VM set")
		}
		relative := "capture/" + operation
		directories[relative] = true
		directories[relative+"/ch"] = true
		if err := addFixed(relative, boundary.Parent); err != nil {
			return empty, nil, err
		}
		for _, name := range []string{
			"config.json",
			"state.json",
			"memory-ranges",
		} {
			files[relative+"/ch/"+name] = 0
		}
	}
	return inspectVMFileTopology(ctx, stateRoot, first, directories, files, fixed)
}
