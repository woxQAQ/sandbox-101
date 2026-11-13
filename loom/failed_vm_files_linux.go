package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

// InspectFailedVMDeletion proves only destructive ownership. Incomplete delivery
// bytes remain unpublished; their pinned private directories and fixed names can
// authorize an exact deletion receipt after all known runtime instances exit.
func InspectFailedVMDeletion(ctx context.Context, stateRoot string, resource Resource, operation string, diskIdentity *FileIdentity, materials *RestoreMaterials) (DeletionIntent, []string, error) {
	empty := DeletionIntent{Format: 1, Path: resource.Dir, Dir: true, Nodes: map[string]FileIdentity{}}
	if !ValidResourceID(operation, "op-") || !ValidResourceID(resource.VMID, "vm-") || !ValidResourceID(resource.Instance, "run-") || resource.Dir != filepath.Join(stateRoot, "vms", resource.VMID) || resource.RunDir != filepath.Join(resource.Dir, "runs", resource.Instance) {
		return empty, nil, fmt.Errorf("failed candidate paths differ from frozen intent")
	}
	run := "runs/" + resource.Instance
	directories := map[string]bool{".": true, "capture": true, "runs": true, run: true, run + "/root": true, run + "/io": true}
	files := map[string]os.FileMode{"network.json": 0, "network-policy.json": 0, "network-removing.json": 0, run + "/launch.json": 0, run + "/console.log": 0, run + "/process.json": 0, run + "/io/api.sock": os.ModeSocket, run + "/io/vsock": os.ModeSocket}
	fixed := map[string]FileIdentity{}
	if resource.RestoreDir == "" {
		disk := "disks/" + operation + ".raw"
		if materials != nil || resource.Disk != filepath.Join(resource.Dir, disk) {
			return empty, nil, fmt.Errorf("failed cold candidate path mismatch")
		}
		if diskIdentity == nil {
			if _, err := os.Lstat(resource.Dir); os.IsNotExist(err) {
				return empty, nil, nil
			}
			return empty, nil, fmt.Errorf("existing failed cold candidate lacks prepared inode proof")
		}
		directories["disks"] = true
		files[disk] = 0
		fixed[disk] = *diskIdentity
	} else {
		version := "versions/" + operation
		if resource.RestoreDir != filepath.Join(resource.Dir, version, "restore") || resource.Disk != filepath.Join(resource.Dir, version, "disk.raw") {
			return empty, nil, fmt.Errorf("failed restore candidate path mismatch")
		}
		if materials == nil {
			if _, err := os.Lstat(resource.Dir); os.IsNotExist(err) {
				return empty, nil, nil
			}
			return empty, nil, fmt.Errorf("existing failed restore lacks prepared directory identity")
		}
		if !reflect.DeepEqual(materials.Resource, resource) || len(materials.Files) != 4 || materials.Version.Inode == 0 || materials.Restore.Inode == 0 {
			return empty, nil, fmt.Errorf("failed restore boundary proof incomplete")
		}
		directories["versions"] = true
		directories[version] = true
		directories[version+"/restore"] = true
		fixed[version] = materials.Version
		fixed[version+"/restore"] = materials.Restore
		for _, name := range RestoreFileNames() {
			relative := version + "/restore/" + name
			if name == "disk.raw" {
				relative = version + "/disk.raw"
			}
			proof := materials.Files[name]
			if proof.Source == "" || proof.Size <= 0 {
				return empty, nil, fmt.Errorf("failed restore source identity missing")
			}
			files[relative] = 0
			if proof.Candidate != "" {
				if proof.Candidate != proof.Source || proof.Identity.Inode == 0 {
					return empty, nil, fmt.Errorf("failed restore candidate identity inconsistent")
				}
				fixed[relative] = proof.Identity
			} else if materials.Complete {
				return empty, nil, fmt.Errorf("completed restore lacks candidate identity")
			}
			if !materials.Complete && name != "memory-ranges" {
				files[relative+".pending"] = 0
			}
			if name == "memory-ranges" {
				fixed[relative] = proof.Identity
			}
		}
	}
	return inspectVMFileTopology(ctx, stateRoot, resource, directories, files, fixed)
}
