package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// InspectVMDeletion accepts one explicitly proved cold or restored version,
// its exact run, and the supplied published-capture boundaries. No unknown
// version/run/file is adopted. The caller persists the inode receipt before unlink.
func InspectVMDeletion(ctx context.Context, stateRoot string, resource Resource, diskIdentity FileIdentity, materials *RestoreMaterials, captures []CaptureBoundary) (DeletionIntent, []string, error) {
	intent := DeletionIntent{
		Format: 1,
		Path:   resource.Dir,
		Dir:    true,
		Nodes:  map[string]FileIdentity{},
	}
	if !ValidResourceID(resource.VMID, "vm-") || !ValidResourceID(resource.Instance, "run-") || resource.Dir != filepath.Join(stateRoot, "vms", resource.VMID) || resource.RunDir != filepath.Join(resource.Dir, "runs", resource.Instance) {
		return intent, nil, fmt.Errorf("deletion resource differs from frozen input")
	}
	run := "runs/" + resource.Instance
	directories := map[string]bool{
		".":           true,
		"capture":     true,
		"runs":        true,
		run:           true,
		run + "/root": true,
		run + "/io":   true,
	}
	files := map[string]os.FileMode{
		"network.json":          0,
		"network-policy.json":   0,
		"network-removing.json": 0,
		run + "/launch.json":    0,
		run + "/console.log":    0,
		run + "/process.json":   0,
		run + "/io/api.sock":    os.ModeSocket,
		run + "/io/vsock":       os.ModeSocket,
	}
	fixed := map[string]FileIdentity{}
	if resource.RestoreDir == "" {
		disk := filepath.Base(resource.Disk)
		if materials != nil || !strings.HasSuffix(disk, ".raw") || !ValidResourceID(strings.TrimSuffix(disk, ".raw"), "op-") || resource.Disk != filepath.Join(resource.Dir, "disks", disk) {
			return intent, nil, fmt.Errorf("cold disk path differs from historical version")
		}
		directories["disks"] = true
		files["disks/"+disk] = 0
		fixed["disks/"+disk] = diskIdentity
	} else {
		operation := filepath.Base(filepath.Dir(resource.RestoreDir))
		version := "versions/" + operation
		if !ValidResourceID(operation, "op-") || resource.RestoreDir != filepath.Join(resource.Dir, version, "restore") || resource.Disk != filepath.Join(resource.Dir, version, "disk.raw") || materials == nil || !materials.Complete || !reflect.DeepEqual(materials.Resource, resource) || len(materials.Files) != 4 || materials.Files["disk.raw"].Identity != diskIdentity {
			return intent, nil, fmt.Errorf("restore deletion completion proof missing")
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
			if proof.Source == "" || proof.Source != proof.Candidate || proof.Size <= 0 {
				return intent, nil, fmt.Errorf("restore deletion file identity missing")
			}
			files[relative] = 0
			fixed[relative] = proof.Identity
		}
	}
	for _, boundary := range captures {
		operation := filepath.Base(boundary.Path)
		if !ValidResourceID(operation, "op-") || boundary.Path != filepath.Join(resource.Dir, "capture", operation) {
			return intent, nil, fmt.Errorf("invalid explicit capture deletion boundary")
		}
		relative := "capture/" + operation
		if _, duplicate := fixed[relative]; duplicate {
			return intent, nil, fmt.Errorf("duplicate capture deletion boundary")
		}
		fixed[relative] = boundary.Parent
		directories[relative] = true
		directories[relative+"/ch"] = true
		for _, name := range []string{
			"config.json",
			"state.json",
			"memory-ranges",
		} {
			files[relative+"/ch/"+name] = 0
		}
	}
	return inspectVMFileTopology(ctx, stateRoot, resource, directories, files, fixed)
}

func inspectVMFileTopology(ctx context.Context, stateRoot string, resource Resource, directories map[string]bool, files map[string]os.FileMode, fixed map[string]FileIdentity) (DeletionIntent, []string, error) {
	intent := DeletionIntent{
		Format: 1,
		Path:   resource.Dir,
		Dir:    true,
		Nodes:  map[string]FileIdentity{},
	}
	if _, err := os.Lstat(resource.Dir); os.IsNotExist(err) {
		return intent, nil, nil
	}
	if err := ValidatePath(ctx, stateRoot, resource.Dir, true); err != nil {
		return intent, nil, err
	}
	var paths []string
	err := filepath.WalkDir(resource.Dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		relative, _ := filepath.Rel(resource.Dir, path)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if expected, known := fixed[relative]; known && IdentityOf(info) != expected {
			return fmt.Errorf("historical deletion inode changed: %s", path)
		}
		if entry.IsDir() {
			if !directories[relative] {
				return fmt.Errorf("unknown VM directory retained: %s", path)
			}
		} else {
			kind, known := files[relative]
			if !known || info.Mode()&os.ModeType != kind {
				return fmt.Errorf("unknown VM artifact retained: %s", path)
			}
			paths = append(paths, path)
		}
		intent.Nodes[relative] = IdentityOf(info)
		return nil
	})
	if err != nil {
		return intent, nil, err
	}
	if err = rejectDeletionMounts(resource.Dir); err != nil {
		return intent, nil, err
	}
	return intent, paths, nil
}
