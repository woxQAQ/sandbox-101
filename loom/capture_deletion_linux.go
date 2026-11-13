package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// InspectCaptureDeletion inventories only the frozen failed capture boundary and
// its possible publication destination. Existing material without a preparation
// identity remains unowned; content checksums never create deletion authority.
func InspectCaptureDeletion(ctx context.Context, stateRoot string, r Resource, operation, snapshot string, boundary *CaptureBoundary) ([]DeletionIntent, error) {
	if !ValidResourceID(r.VMID, "vm-") || r.Dir != filepath.Join(stateRoot, "vms", r.VMID) || !ValidResourceID(operation, "op-") || !ValidResourceID(snapshot, "snap-") {
		return nil, fmt.Errorf("invalid capture deletion resource")
	}
	raw := filepath.Join(r.Dir, "capture", operation)
	final := filepath.Join(stateRoot, "snapshots", snapshot)
	if boundary != nil && (boundary.Path != raw || boundary.Parent.Inode == 0 || boundary.Publication.Inode == 0) {
		return nil, fmt.Errorf("capture preparation identity changed")
	}
	var intents []DeletionIntent
	for _, path := range []string{raw, final} {
		directories := map[string]bool{".": true, "ch": true}
		files := map[string]os.FileMode{}
		fixed := map[string]FileIdentity{}
		for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
			files["ch/"+name] = 0
		}
		if path == raw {
			directories["publication"] = true
			directories["publication/ch"] = true
			files["publication/disk.raw"] = 0
			files["publication/manifest.json"] = 0
			for _, name := range []string{"config.json", "state.json", "memory-ranges"} {
				files["publication/ch/"+name] = 0
			}
			if boundary != nil {
				fixed["."] = boundary.Parent
				fixed["publication"] = boundary.Publication
			}
		} else {
			files["disk.raw"] = 0
			files["manifest.json"] = 0
			if boundary != nil {
				fixed["."] = boundary.Publication
			}
		}
		if boundary == nil {
			if _, err := os.Lstat(path); !os.IsNotExist(err) {
				return nil, fmt.Errorf("existing capture directory lacks preparation proof: %v", err)
			}
		}
		intent, _, err := inspectVMFileTopology(ctx, stateRoot, Resource{Dir: path}, directories, files, fixed)
		if err != nil {
			return nil, err
		}
		intents = append(intents, intent)
	}
	return intents, nil
}

// VerifyCaptureDeletion binds a persisted receipt to the original preparation
// identities, including recovery after some or all names have been removed.
func VerifyCaptureDeletion(intents []DeletionIntent, raw, final string, boundary *CaptureBoundary) error {
	if len(intents) != 2 {
		return fmt.Errorf("capture deletion requires both destinations")
	}
	for index, path := range []string{raw, final} {
		intent := intents[index]
		if intent.Format != 1 || !intent.Dir || intent.Path != path {
			return fmt.Errorf("capture deletion receipt destination changed")
		}
		if len(intent.Nodes) == 0 {
			continue
		}
		if boundary == nil || boundary.Path != raw {
			return fmt.Errorf("capture deletion receipt lacks original preparation identity")
		}
		expected := boundary.Publication
		if index == 0 {
			expected = boundary.Parent
		}
		if actual, ok := intent.Nodes["."]; !ok || actual != expected {
			return fmt.Errorf("capture deletion root differs from preparation identity")
		}
		if index == 0 {
			if actual, ok := intent.Nodes["publication"]; ok && actual != boundary.Publication {
				return fmt.Errorf("capture deletion publication identity changed")
			}
		}
	}
	return nil
}
