package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// InspectPublishedCaptureRawDeletion owns only the original raw boundary. The
// independently published snapshot is outside this deletion topology.
func InspectPublishedCaptureRawDeletion(ctx context.Context, stateRoot string, r Resource, operation string, boundary *CaptureBoundary, digests map[string]string) (DeletionIntent, error) {
	var empty DeletionIntent
	raw := filepath.Join(r.Dir, "capture", operation)
	if !ValidResourceID(r.VMID, "vm-") || r.Dir != filepath.Join(stateRoot, "vms", r.VMID) || !ValidResourceID(operation, "op-") || boundary == nil || boundary.Path != raw || boundary.Parent.Inode == 0 {
		return empty, fmt.Errorf("published capture raw boundary lacks original identity")
	}
	files := map[string]os.FileMode{}
	for _, name := range []string{
		"config.json",
		"state.json",
		"memory-ranges",
	} {
		relative := "ch/" + name
		files[relative] = 0
		if digests[relative] == "" {
			return empty, fmt.Errorf("published capture artifact digest missing")
		}
	}
	intent, _, err := inspectVMFileTopology(ctx, stateRoot, Resource{
		Dir: raw,
	}, map[string]bool{
		".":  true,
		"ch": true,
	}, files, map[string]FileIdentity{
		".": boundary.Parent,
	})
	if err != nil {
		return empty, err
	}
	for relative := range files {
		actual, err := DigestFileContext(ctx, filepath.Join(raw, relative))
		if err != nil {
			return empty, err
		}
		if actual != digests[relative] {
			return empty, fmt.Errorf("published raw capture bytes changed")
		}
	}
	return intent, VerifyPublishedCaptureRawDeletion(intent, raw, boundary)
}

func VerifyPublishedCaptureRawDeletion(intent DeletionIntent, raw string, boundary *CaptureBoundary) error {
	if intent.Format != 1 || !intent.Dir || intent.Path != raw || boundary == nil || boundary.Path != raw || intent.Nodes["."] != boundary.Parent || len(intent.Nodes) != 5 {
		return fmt.Errorf("published raw capture receipt changed")
	}
	return nil
}
