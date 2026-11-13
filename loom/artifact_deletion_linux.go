package loom

import (
	"context"
	"fmt"
	"os"
)

// InspectOwnedArtifactTopology freezes only an explicitly prepared directory and
// its declared producer topology. Absence cannot establish initial ownership.
func InspectOwnedArtifactTopology(ctx context.Context, stateRoot, path string, directories map[string]bool, files map[string]os.FileMode, fixed map[string]FileIdentity) (DeletionIntent, error) {
	var empty DeletionIntent
	if !directories["."] || fixed["."].Inode == 0 {
		return empty, fmt.Errorf("artifact deletion lacks original root identity")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return empty, err
	}
	if !info.IsDir() || IdentityOf(info) != fixed["."] {
		return empty, fmt.Errorf("artifact root identity changed")
	}
	intent, _, err := inspectVMFileTopology(ctx, stateRoot, Resource{Dir: path}, directories, files, fixed)
	return intent, err
}
