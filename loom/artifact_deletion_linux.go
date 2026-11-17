package loom

import (
	"context"
	"fmt"
	"os"
)

// ArtifactTopology fixes producer-owned names, file kinds and historical inode
// identities. Inspection rejects any observed entry outside this declared tree.
type ArtifactTopology struct {
	Directories map[string]bool
	Files       map[string]os.FileMode
	Fixed       map[string]FileIdentity
}

// InspectOwnedArtifactTopology freezes only an explicitly prepared directory and
// its declared producer topology. Absence cannot establish initial ownership.
func InspectOwnedArtifactTopology(ctx context.Context, stateRoot, path string, topology ArtifactTopology) (DeletionIntent, error) {
	directories, fixed := topology.Directories, topology.Fixed
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
	intent, _, err := inspectArtifactTopology(ctx, stateRoot, path, topology)
	return intent, err
}
