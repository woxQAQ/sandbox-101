package loom

import (
	"context"
	"fmt"
	"path/filepath"
)

// SnapshotDeletionTarget binds the published snapshot identity to its frozen
// directory inode and exact artifact digests.
type SnapshotDeletionTarget struct {
	ID          string
	Publication FileIdentity
	Files       map[string]string
}

// InspectSnapshotDeletion requires the original published directory and fixed
// artifact digests before recording the first precise deletion receipt.
func InspectSnapshotDeletion(ctx context.Context, stateRoot string, target SnapshotDeletionTarget) (DeletionIntent, error) {
	id, publication, files := target.ID, target.Publication, target.Files
	var empty DeletionIntent
	if !ValidResourceID(id, "snap-") || publication.Inode == 0 || len(files) != 4 {
		return empty, fmt.Errorf("snapshot deletion publication proof missing")
	}
	path := filepath.Join(stateRoot, "snapshots", id)
	root, err := OpenCaptureDir(path)
	if err != nil {
		return empty, err
	}
	info, err := root.Stat()
	closeErr := root.Close()
	if err != nil {
		return empty, err
	}
	if closeErr != nil {
		return empty, closeErr
	}
	if IdentityOf(info) != publication {
		return empty, fmt.Errorf("snapshot deletion publication inode changed")
	}
	allowed := map[string]string{
		"manifest.json": "",
	}
	for _, name := range []string{
		"ch/config.json",
		"ch/state.json",
		"ch/memory-ranges",
		"disk.raw",
	} {
		if files[name] == "" {
			return empty, fmt.Errorf("snapshot artifact digest missing")
		}
		allowed[name] = files[name]
	}
	paths, err := OwnedDirectoryFiles(ctx, stateRoot, path, allowed)
	if err != nil {
		return empty, err
	}
	if len(paths) != 5 {
		return empty, fmt.Errorf("snapshot published artifact set incomplete")
	}
	intent, err := InspectDeletion(ctx, path, paths, true)
	if err != nil {
		return empty, err
	}
	return intent, VerifySnapshotDeletion(intent, path, publication)
}

func VerifySnapshotDeletion(intent DeletionIntent, path string, publication FileIdentity) error {
	if intent.Format != 1 || !intent.Dir || intent.Path != path || intent.Nodes["."] != publication {
		return fmt.Errorf("snapshot deletion receipt differs from publication identity")
	}
	for _, name := range []string{
		"ch",
		"ch/config.json",
		"ch/state.json",
		"ch/memory-ranges",
		"disk.raw",
		"manifest.json",
	} {
		if intent.Nodes[name].Inode == 0 {
			return fmt.Errorf("snapshot deletion receipt lacks published artifact")
		}
	}
	return nil
}
