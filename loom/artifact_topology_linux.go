package loom

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func inspectArtifactTopology(ctx context.Context, stateRoot, root string, topology ArtifactTopology) (DeletionIntent, []string, error) {
	intent := DeletionIntent{
		Format: 1,
		Path:   root,
		Dir:    true,
		Nodes:  map[string]FileIdentity{},
	}
	if _, err := os.Lstat(root); os.IsNotExist(err) {
		return intent, nil, nil
	}
	if err := ValidatePath(ctx, stateRoot, root, true); err != nil {
		return intent, nil, err
	}
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		relative, _ := filepath.Rel(root, path)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if expected, known := topology.Fixed[relative]; known && IdentityOf(info) != expected {
			return fmt.Errorf("historical deletion inode changed: %s", path)
		}
		if entry.IsDir() {
			if !topology.Directories[relative] {
				return fmt.Errorf("unknown VM directory retained: %s", path)
			}
		} else {
			kind, known := topology.Files[relative]
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
	if err = rejectDeletionMounts(root); err != nil {
		return intent, nil, err
	}
	return intent, paths, nil
}
