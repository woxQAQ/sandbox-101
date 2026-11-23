package machine

import (
	"context"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

func fileIdentity(device, inode, mode int64) loom.FileIdentity {
	return loom.FileIdentity{
		Device: uint64(device),
		Inode:  uint64(inode),
		Mode:   uint32(mode),
	}
}

func launchObservation(p dbgen.ResourceProgress) *loom.Observation {
	if !p.ObservationPresent {
		return nil
	}
	return &loom.Observation{
		Alive:      p.Alive,
		Paused:     p.Paused,
		Booted:     p.Booted,
		Configured: p.Configured,
		PID:        int(p.Pid),
		StartTime:  uint64(p.StartTime),
		Network:    p.Network,
	}
}

func readMaterials(ctx context.Context, q *dbgen.Queries, id string, r loom.Resource) (*loom.RestoreMaterials, error) {
	row, err := q.ReadRestoreMaterials(ctx, id)
	if err != nil {
		return nil, err
	}
	m := &loom.RestoreMaterials{
		Resource: r,
		Version:  fileIdentity(row.VersionDevice, row.VersionInode, row.VersionMode),
		Restore:  fileIdentity(row.RestoreDevice, row.RestoreInode, row.RestoreMode),
		Complete: row.Complete,
	}
	files, err := q.ReadRestoreFiles(ctx, id)
	if err != nil {
		return nil, err
	}
	if row.FilesPresent {
		m.Files = make(map[string]loom.RestoreFileProof, len(files))
	} else if len(files) > 0 {
		return nil, fmt.Errorf("restore file inventory changed")
	}
	for _, f := range files {
		m.Files[f.Name] = loom.RestoreFileProof{
			Source:    f.Source,
			Candidate: f.Candidate,
			Size:      f.Size,
			Identity:  fileIdentity(f.Device, f.Inode, f.Mode),
		}
	}
	return m, nil
}

func replaceMaterials(ctx context.Context, q *dbgen.Queries, step ResourceStep, next *loom.RestoreMaterials) error {
	if step.State.Materials != nil {
		if err := store.RequireOne(q.ClearRestoreMaterials(ctx, step.OperationID)); err != nil {
			return err
		}
	}
	if next == nil {
		return nil
	}
	if !reflect.DeepEqual(step.Input.Resource, next.Resource) {
		return fmt.Errorf("restore materials belong to a different resource")
	}
	if err := store.RequireOne(q.InsertRestoreMaterials(ctx, dbgen.InsertRestoreMaterialsParams{
		OperationID:   step.OperationID,
		Complete:      next.Complete,
		FilesPresent:  next.Files != nil,
		VersionDevice: int64(next.Version.Device),
		VersionInode:  int64(next.Version.Inode),
		VersionMode:   int64(next.Version.Mode),
		RestoreDevice: int64(next.Restore.Device),
		RestoreInode:  int64(next.Restore.Inode),
		RestoreMode:   int64(next.Restore.Mode),
	})); err != nil {
		return err
	}
	for name, f := range next.Files {
		if err := store.RequireOne(q.InsertRestoreFile(ctx, dbgen.InsertRestoreFileParams{
			OperationID: step.OperationID,
			Name:        name,
			Source:      f.Source,
			Candidate:   f.Candidate,
			Size:        f.Size,
			Device:      int64(f.Identity.Device),
			Inode:       int64(f.Identity.Inode),
			Mode:        int64(f.Identity.Mode),
		})); err != nil {
			return err
		}
	}
	return nil
}
