package image

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/image/internal/dbgen"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/loom/imageimport"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

func artifactValue(row dbgen.ImageArtifactValue) (host.ImageArtifact, error) {
	a := host.ImageArtifact{
		Role:        row.Role,
		Path:        row.Path,
		SHA256:      row.Sha256,
		DiffID:      row.DiffID,
		DeviceID:    int(row.DeviceID),
		GuestPath:   row.GuestPath,
		Serial:      row.Serial,
		CacheSHA256: row.CacheSha256,
	}
	if operation.Digest(operation.Encode(a)) != row.ArtifactHash {
		return a, fmt.Errorf("image artifact witness changed")
	}
	return a, nil
}

func retainArtifact(ctx context.Context, q *dbgen.Queries, a host.ImageArtifact) (string, error) {
	hash := operation.Digest(operation.Encode(a))
	row, err := q.GetArtifactValue(ctx, hash)
	if err == nil {
		_, err = artifactValue(row)
		return hash, err
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	return hash, store.RequireOne(q.InsertArtifactValue(ctx, dbgen.InsertArtifactValueParams{
		ArtifactHash: hash,
		Role:         a.Role,
		Path:         a.Path,
		Sha256:       a.SHA256,
		DiffID:       a.DiffID,
		DeviceID:     int64(a.DeviceID),
		GuestPath:    a.GuestPath,
		Serial:       a.Serial,
		CacheSha256:  a.CacheSHA256,
	}))
}

func retainCacheWitness(ctx context.Context, q *dbgen.Queries, cache *imageimport.ImageCacheWitness) (string, error) {
	hash := operation.Digest(operation.Encode(cache))
	row, err := q.GetCacheWitness(ctx, hash)
	if err == nil {
		_, err = cacheWitness(row)
		return hash, err
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	return hash, store.RequireOne(q.InsertCacheWitness(ctx, dbgen.InsertCacheWitnessParams{
		WitnessHash:     hash,
		Builder:         cache.Cache.Builder,
		DiffID:          cache.Cache.DiffID,
		PhysicalBytes:   cache.PhysicalBytes,
		Path:            cache.Path,
		ReceiptSha256:   cache.ReceiptSHA256,
		DirectoryDevice: int64(cache.Directory.Device),
		DirectoryInode:  int64(cache.Directory.Inode),
		DirectoryMode:   int64(cache.Directory.Mode),
		DataDevice:      int64(cache.Data.Device),
		DataInode:       int64(cache.Data.Inode),
		DataMode:        int64(cache.Data.Mode),
		ReceiptDevice:   int64(cache.Receipt.Device),
		ReceiptInode:    int64(cache.Receipt.Inode),
		ReceiptMode:     int64(cache.Receipt.Mode),
		Format:          int64(cache.Cache.Format),
		Profile:         cache.Cache.Profile,
		Sha256:          cache.Cache.SHA256,
		TarBytes:        cache.Cache.Bytes,
		Entries:         cache.Cache.Entries,
	}))
}

func preparationIdentity(device, inode, mode int64) loom.FileIdentity {
	return loom.FileIdentity{
		Device: uint64(device),
		Inode:  uint64(inode),
		Mode:   uint32(mode),
	}
}

func readPreparation(ctx context.Context, q *dbgen.Queries, id string) (*imageimport.ImageImportBoundary, error) {
	b, err := q.GetImagePreparation(ctx, id)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &imageimport.ImageImportBoundary{
		Path:   b.Path,
		Work:   preparationIdentity(b.WorkDevice, b.WorkInode, b.WorkMode),
		Output: preparationIdentity(b.OutputDevice, b.OutputInode, b.OutputMode),
	}, nil
}

func recordPreparation(ctx context.Context, q *dbgen.Queries, id string, previous, next *imageimport.ImageImportBoundary) error {
	if reflect.DeepEqual(previous, next) {
		return nil
	}
	if previous != nil || next == nil {
		return fmt.Errorf("image preparation changed")
	}
	return store.RequireOne(q.InsertImagePreparation(ctx, dbgen.InsertImagePreparationParams{
		OperationID:  id,
		Path:         next.Path,
		WorkDevice:   int64(next.Work.Device),
		WorkInode:    int64(next.Work.Inode),
		WorkMode:     int64(next.Work.Mode),
		OutputDevice: int64(next.Output.Device),
		OutputInode:  int64(next.Output.Inode),
		OutputMode:   int64(next.Output.Mode),
	}))
}
