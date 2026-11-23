package quota

import (
	"context"

	"github.com/woxQAQ/haven/quota/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// CapacityUsage sums node reservations without assigning them to a new owner.
type CapacityUsage struct {
	Bytes     int64
	MemoryMiB int64
	CPUs      int64
}

func ReservedCapacity(ctx context.Context, db store.DBTX) (CapacityUsage, error) {
	row, err := dbgen.New(db).ReservedCapacity(ctx)
	return CapacityUsage{
		Bytes:     row.StorageBytes,
		MemoryMiB: row.MemoryMib,
		CPUs:      row.Vcpus,
	}, err
}
