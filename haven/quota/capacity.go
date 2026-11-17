package quota

import "context"

// CapacityUsage sums node reservations without assigning them to a new owner.
type CapacityUsage struct {
	Bytes     int64
	MemoryMiB int64
	CPUs      int64
}

func (r *Repository) ReservedCapacity(ctx context.Context) (CapacityUsage, error) {
	row, err := r.queries.ReservedCapacity(ctx)
	return CapacityUsage{
		Bytes:     row.Bytes,
		MemoryMiB: row.Memory,
		CPUs:      row.Cpus,
	}, err
}
