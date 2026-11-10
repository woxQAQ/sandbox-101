package api

import (
	"context"
	"time"
)

// Client is the durable operation and inventory contract shared by the engine
// and its UDS client. Submission acknowledges acceptance, not completion.
type Client interface {
	// Submit durably accepts a request. The same key and parameters return the
	// original operation; reusing the key with different parameters conflicts.
	// Cancellation does not revoke an already accepted operation.
	Submit(context.Context, Request) (Operation, error)
	// GetOperation looks up an operation by ID, including its fixed terminal
	// result. Operation records outlive the VM or snapshot they acted on.
	GetOperation(context.Context, string) (Operation, error)
	// FindOperation looks up an operation by its request's idempotency key,
	// allowing recovery after a lost submission response.
	FindOperation(context.Context, string) (Operation, error)
	// GetVM returns the VM record by ID. Its phase is the last confirmed phase;
	// verification and instance fields describe the recorded runtime evidence.
	GetVM(context.Context, string) (VM, error)
	// ListVMs returns all current VM records, including those being deleted.
	// Inventory is not proof that a VMM is alive or that an absent ID is reclaimed.
	ListVMs(context.Context) ([]VM, error)
	// GetSnapshot returns the snapshot record by ID, whether or not it is
	// published or being deleted. Only published, nondeleting snapshots are usable.
	GetSnapshot(context.Context, string) (Snapshot, error)
	// ListSnapshots returns all current snapshot records, including unpublished
	// and deleting snapshots; it does not acquire restore leases.
	ListSnapshots(context.Context) ([]Snapshot, error)
}

// Wait submits r and polls its operation until success, failure, a client error,
// or context cancellation. A failed operation returns its recorded error (or
// Internal if absent). Cancellation stops waiting, not the accepted work.
// Retrying with the same request recovers the original operation and result.
func Wait(ctx context.Context, c Client, r Request) (Operation, error) {
	op, err := c.Submit(ctx, r)
	if err != nil {
		return op, err
	}
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		switch op.Status {
		case "succeeded":
			return op, nil
		case "failed":
			if op.Error != nil {
				return op, op.Error
			}
			return op, Fail(Internal, "operation failed")
		}
		select {
		case <-ctx.Done():
			return op, ctx.Err()
		case <-t.C:
		}
		op, err = c.GetOperation(ctx, op.ID)
		if err != nil {
			return op, err
		}
	}
}
