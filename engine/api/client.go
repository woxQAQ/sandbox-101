package api

import (
	"context"
	"time"
)

type Client interface {
	Submit(context.Context, Request) (Operation, error)
	GetOperation(context.Context, string) (Operation, error)
	FindOperation(context.Context, string) (Operation, error)
	GetVM(context.Context, string) (VM, error)
	ListVMs(context.Context) ([]VM, error)
	GetSnapshot(context.Context, string) (Snapshot, error)
	ListSnapshots(context.Context) ([]Snapshot, error)
}

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
