// Package node exposes node status and controls admission draining.
package node

import (
	"context"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

type Metadata interface {
	// Identity returns the stable identity of the open store.
	Identity() store.Identity
	// Reviewed reports whether the store was opened for recovery review.
	Reviewed() bool
	// Epoch reads the current durable store epoch.
	Epoch(ctx context.Context) (int64, error)
}

type Service struct {
	metadata     Metadata
	availability *execution.Availability
}

func New(metadata Metadata, availability *execution.Availability) *Service {
	return &Service{metadata: metadata, availability: availability}
}

// ReadNodeStatus reads metadata under the caller's existing store custody.
func ReadNodeStatus(ctx context.Context, metadata Metadata) (types.NodeStatus, error) {
	status := types.NodeStatus{
		Format:           store.Format,
		Version:          store.Version,
		StoreID:          metadata.Identity().Identity,
		RecoveryReviewed: metadata.Reviewed(),
	}
	var err error
	status.Epoch, err = metadata.Epoch(ctx)
	return status, err
}

func (s *Service) Status(ctx context.Context, principal authorization.Principal) (types.NodeStatus, error) {
	if !principal.Root || principal.SandboxID != "" {
		return types.NodeStatus{}, errs.ErrForbidden
	}
	status, err := ReadNodeStatus(ctx, s.metadata)
	if err != nil {
		return status, err
	}
	status.Draining = s.availability.IsDraining()
	return status, nil
}

func (s *Service) Drain(principal authorization.Principal) error {
	if !principal.Root || principal.SandboxID != "" {
		return errs.ErrForbidden
	}
	s.availability.Drain()
	return nil
}
