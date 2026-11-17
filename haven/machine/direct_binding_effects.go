package machine

import (
	"context"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/loom"
)

// Bind verifies physical policy installation and binds cleanup to the supplied
// original retirement step. Sandbox validates its configuration before this call.
func (service *DirectPolicies) Bind(ctx context.Context, step DirectRetirementStep, expected *loom.NetworkPolicy) (func() error, error) {
	backend, ok := service.backend.(loom.NetworkPolicyBackend)
	if !ok {
		return nil, fmt.Errorf("%w: direct policy effects unavailable", errs.ErrIncompatible)
	}
	if err := backend.VerifyNetworkPolicy(ctx, step.Input.Source.Input.Resource, expected); err != nil {
		return nil, err
	}
	return func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return service.retireDirect(cleanupCtx, step.OperationID)
	}, nil
}
