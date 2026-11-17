package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
)

type SupplyCommands interface {
	AdmitRefill(context.Context, authorization.Principal, string, operation.Command) (operation.Record, error)
	AdmitTemplate(context.Context, authorization.Principal, string, operation.Command) (operation.Record, error)
}

// SandboxRequests routes accepted command kinds to their owning business service.
// It owns no persistence, execution state, or resource effects.
type SandboxRequests struct {
	sandboxes  *Sandboxes
	lifecycle  *Lifecycle
	supply     SupplyCommands
	forks      *Forks
	operations *operation.Operations
}

type SandboxRequestDependencies struct {
	Sandboxes  *Sandboxes
	Lifecycle  *Lifecycle
	Supply     SupplyCommands
	Forks      *Forks
	Operations *operation.Operations
}

func NewSandboxRequests(dependencies SandboxRequestDependencies) *SandboxRequests {
	return &SandboxRequests{
		sandboxes:  dependencies.Sandboxes,
		lifecycle:  dependencies.Lifecycle,
		supply:     dependencies.Supply,
		forks:      dependencies.Forks,
		operations: dependencies.Operations,
	}
}

func (service *SandboxRequests) Submit(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	switch command.Action {
	case "claim":
		return service.sandboxes.AdmitClaim(ctx, principal, key, command)
	case "configure":
		return service.sandboxes.AdmitConfigure(ctx, principal, key, command)
	case "checkpoint_delete":
		return service.lifecycle.AdmitCheckpointDelete(ctx, principal, key, command)
	case "refill":
		return service.supply.AdmitRefill(ctx, principal, key, command)
	case "template_put", "template_build":
		return service.supply.AdmitTemplate(ctx, principal, key, command)
	case "renew":
		return service.lifecycle.AdmitRenew(ctx, principal, key, command)
	case "rollback":
		return service.lifecycle.AdmitRollback(ctx, principal, key, command)
	case "fork":
		return service.forks.AdmitFork(ctx, principal, key, command)
	case "resume":
		return service.lifecycle.AdmitResume(ctx, principal, key, command)
	case "release", "checkpoint", "pause":
		return service.lifecycle.AdmitLifecycle(ctx, principal, key, command)
	default:
		return operation.Record{}, fmt.Errorf("%w: unsupported sandbox action", errs.ErrInvalid)
	}
}

func (service *SandboxRequests) Execute(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	accepted, err := service.Submit(ctx, principal, key, command)
	if err != nil {
		return accepted, err
	}
	return service.operations.Wait(ctx, principal, accepted.ID)
}
