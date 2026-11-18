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

func (sr *SandboxRequests) Submit(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	switch command.Action {
	case "claim":
		return sr.sandboxes.AdmitClaim(ctx, principal, key, command)
	case "configure":
		return sr.sandboxes.AdmitConfigure(ctx, principal, key, command)
	case operation.ActionCheckpointDelete:
		return sr.lifecycle.AdmitCheckpointDelete(ctx, principal, key, command)
	case "refill":
		return sr.supply.AdmitRefill(ctx, principal, key, command)
	case "template_put", "template_build":
		return sr.supply.AdmitTemplate(ctx, principal, key, command)
	case "renew":
		return sr.lifecycle.AdmitRenew(ctx, principal, key, command)
	case "rollback":
		return sr.lifecycle.AdmitRollback(ctx, principal, key, command)
	case "fork":
		return sr.forks.AdmitFork(ctx, principal, key, command)
	case "resume":
		return sr.lifecycle.AdmitResume(ctx, principal, key, command)
	case "release", "checkpoint", "pause":
		return sr.lifecycle.AdmitLifecycle(ctx, principal, key, command)
	default:
		return operation.Record{}, fmt.Errorf("%w: unsupported sandbox action", errs.ErrInvalid)
	}
}

func (sr *SandboxRequests) Execute(ctx context.Context, principal authorization.Principal, key string, command operation.Command) (operation.Record, error) {
	accepted, err := sr.Submit(ctx, principal, key, command)
	if err != nil {
		return accepted, err
	}
	return sr.operations.Wait(ctx, principal, accepted.ID)
}
