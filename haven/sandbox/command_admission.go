package sandbox

import (
	"context"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

// CommandAdmission checks capability scope before dispatching each intent to its admission service.
type CommandAdmission struct {
	sandboxes        *Sandboxes
	lifecycle        *Lifecycle
	supply           *supply.Supply
	forks            *Forks
	operationMonitor *operation.Monitor
}

func NewCommandAdmission(sandboxes *Sandboxes, lifecycle *Lifecycle, supply *supply.Supply, forks *Forks, operations *operation.Monitor) *CommandAdmission {
	return &CommandAdmission{
		sandboxes:        sandboxes,
		lifecycle:        lifecycle,
		supply:           supply,
		forks:            forks,
		operationMonitor: operations,
	}
}

func (sr *CommandAdmission) Submit(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	// A sandbox capability admits only renewal or release of its own sandbox.
	// This admission boundary covers direct service calls, not only HTTP.
	if principal.SandboxID != "" && (command.SandboxID != principal.SandboxID || (command.Action != "renew" && command.Action != "release")) {
		return operation.State{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	switch command.Action {
	case "claim":
		return sr.sandboxes.AdmitClaim(ctx, principal, key, command)
	case "configure":
		return sr.sandboxes.AdmitConfigure(ctx, principal, key, command)
	case "checkpoint_delete":
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
		return operation.State{}, fmt.Errorf("%w: unsupported sandbox action", errs.ErrInvalid)
	}
}

func (sr *CommandAdmission) Execute(ctx context.Context, principal authorization.Principal, key string, command operation.Intent) (operation.State, error) {
	accepted, err := sr.Submit(ctx, principal, key, command)
	if err != nil {
		return accepted, err
	}
	return sr.operationMonitor.Wait(ctx, principal, accepted.ID)
}
