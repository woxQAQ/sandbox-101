package operation

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/types"
)

const ScopeTenant = "tenant"

// Scope keeps an operation's durable authorization kind and identity together.
// The caller must still establish authority before querying that scope.
type Scope struct {
	Kind string
	ID   string
}

func (r *Repository) StepTarget(ctx context.Context, id, key, hash string) (candidate, expected string, err error) {
	row, err := r.queries.GetStepTarget(ctx, dbgen.GetStepTargetParams{
		OperationID: id,
		StepKey:     key,
		InputHash:   hash,
	})
	return row.CandidateResourceID, row.ExpectedInstance, err
}

func (r *Repository) LiveResult(ctx context.Context, id string) (Result, error) {
	status, result, err := r.ResultState(ctx, id)
	if err == nil && Terminal(status) {
		return Result{}, sql.ErrNoRows
	}
	return result, err
}

func (r *Repository) ReplaceLiveResult(ctx context.Context, id string, result Result) error {
	return r.updateResult(ctx, id, "", result)
}

// CommandIdentity binds credential-bearing retries to the original authenticated
// role and accepted operation. Complete secrets remain only in the shared registry.
type CommandIdentity struct {
	ctx              context.Context
	repository       *Repository
	operationMonitor *Monitor
	registry         *ephemeral.Registry
	sign             func(...string) string
}

func NewCommandIdentity(ctx context.Context, repository *Repository, operations *Monitor, registry *ephemeral.Registry, sign func(...string) string) *CommandIdentity {
	return &CommandIdentity{
		ctx:              ctx,
		repository:       repository,
		operationMonitor: operations,
		registry:         registry,
		sign:             sign,
	}
}

func (ci *CommandIdentity) CommandMAC(principal authorization.Principal, command Intent) string {
	role := "tenant"
	if principal.Root {
		role = "root"
	} else if principal.SandboxID != "" {
		role = "sandbox"
	}
	return role + ":" + ci.sign("sandbox-command-v1", string(Encode(principal)), string(Encode(command)))
}

// CommandCredentials binds the stored credential MAC to the ephemeral secrets
// supplied for that same retry. Secrets are restored only after MAC verification.
type CommandCredentials struct {
	MAC     string
	Secrets map[string]egress.Secret
}

// Existing preserves scoped publication and original credential matching before
// restoring the accepted request's in-memory secrets.
func (ci *CommandIdentity) Existing(ctx context.Context, principal authorization.Principal, key string, credentials CommandCredentials) (State, bool, error) {
	id, err := ci.repository.RequestID(ctx, principal.TenantID, key)
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	accepted, err := ci.operationMonitor.Get(ctx, principal, id)
	if err != nil {
		return accepted, true, err
	}
	if err = ci.MatchCredentials(id, credentials.MAC); err != nil {
		return accepted, true, err
	}
	if !Terminal(accepted.Status) {
		ci.registry.SetOperationSecrets(id, credentials.Secrets)
	}
	return accepted, true, nil
}

func (ci *CommandIdentity) MatchCredentials(operationID, mac string) error {
	expected, err := ci.repository.CredentialMAC(ci.ctx, operationID)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(expected), []byte(mac)) {
		return fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
	}
	return nil
}

// AcceptedCommand projects the original tenant command, including commands served by provisioning.
func AcceptedCommand(request Request) (types.Command, error) {
	c := request.Command
	if c == nil {
		return types.Command{}, fmt.Errorf("%w: public operation lacks accepted command", errs.ErrManualRecovery)
	}
	command := types.Command{
		Action:        c.Action,
		Template:      c.Template,
		SandboxID:     c.SandboxID,
		CheckpointID:  c.CheckpointID,
		TemplateID:    c.TemplateID,
		TTLSeconds:    c.TTLSeconds,
		OnExpire:      c.OnExpire,
		AutoRenew:     c.AutoRenew,
		LeaseDeadline: c.LeaseDeadline,
		Env:           c.Env,
		Policy:        c.Policy,
		Count:         c.Count,
	}
	if command.Action == "" {
		return command, fmt.Errorf("%w: public operation lacks accepted command", errs.ErrManualRecovery)
	}
	// Checkpoint deletion is stored as "checkpoint_delete" and published as
	// "delete_checkpoint"; the reverse translation lives in the API delete-checkpoint route.
	if command.Action == "checkpoint_delete" {
		command.Action = "delete_checkpoint"
	}
	return command, nil
}
