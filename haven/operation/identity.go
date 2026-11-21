package operation

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/ephemeral"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/operation/internal/dbgen"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/types"
)

// Scope keeps an operation's durable authorization kind and identity together.
// The caller must still establish authority before querying that scope.
type Scope struct {
	Kind string
	ID   string
}

// RequestIdentity keeps the original request alongside its current status.
// Readers verify the stored bytes; current resource ownership cannot replace it.
type RequestIdentity struct {
	Status    string
	ScopeKind string
	ScopeID   string
	Hash      string
	Request   []byte
}

func (r *Repository) RequestIdentity(ctx context.Context, id string) (RequestIdentity, error) {
	row, err := r.queries.GetRequestIdentity(ctx, id)
	return RequestIdentity{
		Status:    row.Status,
		ScopeKind: row.ScopeKind,
		ScopeID:   row.ScopeID,
		Hash:      row.RequestHash,
		Request:   row.Request,
	}, err
}

func (r *Repository) StepTarget(ctx context.Context, id, key, hash string) (candidate, expected string, err error) {
	row, err := r.queries.GetStepTarget(ctx, dbgen.GetStepTargetParams{
		OperationID: id,
		StepKey:     key,
		InputHash:   hash,
	})
	return row.CandidateResourceID, row.ExpectedInstance, err
}

func (r *Repository) LiveBody(ctx context.Context, id string) ([]byte, error) {
	return r.queries.GetLiveOperationBody(ctx, id)
}

func (r *Repository) ReplaceLiveBody(ctx context.Context, id string, body []byte) error {
	return store.RequireOne(r.queries.ReplaceLiveOperationBody(ctx, dbgen.ReplaceLiveOperationBodyParams{
		Body: body,
		ID:   id,
	}))
}

// CommandIdentity binds credential-bearing retries to the original authenticated
// role and accepted operation. Complete secrets remain only in the shared registry.
type CommandIdentity struct {
	ctx        context.Context
	operations *Repository
	view       *Monitor
	registry   *ephemeral.Registry
	sign       func(...string) string
}

func NewCommandIdentity(ctx context.Context, operations *Repository, view *Monitor, registry *ephemeral.Registry, sign func(...string) string) *CommandIdentity {
	return &CommandIdentity{
		ctx:        ctx,
		operations: operations,
		view:       view,
		registry:   registry,
		sign:       sign,
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
	id, err := ci.operations.RequestID(ctx, principal.TenantID, key)
	if errors.Is(err, sql.ErrNoRows) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	accepted, err := ci.view.Operation(ctx, principal, id)
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
	expected, err := ci.operations.CredentialMAC(ci.ctx, operationID)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(expected), []byte(mac)) {
		return fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
	}
	return nil
}

// AcceptedCommand reads the sanitized immutable request envelope. It does not
// infer an action from the current object or a later operation.
func AcceptedCommand(request []byte) (types.Command, error) {
	var command types.Command
	var envelope struct {
		Request json.RawMessage
		Claim   json.RawMessage
	}
	if err := json.Unmarshal(request, &envelope); err != nil {
		return command, err
	}
	payload := envelope.Request
	if len(envelope.Claim) > 0 {
		payload = envelope.Claim
	}
	if err := json.Unmarshal(payload, &command); err != nil {
		return command, err
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
