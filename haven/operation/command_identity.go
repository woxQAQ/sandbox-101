package operation

import (
	"context"
	"crypto/hmac"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/access"
	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/internal/errs"
)

// CommandIdentity binds credential-bearing retries to the original authenticated
// role and accepted operation. Complete secrets remain only in the shared registry.
type CommandIdentity struct {
	ctx        context.Context
	operations *Repository
	view       *Operations
	access     *access.Registry
	sign       func(...string) string
}

type CommandIdentityDependencies struct {
	Operations *Repository
	View       *Operations
	Registry   *access.Registry
	Sign       func(...string) string
}

func NewCommandIdentity(ctx context.Context, dependencies CommandIdentityDependencies) *CommandIdentity {
	return &CommandIdentity{
		ctx:        ctx,
		operations: dependencies.Operations,
		view:       dependencies.View,
		access:     dependencies.Registry,
		sign:       dependencies.Sign,
	}
}

func (service *CommandIdentity) CommandMAC(principal authorization.Principal, command Command) string {
	role := "tenant"
	if principal.Root {
		role = "root"
	} else if principal.SandboxID != "" {
		role = "sandbox"
	}
	return role + ":" + service.sign("sandbox-command-v1", string(Encode(principal)), string(Encode(command)))
}

// CommandCredentials binds the stored credential MAC to the ephemeral secrets
// supplied for that same retry. Secrets are restored only after MAC verification.
type CommandCredentials struct {
	MAC     string
	Secrets map[string]egress.Secret
}

// Existing preserves scoped publication and original credential matching before
// restoring the accepted request's in-memory secrets.
func (service *CommandIdentity) Existing(ctx context.Context, principal authorization.Principal, key string, credentials CommandCredentials) (Record, bool, error) {
	id, err := service.operations.RequestID(ctx, principal.TenantID, key)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, false, nil
	}
	if err != nil {
		return Record{}, false, err
	}
	accepted, err := service.view.Operation(ctx, principal, id)
	if err != nil {
		return accepted, true, err
	}
	if err = service.MatchCredentials(id, credentials.MAC); err != nil {
		return accepted, true, err
	}
	if !Terminal(accepted.Status) {
		service.access.SetOperationSecrets(id, credentials.Secrets)
	}
	return accepted, true, nil
}

func (service *CommandIdentity) MatchCredentials(operationID, mac string) error {
	expected, err := service.operations.CredentialMAC(service.ctx, operationID)
	if err != nil {
		return err
	}
	if !hmac.Equal([]byte(expected), []byte(mac)) {
		return fmt.Errorf("%w: idempotency key has different parameters", errs.ErrConflict)
	}
	return nil
}
