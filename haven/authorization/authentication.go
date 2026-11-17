package authorization

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"fmt"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/ownership"
)

// CapabilityReader searches sandbox capabilities without exposing settings,
// admission, workflow dispatch or database ownership to authentication.
type CapabilityReader interface {
	FindCapability(context.Context, bool, func(ownership.Sandbox) bool) (ownership.Sandbox, error)
}

// Authentication resolves an administrator, tenant or sandbox capability. It has
// no workflow runner, runtime effects, transaction ownership or secret registry.
type Authentication struct {
	rootToken string
	tenants   *Repository
	sandboxes CapabilityReader
	sign      func(...string) string
}

func NewAuthentication(rootToken string, tenants *Repository, sandboxes CapabilityReader, sign func(...string) string) *Authentication {
	return &Authentication{
		rootToken: rootToken,
		tenants:   tenants,
		sandboxes: sandboxes,
		sign:      sign,
	}
}

func (s *Authentication) Authenticate(ctx context.Context, token string) (Principal, error) {
	return s.authenticate(ctx, token, false)
}

// AuthenticateOperation permits retired capabilities only for history lookup;
// the operation use case must additionally enforce the original operation scope.
func (s *Authentication) AuthenticateOperation(ctx context.Context, token string) (Principal, error) {
	return s.authenticate(ctx, token, true)
}

func (s *Authentication) authenticate(ctx context.Context, token string, retired bool) (Principal, error) {
	if token == "" {
		return Principal{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
	}
	if s.rootToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(s.rootToken)) == 1 {
		return Principal{
			TenantID: "root",
			Root:     true,
		}, nil
	}
	tenant, err := s.tenants.TenantForToken(ctx, token)
	if err == nil {
		return Principal{
			TenantID: tenant,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Principal{}, err
	}
	candidate, err := s.sandboxes.FindCapability(ctx, retired, func(candidate ownership.Sandbox) bool {
		return subtle.ConstantTimeCompare([]byte(token), []byte(s.SandboxToken(candidate.TenantID, candidate.ID))) == 1
	})
	if err == nil {
		return Principal{
			TenantID:  candidate.TenantID,
			SandboxID: candidate.ID,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Principal{}, err
	}
	return Principal{}, fmt.Errorf("%w: not found", errs.ErrNotFound)
}

func (s *Authentication) SandboxToken(tenant, id string) string {
	return s.sign("sandbox-token-v1", tenant, id)
}
