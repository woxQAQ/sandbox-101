package sandboxd

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/woxQAQ/sandbox-engine/api"
)

func (s *Service) Authenticate(
	ctx context.Context,
	token string,
) (Principal, error) {
	return s.authenticateToken(ctx, token, false)
}

// Terminated credentials remain usable only for authorized operation polling.
func (s *Service) authenticateToken(
	ctx context.Context,
	token string,
	allowTerminated bool,
) (Principal, error) {
	if token == "" {
		return Principal{}, api.Fail(api.NotFound, "not found")
	}
	if hmac.Equal(
		[]byte(hashToken(token)),
		[]byte(hashToken(s.opts.RootToken)),
	) {
		return Principal{Root: true, TenantID: "root"}, nil
	}
	var id string
	err := s.store.db.QueryRowContext(ctx, "SELECT id FROM tenants WHERE token_hash=?", hashToken(token)).
		Scan(&id)
	if err == nil {
		return Principal{TenantID: id}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Principal{}, err
	}
	// A token carries an opaque id prefix solely to avoid scanning all sandboxes.
	if parts := strings.SplitN(token, ".", 2); len(parts) == 2 {
		b, err := getSandbox(ctx, s.store.db, parts[0])
		if err == nil && (b.State != "terminated" || allowTerminated) &&
			hmac.Equal([]byte(parts[1]), []byte(s.store.token(b))) {
			return Principal{TenantID: b.TenantID, SandboxID: b.ID}, nil
		}
		if err != nil && !isCode(err, api.NotFound) {
			return Principal{}, err
		}
	}
	return Principal{}, api.Fail(api.NotFound, "not found")
}

func (s *Service) issuedToken(
	b Sandbox,
) string {
	return b.ID + "." + s.store.token(b)
}

func visible(p Principal, tenant, id string) bool {
	return p.Root ||
		(p.TenantID == tenant && (p.SandboxID == "" || p.SandboxID == id))
}

func (s *store) sign(parts ...string) string {
	h := hmac.New(sha256.New, s.key)
	for _, p := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hashToken(
	v string,
) string {
	h := sha256.Sum256([]byte(v))
	return hex.EncodeToString(h[:])
}

func (s *store) token(
	b Sandbox,
) string {
	return s.sign("sandbox-token-v1", b.ID, b.TenantID, b.Epoch)
}

// Principal identifies the root, tenant, or sandbox scope used for authorization.
type Principal struct {
	TenantID  string
	SandboxID string
	Root      bool
}
