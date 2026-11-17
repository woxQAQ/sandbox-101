package data

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/authorization"
	"github.com/woxQAQ/haven/internal/errs"
	operation "github.com/woxQAQ/haven/operation"
)

type previewCapability struct {
	Sandbox string `json:"s"`
	Tenant  string `json:"t"`
	Epoch   string `json:"e"`
	Port    int    `json:"p"`
	Expires int64  `json:"x"`
}

// PreviewTarget contains only the verified forwarding authority of a capability.
type PreviewTarget struct {
	Principal authorization.Principal
	SandboxID string
	Port      int
}

func (service *Data) PreviewURL(ctx context.Context, principal authorization.Principal, id string, port int, ttl time.Duration) (string, error) {
	sandbox, err := service.catalog.Get(ctx, principal, id)
	if err != nil {
		return "", err
	}
	if sandbox.State != "running" && sandbox.State != "paused" {
		return "", fmt.Errorf("%w: sandbox not available", errs.ErrConflict)
	}
	if port < 1 || port > 65535 || ttl <= 0 {
		return "", fmt.Errorf("%w: valid port and positive duration required", errs.ErrInvalid)
	}
	expires := time.Now().Add(ttl)
	if expires.After(sandbox.Deadline) {
		expires = sandbox.Deadline
	}
	if !expires.After(time.Now()) {
		return "", fmt.Errorf("%w: lease expired", errs.ErrConflict)
	}
	value := previewCapability{
		Sandbox: sandbox.ID,
		Tenant:  sandbox.TenantID,
		Epoch:   sandbox.Epoch,
		Port:    port,
		Expires: expires.Unix(),
	}
	body := base64.RawURLEncoding.EncodeToString(operation.Encode(value))
	return "/preview/" + body + "." + service.sign("preview-v1", body) + "/", nil
}

func (service *Data) ResolvePreview(ctx context.Context, body, signature string) (PreviewTarget, error) {
	denied := fmt.Errorf("%w: not found", errs.ErrNotFound)
	if !hmac.Equal([]byte(signature), []byte(service.sign("preview-v1", body))) {
		return PreviewTarget{}, denied
	}
	payload, err := base64.RawURLEncoding.DecodeString(body)
	var value previewCapability
	if err != nil || json.Unmarshal(payload, &value) != nil || value.Port < 1 || value.Port > 65535 || time.Now().Unix() >= value.Expires {
		return PreviewTarget{}, denied
	}
	principal := authorization.Principal{
		TenantID:  value.Tenant,
		SandboxID: value.Sandbox,
	}
	sandbox, err := service.catalog.Get(ctx, principal, value.Sandbox)
	if err != nil || sandbox.Epoch != value.Epoch || sandbox.State == "terminated" || value.Expires > sandbox.Deadline.Unix() {
		return PreviewTarget{}, denied
	}
	return PreviewTarget{
		Principal: principal,
		SandboxID: value.Sandbox,
		Port:      value.Port,
	}, nil
}
