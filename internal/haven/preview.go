package haven

import (
	"context"
	"crypto/hmac"
	"encoding/base64"
	"encoding/json"
	"time"

	"github.com/woxQAQ/haven/loom"
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
	Principal Principal
	SandboxID string
	Port      int
}

func (service *Service) PreviewURL(ctx context.Context, principal Principal, id string, port int, ttl time.Duration) (string, error) {
	sandbox, err := service.Get(ctx, principal, id)
	if err != nil {
		return "", err
	}
	if sandbox.State != "running" && sandbox.State != "paused" {
		return "", loom.Fail(loom.Conflict, "sandbox not available")
	}
	if port < 1 || port > 65535 || ttl <= 0 {
		return "", loom.Fail(loom.Invalid, "valid port and positive duration required")
	}
	expires := time.Now().Add(ttl)
	if expires.After(sandbox.Deadline) {
		expires = sandbox.Deadline
	}
	if !expires.After(time.Now()) {
		return "", loom.Fail(loom.Conflict, "lease expired")
	}
	value := previewCapability{Sandbox: sandbox.ID, Tenant: sandbox.TenantID, Epoch: sandbox.Epoch, Port: port, Expires: expires.Unix()}
	body := base64.RawURLEncoding.EncodeToString(encode(value))
	return "/preview/" + body + "." + service.store.sign("preview-v1", body) + "/", nil
}

func (service *Service) ResolvePreview(ctx context.Context, body, signature string) (PreviewTarget, error) {
	denied := loom.Fail(loom.NotFound, "not found")
	if !hmac.Equal([]byte(signature), []byte(service.store.sign("preview-v1", body))) {
		return PreviewTarget{}, denied
	}
	payload, err := base64.RawURLEncoding.DecodeString(body)
	var value previewCapability
	if err != nil || json.Unmarshal(payload, &value) != nil || value.Port < 1 || value.Port > 65535 || time.Now().Unix() >= value.Expires {
		return PreviewTarget{}, denied
	}
	principal := Principal{TenantID: value.Tenant, SandboxID: value.Sandbox}
	sandbox, err := service.Get(ctx, principal, value.Sandbox)
	if err != nil || sandbox.Epoch != value.Epoch || sandbox.State == "terminated" || value.Expires > sandbox.Deadline.Unix() {
		return PreviewTarget{}, denied
	}
	return PreviewTarget{Principal: principal, SandboxID: value.Sandbox, Port: value.Port}, nil
}
