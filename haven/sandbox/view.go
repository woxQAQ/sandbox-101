package sandbox

import (
	"time"

	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/types"
)

// View combines stable identity and current settings for business delivery.
// Environment values remain private and are excluded from serialized views.
type View struct {
	LastError       string            `json:"last_error,omitempty"`
	Epoch           string            `json:"creation_epoch"`
	CreatedAt       time.Time         `json:"created_at"`
	ProvisionSource string            `json:"provision_source,omitempty"`
	ResumeAttempt   uint64            `json:"resume_attempt,omitempty"`
	ID              string            `json:"id"`
	TenantID        string            `json:"tenant_id"`
	State           string            `json:"state"`
	VMID            string            `json:"vm_id,omitempty"`
	Instance        string            `json:"instance,omitempty"`
	SnapshotID      string            `json:"snapshot_id,omitempty"`
	TemplateID      string            `json:"template_id"`
	Shape           loom.Shape        `json:"shape"`
	Deadline        time.Time         `json:"deadline"`
	PausedUntil     time.Time         `json:"paused_until"`
	TTLSeconds      int64             `json:"ttl_seconds"`
	OnExpire        string            `json:"on_expire"`
	AutoRenew       bool              `json:"auto_renew"`
	Env             map[string]string `json:"-"`
	Policy          egress.Policy     `json:"policy"`
	RequiresSecrets bool              `json:"requires_secrets"`
	OperationID     string            `json:"operation_id,omitempty"`
}

func (v View) Public() types.Sandbox {
	return types.Sandbox{
		ID:              v.ID,
		TenantID:        v.TenantID,
		State:           v.State,
		VMID:            v.VMID,
		Instance:        v.Instance,
		SnapshotID:      v.SnapshotID,
		TemplateID:      v.TemplateID,
		ProvisionSource: v.ProvisionSource,
		Shape:           types.Shape(v.Shape),
		Deadline:        v.Deadline,
		PausedUntil:     v.PausedUntil,
		TTLSeconds:      v.TTLSeconds,
		OnExpire:        v.OnExpire,
		AutoRenew:       v.AutoRenew,
		Policy:          types.Policy(v.Policy),
		RequiresSecrets: v.RequiresSecrets,
		ResumeAttempt:   v.ResumeAttempt,
		Epoch:           v.Epoch,
		OperationID:     v.OperationID,
		LastError:       v.LastError,
		CreatedAt:       v.CreatedAt,
	}
}
