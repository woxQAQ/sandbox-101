package operation

import (
	"github.com/woxQAQ/haven/loom"
)

// Template is the accepted template specification used in frozen commands.
// The host shape and JSON field order are retained independently of wire DTOs.
type TemplateSpec struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Image      string     `json:"image"`
	Shape      loom.Shape `json:"shape"`
	SnapshotID string     `json:"snapshot_id,omitempty"`
	State      string     `json:"state"`
	TargetWarm int        `json:"target_warm"`
	Warmup     string     `json:"warmup,omitempty"`
}
