package supply

import (
	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/types"
)

// TemplateRecord is a published or proposed template version. Publication is verified
// against the original supply workflow before a catalog record is used.
type TemplateRecord struct {
	Version    string      `json:"version"`
	State      string      `json:"state"`
	Warmup     string      `json:"warmup,omitempty"`
	ID         string      `json:"id"`
	TenantID   string      `json:"tenant_id"`
	Config     loom.Config `json:"config"`
	SnapshotID string      `json:"snapshot_id,omitempty"`
	TargetWarm int         `json:"target_warm"`
}

func (record TemplateRecord) Public() types.Template {
	return types.Template{
		ID:         record.ID,
		TenantID:   record.TenantID,
		Image:      record.Config.Image,
		Shape:      types.Shape(record.Config.Shape),
		SnapshotID: record.SnapshotID,
		State:      record.State,
		TargetWarm: record.TargetWarm,
		Warmup:     record.Warmup,
	}
}
