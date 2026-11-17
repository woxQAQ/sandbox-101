package ownership

import (
	"time"

	"github.com/woxQAQ/haven/loom"
)

// Identity fixes business birth independently of VM generations and pool age.
type Birth struct {
	TemplateID      string     `json:"template_id"`
	Shape           loom.Shape `json:"shape"`
	Epoch           string     `json:"creation_epoch"`
	CreatedAt       time.Time  `json:"created_at"`
	ProvisionSource string     `json:"provision_source"`
}

type Sandbox struct {
	Identity        Birth          `json:"identity"`
	LastError       string         `json:"last_error,omitempty"`
	SlotAssignment  *GenerationRef `json:"slot_assignment,omitempty"`
	ID              string         `json:"id"`
	TenantID        string         `json:"tenant_id"`
	OperationID     string         `json:"operation_id"`
	VMID            string         `json:"vm_id"`
	Instance        string         `json:"instance,omitempty"`
	State           string         `json:"state"`
	SnapshotID      string         `json:"snapshot_id,omitempty"`
	SourceOperation string         `json:"source_operation"`
}

// GenerationRef names an immutable completed resource step without copying ancestry.
type GenerationRef struct {
	OperationID string `json:"operation_id"`
	InputHash   string `json:"input_hash"`
}
