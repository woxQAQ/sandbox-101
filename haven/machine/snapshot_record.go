package machine

import (
	"encoding/json"

	"github.com/woxQAQ/haven/loom"
)

// Manifest preserves the frozen capture representation, including field order.
type Manifest struct {
	Format          int               `json:"format"`
	Backend         string            `json:"backend"`
	Architecture    string            `json:"architecture"`
	CPU             string            `json:"cpu"`
	Config          loom.Config       `json:"config"`
	MemoryMode      string            `json:"memory_mode"`
	PublicationMode string            `json:"publication_mode"`
	Files           map[string]string `json:"files"`
	Image           json.RawMessage   `json:"image"`
}

type SnapshotRecord struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	OperationID    string    `json:"operation_id"`
	SourceVM       string    `json:"source_vm"`
	SourceInstance string    `json:"source_instance"`
	Dir            string    `json:"dir"`
	Manifest       *Manifest `json:"manifest,omitempty"`
}

// Source retains immutable provenance without importing a resource workflow.
// The application must verify the original capture and current responsibility.
type Source struct {
	Snapshot         SnapshotRecord    `json:"snapshot"`
	CaptureOperation string            `json:"capture_operation"`
	CaptureHash      string            `json:"capture_hash"`
	Publication      loom.FileIdentity `json:"publication"`
}
