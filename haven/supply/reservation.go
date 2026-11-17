package supply

import (
	"time"

	"github.com/woxQAQ/haven/ownership"
)

// Reservation retains the template revision and original resource generation.
// Current VM ownership cannot replace Source when validating a warm handoff.
type PoolReservation struct {
	Template  TemplateRecord          `json:"template"`
	SandboxID string                  `json:"sandbox_id"`
	VMID      string                  `json:"vm_id"`
	Source    ownership.GenerationRef `json:"source"`
	Instance  string                  `json:"instance,omitempty"`
	CreatedAt time.Time               `json:"created_at"`
}
