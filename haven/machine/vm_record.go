package machine

import "github.com/woxQAQ/haven/loom"

// Record is the persisted candidate and its observed readiness. It supplies no
// substitute for an independently retained instance/version ownership witness.
type VMRecord struct {
	Resource    loom.Resource     `json:"resource"`
	Observation *loom.Observation `json:"observation,omitempty"`
	Verified    bool              `json:"verified"`
	AgentReady  bool              `json:"agent_ready"`
	Deleting    bool              `json:"deleting"`
}

// RetainedVersion retains the admitted resource and disk identity for one generation.
type RetainedVersion struct {
	Resource     loom.Resource      `json:"resource"`
	DiskIdentity *loom.FileIdentity `json:"disk_identity,omitempty"`
}
