package sandbox

import (
	"github.com/woxQAQ/haven/egress"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
)

// ClaimAdmission carries the sanitized command/settings and ephemeral host
// credentials. Only the explicit frozen inputs and credential MAC are persisted.
type ClaimAdmission struct {
	Command  operation.Command
	Settings Settings
	MAC      string
	Secrets  map[string]egress.Secret
}

type ProvisionTarget struct {
	Activation    *ClaimAdmission
	Identity      *ownership.Birth
	SlotHolder    string
	Budget        *quota.SlotBudget
	NewSandboxID  string
	Sandbox       ownership.Sandbox
	SlotOperation string
	Parent        machine.ResourceParent
}
