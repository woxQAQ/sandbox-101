package sandbox

import (
	"time"

	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/supply"
)

// ProvisionCapacity is the node's fixed admission budget for VM reservations.
type ProvisionCapacity struct {
	MaxMemoryMiB int64
	MaxVCPUs     int
	SafetyBytes  int64
}

type provisionalTemplateParent interface {
	ProvisioningTemplate() (supply.TemplateRecord, bool)
}

func provisionalTemplate(parent machine.ResourceParent) (supply.TemplateRecord, bool) {
	if provider, ok := parent.(provisionalTemplateParent); ok {
		return provider.ProvisioningTemplate()
	}
	return supply.TemplateRecord{}, false
}

func NewSandboxIdentity(selected supply.TemplateRecord, source string) ownership.Birth {
	return ownership.Birth{
		TemplateID:      selected.ID,
		Shape:           selected.Config.Shape,
		Epoch:           operation.NewID("epoch-"),
		CreatedAt:       time.Now().UTC(),
		ProvisionSource: source,
	}
}
