package sandbox

import (
	"github.com/woxQAQ/haven/machine"
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
