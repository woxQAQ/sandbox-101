package sandbox

import (
	"time"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/supply"
)

func NewSandboxIdentity(selected supply.TemplateRecord, source string) ownership.Birth {
	return ownership.Birth{
		TemplateID:      selected.ID,
		Shape:           selected.Config.Shape,
		Epoch:           operation.NewID("epoch-"),
		CreatedAt:       time.Now().UTC(),
		ProvisionSource: source,
	}
}
