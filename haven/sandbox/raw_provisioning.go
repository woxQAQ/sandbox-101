package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/loom/host"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

// AdmitRaw exposes only the undelivered build/refill subset of resource admission.
// It shares the original transaction, gate, workers and continuous VM guard.
func (service *VMProvisioning) AdmitRaw(ctx context.Context, tenantID, key string, frozen []byte, hash string, selected supply.TemplateRecord, manifest host.ImageManifest, restore *machine.Source, target supply.RawTarget) (operation.Record, error) {
	return service.Admit(ctx, tenantID, key, frozen, hash, selected, manifest, restore, nil, &ProvisionTarget{
		NewSandboxID: target.NewSandboxID,
		Budget:       target.Budget,
		Parent:       target.Parent,
	})
}
