package sandbox

import (
	"context"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/supply"
)

// AdmitRaw exposes only the undelivered build/refill subset of resource admission.
// It shares the original transaction, gate, workers and continuous VM guard.
func (service *VMProvisioning) AdmitRaw(ctx context.Context, request supply.ProvisionRequest, target supply.RawTarget) (operation.Record, error) {
	return service.Admit(ctx, ProvisionAdmission{
		Request: request,
		Target: &ProvisionTarget{
			NewSandboxID: target.NewSandboxID,
			Budget:       target.Budget,
			Parent:       target.Parent,
		},
	})
}
