package sandbox

import (
	"database/sql"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/sandbox/internal/dbgen"
	"github.com/woxQAQ/haven/store"
)

// AdmitIn joins the initial activation and configuration fence to the caller's
// resource transaction. Secret registration and dispatch follow its commit.
func (service *Activations) AdmitIn(tx *sql.Tx, record operation.Record, resource machine.ResourceInput, claim ClaimAdmission) error {
	claim.Settings.Generation = record.ID
	input := ActivationInput{
		RequestTenantID:   resource.TenantID,
		TenantID:          resource.TenantID,
		SandboxID:         resource.SandboxID,
		ResourceOperation: record.ID,
		ResourceHash:      operation.Digest(operation.Encode(resource)),
		ExpectedInstance:  resource.Resource.Instance,
		Settings:          claim.Settings,
		ResetIdentity:     true,
	}
	payload := operation.Encode(input)
	if claim.MAC != "" {
		if err := operation.New(tx).InsertCredential(service.ctx, record.ID, claim.MAC); err != nil {
			return err
		}
	}
	if err := store.RequireOne(dbgen.New(tx).InsertSandboxSettings(service.ctx, dbgen.InsertSandboxSettingsParams{
		SandboxID: resource.SandboxID,
		Body:      operation.Encode(claim.Settings),
	})); err != nil {
		return err
	}
	if err := store.RequireOne(dbgen.New(tx).InsertConfigurationFence(service.ctx, dbgen.InsertConfigurationFenceParams{
		SandboxID:   resource.SandboxID,
		OperationID: record.ID,
	})); err != nil {
		return err
	}
	return store.RequireOne(dbgen.New(tx).InsertStep(service.ctx, dbgen.InsertStepParams{
		OperationID:         record.ID,
		StepKey:             "activation",
		Action:              "activate",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: resource.Resource.VMID,
		ExpectedInstance:    resource.Resource.Instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	}))
}
