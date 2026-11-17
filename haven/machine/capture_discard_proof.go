package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
)

// verifyCaptureDiscardSource verifies the immutable failure and stopped source
// against the owner expected at this transaction boundary. Custody transfer and
// the admitted child operation are checked by their separate stage entrypoints.
func (service *CaptureDiscard) verifyCaptureDiscardSource(ctx context.Context, tx *sql.Tx, step CaptureDiscardStep, expectedOwner string) (ownership.Sandbox, error) {
	var empty ownership.Sandbox
	input, request := step.Input, step.Input.Request
	source := input.Source
	resource := source.Input.Source
	historical, err := LoadCapture(ctx, tx, request.FailedOperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(historical, source) || source.OperationID != request.FailedOperationID || source.Status != "failed" || source.State.ResumeRequired || source.State.Error == "" || source.Input.Request.TenantID != request.TenantID || source.Input.Request.SandboxID != request.SandboxID || source.Input.Request.ExpectedInstance != request.ExpectedInstance || resource.State.Witness != request.ExpectedInstance {
		return empty, fmt.Errorf("capture discard requires immutable resumed failure")
	}
	if err = service.proofs.VerifyGeneration(ctx, tx, resource); err != nil {
		return empty, err
	}
	generations, err := service.proofs.GenerationHistory(ctx, tx, resource)
	if err != nil {
		return empty, err
	}
	if err = CheckGenerationCounts(ctx, tx, resource, append(generations, resource)); err != nil {
		return empty, err
	}
	stopped, err := LoadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if stopped.Hash != input.StopHash || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != resource.OperationID || stopped.Input.SourceHash != resource.Hash || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != request.SandboxID || stopped.Input.Request.ExpectedInstance != request.ExpectedInstance {
		return empty, fmt.Errorf("capture discard stop proof changed")
	}
	sandboxRecord, err := service.ownership.ReadSandbox(ctx, tx, request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	allowedState := sandboxRecord.State == "provisioning"
	if input.Lifecycle != "" {
		allowedState, err = service.ownership.VerifyCaptureDiscardParent(ctx, tx, step, sandboxRecord)
		if err != nil {
			return empty, err
		}
	}
	if !reflect.DeepEqual(sandboxRecord, source.Input.Sandbox) || sandboxRecord.SourceOperation != resource.OperationID || sandboxRecord.Instance != request.ExpectedInstance || !allowedState {
		return empty, fmt.Errorf("capture discard current sandbox ownership changed")
	}
	currentMachine, current, owner, err := LoadVM(ctx, tx, resource)
	if err != nil {
		return empty, err
	}
	currentOwnership, err := LoadOwnership(ctx, tx, resource.Candidate)
	if err != nil {
		return empty, err
	}
	if owner != expectedOwner || current != request.ExpectedInstance || currentOwnership.State != "stopped" || currentMachine.Deleting || !currentMachine.Verified || currentMachine.AgentReady || !reflect.DeepEqual(currentMachine.Resource, resource.Input.Resource) {
		return empty, fmt.Errorf("capture discard current stopped instance changed")
	}
	var frozen struct {
		Action  string
		Request CaptureRequest
	}
	var result operation.Result
	projection, err := operation.New(tx).Projection(ctx, source.OperationID)
	if err != nil {
		return empty, err
	}
	if err = json.Unmarshal(projection.Request, &frozen); err != nil {
		return empty, err
	}
	if err = json.Unmarshal(projection.Body, &result); err != nil {
		return empty, err
	}
	if operation.Digest(projection.Request) != projection.RequestHash || projection.Status != "failed" || projection.ScopeKind != "tenant" || projection.ScopeID != request.TenantID || frozen.Action != "snapshot" || frozen.Request != source.Input.Request || result.Error != source.State.Error || result.SnapshotID != source.Input.Snapshot.ID || result.VMID != resource.Candidate || result.SandboxID != sandboxRecord.ID {
		return empty, fmt.Errorf("capture discard original terminal result changed")
	}
	snapshot := source.Input.Snapshot
	if snapshot.OperationID != source.OperationID || snapshot.TenantID != request.TenantID || snapshot.SourceVM != resource.Candidate || snapshot.SourceInstance != request.ExpectedInstance || snapshot.Manifest != nil || snapshot.Dir != filepath.Join(service.store.Directory, "snapshots", snapshot.ID) {
		return empty, fmt.Errorf("capture discard destination changed")
	}
	return sandboxRecord, nil
}
