package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/loom"
)

type (
	CaptureCleanupRequest struct {
		TenantID           string `json:"tenant_id"`
		Key                string `json:"key"`
		CaptureOperationID string `json:"capture_operation_id"`
		StopOperationID    string `json:"stop_operation_id"`
	}
	captureCleanupInput struct {
		Request CaptureCleanupRequest
		Capture captureStep
		Stop    stopStep
	}
	captureCleanupStep struct {
		OperationID, Hash, Status, Phase string
		Input                            captureCleanupInput
	}
)

func loadCaptureCleanup(ctx context.Context, q queryer, id string) (captureCleanupStep, error) {
	step := captureCleanupStep{
		OperationID: id,
	}
	var input, body []byte
	var candidate, expected, action string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND step_key='capture_cleanup'", id).Scan(&step.Hash, &input, &step.Status, &body, &candidate, &expected, &action)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("raw capture cleanup checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.Phase); err != nil {
		return step, err
	}
	if action != "capture_cleanup" || candidate != step.Input.Capture.Input.Source.Candidate || expected != step.Input.Capture.Input.Request.ExpectedInstance {
		return step, fmt.Errorf("raw capture cleanup identity changed")
	}
	return step, nil
}

func (service *Service) AdmitCaptureCleanup(ctx context.Context, request CaptureCleanupRequest) (Operation, error) {
	return service.admitCaptureCleanup(ctx, request, nil)
}

func (service *Service) admitCaptureCleanup(ctx context.Context, request CaptureCleanupRequest, parent resourceParent) (Operation, error) {
	link := lifecycleParent(parent)
	var empty Operation
	if request.TenantID == "" || request.Key == "" || len(request.Key) > 256 || !loom.ValidResourceID(request.CaptureOperationID, "op-") || !loom.ValidResourceID(request.StopOperationID, "op-") {
		return empty, fmt.Errorf("%w: raw capture cleanup requires original capture and stop proof", ErrInvalid)
	}
	frozen := encode(struct {
		Action  string
		Request CaptureCleanupRequest
	}{"capture_cleanup", request})
	hash := digest(frozen)
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return empty, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return empty, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if cached, found, err := cachedOperation(ctx, tx, request.TenantID, request.Key, hash); found || err != nil {
		if found && err == nil && !terminal(cached.Status) {
			service.dispatch(cached.ID)
		}
		return cached, err
	}
	capture, err := loadCapture(ctx, tx, request.CaptureOperationID)
	if err != nil {
		return empty, err
	}
	if err = allocationCleanupFence(ctx, tx, capture.Input.Source.Candidate, parent); err != nil {
		return empty, err
	}
	if link != nil {
		if link.Input.Rollback == nil || link.Input.Command.Action != "rollback" {
			return empty, fmt.Errorf("%w: raw cleanup parent differs from rollback", ErrInvalid)
		}
		found := false
		for _, ref := range link.Input.Rollback.RawCaptures {
			if ref.OperationID == capture.OperationID && ref.InputHash == capture.Hash {
				found = true
			}
		}
		if !found {
			return empty, fmt.Errorf("%w: raw capture is absent from frozen rollback responsibilities", ErrConflict)
		}
		stopped, _, err := loadResourceLink(ctx, tx, link.Parent.ID, "stop")
		if err != nil {
			return empty, err
		}
		if stopped.ChildID != request.StopOperationID {
			return empty, fmt.Errorf("%w: raw cleanup stop differs from original rollback", ErrConflict)
		}
	}
	stopped, err := loadStop(ctx, tx, request.StopOperationID)
	if err != nil {
		return empty, err
	}
	if capture.Input.Request.TenantID != request.TenantID || stopped.Status != "succeeded" || stopped.Phase != "complete" || stopped.Input.SourceOperation != capture.Input.Source.OperationID || stopped.Input.SourceHash != capture.Input.Source.Hash || stopped.Input.Request.ExpectedInstance != capture.Input.Request.ExpectedInstance || stopped.Input.Request.TenantID != request.TenantID || stopped.Input.Request.SandboxID != capture.Input.Request.SandboxID {
		return empty, fmt.Errorf("%w: raw capture stop proof differs from captured generation", ErrConflict)
	}
	if err = service.captureCleanupProof(ctx, tx, capture, capture.Input.Source.Candidate, capture.Input.Request.ExpectedInstance, "pending"); err != nil {
		return empty, err
	}
	source, err := loadStep(ctx, tx, capture.Input.Source.OperationID)
	if err != nil {
		return empty, err
	}
	if !reflect.DeepEqual(source, capture.Input.Source) {
		return empty, fmt.Errorf("%w: raw capture source history changed", ErrConflict)
	}
	vm, instance, owner, err := service.loadVM(ctx, tx, source)
	if err != nil {
		return empty, err
	}
	if owner != "" || instance != source.State.Witness || vm.Deleting || vm.AgentReady || !vm.Verified {
		return empty, fmt.Errorf("%w: raw capture source is not stopped and unowned", ErrConflict)
	}
	sandbox, err := loadSandbox(ctx, tx, capture.Input.Request.SandboxID, request.TenantID)
	if err != nil {
		return empty, err
	}
	if sandbox.SourceOperation != source.OperationID || sandbox.Instance != source.State.Witness || sandbox.VMID != source.Candidate {
		return empty, fmt.Errorf("%w: raw capture source ownership changed", ErrConflict)
	}
	operation := Operation{
		ID:     newID("op-"),
		Key:    request.Key,
		Status: "pending",
		Result: operationResult{
			SandboxID:  sandbox.ID,
			VMID:       source.Candidate,
			SnapshotID: capture.Input.Snapshot.ID,
		},
	}
	input := captureCleanupInput{
		Request: request,
		Capture: capture,
		Stop:    stopped,
	}
	payload := encode(input)
	if err = service.admitResourceParent(ctx, tx, parent, operation, "capture_cleanup", source.Candidate, hash); err != nil {
		return empty, err
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			operation.ID,
			"tenant",
			request.TenantID,
			request.Key,
			hash,
			frozen,
			"pending",
			encode(operation.Result),
		}},
		{"INSERT INTO operation_steps VALUES(?,'capture_cleanup','capture_cleanup',?,?,?,?,'pending',?)", []any{
			operation.ID,
			digest(payload),
			payload,
			source.Candidate,
			source.State.Witness,
			encode("admitted"),
		}},
		{"UPDATE cleanup_intents SET status='transferred' WHERE id=? AND operation_id=? AND resource_kind='capture' AND resource_id=? AND status='pending'", []any{
			"capture:" + capture.OperationID,
			capture.OperationID,
			source.Candidate,
		}},
		{"INSERT INTO cleanup_intents VALUES(?,?,'capture_raw',?,'pending',?)", []any{
			"raw:" + operation.ID,
			operation.ID,
			source.Candidate,
			payload,
		}},
		{"UPDATE vms SET operation_id=? WHERE id=? AND current_instance=? AND state='stopped' AND operation_id IS NULL", []any{
			operation.ID,
			source.Candidate,
			source.State.Witness,
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return empty, err
		}
	}
	if err = service.fault("capture_cleanup_admission_before_commit"); err != nil {
		return empty, err
	}
	if err = tx.Commit(); err != nil {
		return empty, err
	}
	if err = service.store.checkpoint(); err != nil {
		return operation, err
	}
	service.dispatch(operation.ID)
	return operation, nil
}

func (service *Service) captureRawProof(tx *sql.Tx, step captureCleanupStep) error {
	current, err := loadCaptureCleanup(service.ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(current, step) {
		return fmt.Errorf("raw capture cleanup intent changed")
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	if err = service.captureCleanupProof(service.ctx, tx, capture, source.Candidate, source.State.Witness, "transferred"); err != nil {
		return err
	}
	stopped, err := loadStop(service.ctx, tx, step.Input.Stop.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(stopped, step.Input.Stop) {
		return fmt.Errorf("raw capture original stop proof changed")
	}
	vm, instance, owner, err := service.loadVM(service.ctx, tx, source)
	if err != nil {
		return err
	}
	if owner != step.OperationID || instance != source.State.Witness || vm.Deleting || vm.AgentReady {
		return fmt.Errorf("raw capture current stopped ownership changed")
	}
	var count int
	if err = tx.QueryRowContext(service.ctx, "SELECT COUNT(*) FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", source.Candidate, capture.OperationID).Scan(&count); err != nil || count != 1 {
		return fmt.Errorf("raw capture responsibility missing: %v", err)
	}
	var body, request []byte
	var status, scope, tenant, hash string
	if err = tx.QueryRowContext(service.ctx, "SELECT body,request,status,scope_kind,scope_id,request_hash FROM operations WHERE id=?", step.OperationID).Scan(&body, &request, &status, &scope, &tenant, &hash); err != nil {
		return err
	}
	var result operationResult
	var frozen struct {
		Action  string
		Request CaptureCleanupRequest
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if err = json.Unmarshal(request, &frozen); err != nil {
		return err
	}
	if terminal(status) || scope != "tenant" || tenant != source.Input.TenantID || digest(request) != hash || frozen.Action != "capture_cleanup" || frozen.Request != step.Input.Request || result.SandboxID != source.Input.SandboxID || result.VMID != source.Candidate || result.SnapshotID != capture.Input.Snapshot.ID {
		return fmt.Errorf("raw capture operation authority changed")
	}
	return nil
}

func (service *Service) checkCaptureRaw(step captureCleanupStep) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	return service.captureRawProof(tx, step)
}

func (service *Service) captureRawReceipt(step *captureCleanupStep) (loom.DeletionIntent, error) {
	var intent loom.DeletionIntent
	var body []byte
	capture := step.Input.Capture
	raw := capture.State.Boundary.Path
	err := service.store.db.QueryRowContext(service.ctx, "SELECT body FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='capture_cleanup'", raw, step.OperationID).Scan(&body)
	if err == nil {
		if step.Phase != "deleting" {
			return intent, fmt.Errorf("raw capture receipt outside deletion phase")
		}
		if err = json.Unmarshal(body, &intent); err != nil {
			return intent, err
		}
		return intent, loom.VerifyPublishedCaptureRawDeletion(intent, raw, capture.State.Boundary)
	}
	if !errors.Is(err, sql.ErrNoRows) || step.Phase != "admitted" {
		return intent, fmt.Errorf("raw capture durable receipt missing: %w", err)
	}
	intent, err = loom.InspectPublishedCaptureRawDeletion(service.ctx, service.store.directory, capture.Input.Source.Input.Resource, capture.OperationID, capture.State.Boundary, capture.State.Files)
	if err != nil {
		return intent, err
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return intent, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureRawProof(tx, *step); err != nil {
		return intent, err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO deletion_intents VALUES(?,?,'capture_cleanup',?)", raw, step.OperationID, encode(intent))); err != nil {
		return intent, err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='capture_cleanup' AND input_hash=? AND status='pending' AND body=?", encode("deleting"), step.OperationID, step.Hash, encode(step.Phase))); err != nil {
		return intent, err
	}
	if err = tx.Commit(); err != nil {
		return intent, err
	}
	step.Phase = "deleting"
	step.Status = "running"
	if err = service.store.checkpoint(); err != nil {
		return intent, err
	}
	return intent, service.fault("capture_cleanup_receipt")
}

func (service *Service) runCaptureCleanup(step captureCleanupStep) error {
	if terminal(step.Status) {
		return nil
	}
	if err := service.checkCaptureRaw(step); err != nil {
		return err
	}
	source := step.Input.Capture.Input.Source
	if err := service.inspectStoppedGenerations([]resourceStep{
		source,
	}); err != nil {
		return err
	}
	intent, err := service.captureRawReceipt(&step)
	if err != nil {
		return err
	}
	paths, err := loom.ResumeDeletion(service.ctx, service.store.directory, intent)
	if err != nil {
		return err
	}
	if err = loom.CheckLocalReaders(paths); err != nil {
		return err
	}
	identities := make([]loom.FileIdentity, 0, len(intent.Nodes))
	for _, identity := range intent.Nodes {
		identities = append(identities, identity)
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.checkCaptureRaw(step); err != nil {
		return err
	}
	for _, path := range paths {
		if err = loom.RemoveDeletionFile(service.ctx, service.store.directory, intent, path); err != nil {
			return err
		}
		if err = service.fault("capture_cleanup_unlinked"); err != nil {
			return err
		}
	}
	if err = loom.RemoveDeletionDirectories(service.ctx, service.store.directory, intent); err != nil {
		return err
	}
	if err = loom.CheckLocalReaderIdentities(identities); err != nil {
		return err
	}
	if err = service.fault("capture_cleanup_files_removed"); err != nil {
		return err
	}
	if err = service.inspectStoppedGenerations([]resourceStep{
		source,
	}); err != nil {
		return err
	}
	return service.completeCaptureRaw(step, intent)
}

func (service *Service) completeCaptureRaw(step captureCleanupStep, intent loom.DeletionIntent) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.captureRawProof(tx, step); err != nil {
		return err
	}
	capture := step.Input.Capture
	source := capture.Input.Source
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"DELETE FROM resource_refs WHERE resource_kind='vm' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{
			source.Candidate,
			capture.OperationID,
		}},
		{"DELETE FROM resource_refs WHERE resource_kind='image' AND resource_id=? AND holder_kind='capture' AND holder_id=?", []any{
			source.Input.Resource.Config.Image,
			capture.OperationID,
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='transferred'", []any{
			"capture:" + capture.OperationID,
			capture.OperationID,
		}},
		{"UPDATE cleanup_intents SET status='done' WHERE id=? AND operation_id=? AND status='pending'", []any{
			"raw:" + step.OperationID,
			step.OperationID,
		}},
		{"UPDATE vms SET operation_id=NULL WHERE id=? AND current_instance=? AND operation_id=? AND state='stopped'", []any{
			source.Candidate,
			source.State.Witness,
			step.OperationID,
		}},
		{"DELETE FROM deletion_intents WHERE path=? AND operation_id=? AND step_key='capture_cleanup' AND body=?", []any{
			intent.Path,
			step.OperationID,
			encode(intent),
		}},
		{"UPDATE operation_steps SET status='succeeded',body=? WHERE operation_id=? AND step_key='capture_cleanup' AND input_hash=? AND status='running' AND body=?", []any{
			encode("complete"),
			step.OperationID,
			step.Hash,
			encode(step.Phase),
		}},
		{"UPDATE operations SET status='succeeded' WHERE id=? AND status IN ('pending','running')", []any{
			step.OperationID,
		}},
	} {
		if err = requireWrite(tx.ExecContext(service.ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err = service.fault("capture_cleanup_completion_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}
