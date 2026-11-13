package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func (service *Service) validateStep(step resourceStep) error {
	r := step.Input.Resource
	if err := host.CheckConfig(&r.Config); err != nil {
		return err
	}
	source := step.OperationID
	if step.Action == "release" {
		source = step.Input.SourceOperation
	}
	if (step.Action != "cold_start" && step.Action != "clone" && step.Action != "restore" && step.Action != "release") || !loom.ValidResourceID(source, "op-") || !loom.ValidResourceID(r.VMID, "vm-") || !loom.ValidResourceID(r.Instance, "run-") || !loom.ValidResourceID(r.NetworkToken, "net-") || r.Slot < 1 || r.Slot >= 32768 {
		return fmt.Errorf("unsupported or incomplete resource step identity")
	}
	if ((step.Action == "cold_start" || step.Action == "clone") && step.ExpectedInstance != "") || (step.Action == "release" && step.ExpectedInstance != r.Instance) || (!terminal(step.Status) && step.State.Witness != "") {
		return fmt.Errorf("step instance condition or historical witness is inconsistent")
	}
	if step.Action == "restore" && (step.ExpectedInstance == "" || step.ExpectedInstance == r.Instance || len(step.Input.Ancestors) == 0 || !loom.ValidResourceID(step.Input.StopOperation, "op-")) {
		return fmt.Errorf("restore predecessor/stop proof missing")
	}
	if (step.Input.SlotBudget.Kind != "sandbox_slot" && step.Input.SlotBudget.Kind != "build_slot") || step.Input.SlotBudget.Units < 0 || step.Input.SlotBudget.Units > 1 {
		return fmt.Errorf("resource slot budget is incomplete")
	}
	if step.Input.DiskKey == "" || !loom.ValidResourceID(step.Input.SlotOperation, "op-") || step.Input.RuntimeBytes <= 0 {
		return fmt.Errorf("resource responsibility identity missing")
	}
	if step.Action == "cold_start" {
		unprepared := step.State.Phase == "admitted" || step.State.Phase == "prepare_intent"
		if unprepared != (step.State.DiskIdentity == nil) {
			return fmt.Errorf("cold disk completion proof missing or out of phase")
		}
	}
	if step.Action == "release" && step.Input.DiskIdentity == nil {
		return fmt.Errorf("release disk witness missing")
	}
	directory := filepath.Join(service.store.directory, "vms", r.VMID)
	disk := filepath.Join(directory, "disks", source+".raw")
	restoreDir := ""
	if step.Input.Restore != nil {
		if step.Action == "cold_start" {
			return fmt.Errorf("cold step contains restore source")
		}
		disk = filepath.Join(directory, "versions", source, "disk.raw")
		restoreDir = filepath.Join(directory, "versions", source, "restore")
	} else if step.Action == "clone" || step.Action == "restore" {
		return fmt.Errorf("clone source proof missing")
	}
	if r.Dir != directory || r.Disk != disk || r.RunDir != filepath.Join(directory, "runs", r.Instance) || r.RestoreDir != restoreDir || r.ImageDir != filepath.Join(service.store.directory, "images", r.Config.Image[7:]) || step.Input.Image.Digest != r.Config.Image {
		return fmt.Errorf("resource step paths or image proof mismatch")
	}
	return nil
}

func (service *Service) run(operation string) error {
	direct, directErr := loadDirectRetirement(service.ctx, service.store.db, operation)
	if directErr == nil && !terminal(direct.Status) && direct.State.Phase == "pending" {
		guard := service.vmLock("configuration:" + direct.Input.Source.Input.SandboxID)
		guard.Lock()
		defer guard.Unlock()
		return service.retireDirect(service.ctx, operation)
	}
	if directErr != nil && !errors.Is(directErr, sql.ErrNoRows) {
		return directErr
	}
	artifact, artifactErr := loadImageCleanup(service.ctx, service.store.db, operation)
	if artifactErr == nil {
		lock := service.vmLock("image-cleanup:" + operation)
		lock.Lock()
		defer lock.Unlock()
		artifact, artifactErr = loadImageCleanup(service.ctx, service.store.db, operation)
		if artifactErr != nil {
			return artifactErr
		}
		if terminal(artifact.Status) {
			return nil
		}
		service.artifacts.Lock()
		defer service.artifacts.Unlock()
		return service.runImageCleanup(artifact)
	}
	if !errors.Is(artifactErr, sql.ErrNoRows) {
		return artifactErr
	}
	image, imageErr := loadImageImport(service.ctx, service.store.db, operation)
	if imageErr == nil {
		lock := service.vmLock("image-import:" + operation)
		lock.Lock()
		defer lock.Unlock()
		image, imageErr = loadImageImport(service.ctx, service.store.db, operation)
		if imageErr != nil {
			return imageErr
		}
		if terminal(image.Status) {
			return nil
		}
		service.artifacts.Lock()
		defer service.artifacts.Unlock()
		return service.runImageImport(image)
	}
	if !errors.Is(imageErr, sql.ErrNoRows) {
		return imageErr
	}
	collection, collectionErr := loadGC(service.ctx, service.store.db, operation)
	if collectionErr == nil {
		if terminal(collection.Status) {
			return nil
		}
		lock := service.vmLock("gc:" + operation)
		lock.Lock()
		defer lock.Unlock()
		collection, collectionErr = loadGC(service.ctx, service.store.db, operation)
		if collectionErr != nil {
			return collectionErr
		}
		return service.runGC(collection)
	}
	if !errors.Is(collectionErr, sql.ErrNoRows) {
		return collectionErr
	}
	collector, collectorErr := loadSupplyCleanup(service.ctx, service.store.db, operation)
	if collectorErr == nil {
		lock := service.vmLock("supply-cleanup:" + collector.Input.Source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		collector, collectorErr = loadSupplyCleanup(service.ctx, service.store.db, operation)
		if collectorErr != nil {
			return collectorErr
		}
		return service.runSupplyCleanup(collector)
	}
	if !errors.Is(collectorErr, sql.ErrNoRows) {
		return collectorErr
	}
	group, groupErr := loadFork(service.ctx, service.store.db, operation)
	if groupErr == nil {
		guard := service.vmLock("fork-group:" + operation)
		guard.Lock()
		defer guard.Unlock()
		group, groupErr = loadFork(service.ctx, service.store.db, operation)
		if groupErr != nil {
			return groupErr
		}
		if terminal(group.Status) {
			return nil
		}
		return service.runForkGroup(group)
	}
	if !errors.Is(groupErr, sql.ErrNoRows) {
		return groupErr
	}
	warm, warmErr := loadWarmClaim(service.ctx, service.store.db, operation)
	if warmErr == nil {
		lock := service.vmLock("sandbox:" + warm.Input.Pool.SandboxID)
		lock.Lock()
		defer lock.Unlock()
		warm, warmErr = loadWarmClaim(service.ctx, service.store.db, operation)
		if warmErr != nil {
			return warmErr
		}
		return service.runWarmClaim(warm)
	}
	if !errors.Is(warmErr, sql.ErrNoRows) {
		return warmErr
	}
	supply, supplyErr := loadSupply(service.ctx, service.store.db, operation)
	if supplyErr == nil {
		lock := service.vmLock("template-work:" + supply.Input.Template.ID)
		lock.Lock()
		defer lock.Unlock()
		supply, supplyErr = loadSupply(service.ctx, service.store.db, operation)
		if supplyErr != nil {
			return supplyErr
		}
		return service.runSupply(supply)
	}
	if !errors.Is(supplyErr, sql.ErrNoRows) {
		return supplyErr
	}
	raw, rawErr := loadCaptureCleanup(service.ctx, service.store.db, operation)
	if rawErr == nil {
		lock := service.vmLock(raw.Input.Capture.Input.Source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		raw, rawErr = loadCaptureCleanup(service.ctx, service.store.db, operation)
		if rawErr != nil {
			return rawErr
		}
		return service.runCaptureCleanup(raw)
	}
	if !errors.Is(rawErr, sql.ErrNoRows) {
		return rawErr
	}
	lifecycle, lifecycleErr := loadLifecycle(service.ctx, service.store.db, operation)
	if lifecycleErr == nil {
		lock := service.vmLock("sandbox:" + lifecycle.Input.Sandbox.ID)
		lock.Lock()
		defer lock.Unlock()
		lifecycle, lifecycleErr = loadLifecycle(service.ctx, service.store.db, operation)
		if lifecycleErr != nil {
			return lifecycleErr
		}
		return service.runLifecycle(lifecycle)
	}
	if !errors.Is(lifecycleErr, sql.ErrNoRows) {
		return lifecycleErr
	}
	deletion, err := loadSnapshotDelete(service.ctx, service.store.db, operation)
	if err == nil {
		lock := service.vmLock("snapshot:" + deletion.Input.Request.SnapshotID)
		lock.Lock()
		defer lock.Unlock()
		deletion, err = loadSnapshotDelete(service.ctx, service.store.db, operation)
		if err != nil {
			return err
		}
		return service.runSnapshotDelete(deletion)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	cleanup, err := loadCaptureDiscard(service.ctx, service.store.db, operation)
	if err == nil {
		lock := service.vmLock(cleanup.Input.Source.Input.Source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		cleanup, err = loadCaptureDiscard(service.ctx, service.store.db, operation)
		if err != nil {
			return err
		}
		return service.runCaptureDiscard(cleanup)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	stop, err := loadStop(service.ctx, service.store.db, operation)
	if err == nil {
		source, err := loadStep(service.ctx, service.store.db, stop.Input.SourceOperation)
		if err != nil {
			return err
		}
		lock := service.vmLock(source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		stop, err = loadStop(service.ctx, service.store.db, operation)
		if err != nil {
			return err
		}
		return service.runStop(stop)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	discard, err := loadDiscard(service.ctx, service.store.db, operation)
	if err == nil {
		lock := service.vmLock(discard.Input.Source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		discard, err = loadDiscard(service.ctx, service.store.db, operation)
		if err != nil {
			return err
		}
		return service.runDiscard(discard)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	capture, err := loadCapture(service.ctx, service.store.db, operation)
	if err == nil {
		lock := service.vmLock(capture.Input.Source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		capture, err = loadCapture(service.ctx, service.store.db, operation)
		if err != nil {
			return err
		}
		return service.runCapture(capture)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	activation, activationErr := loadActivation(service.ctx, service.store.db, operation)
	if activationErr == nil && activation.Input.ResourceOperation != operation {
		source, err := loadStep(service.ctx, service.store.db, activation.Input.ResourceOperation)
		if err != nil {
			return err
		}
		lock := service.vmLock(source.Candidate)
		lock.Lock()
		defer lock.Unlock()
		return service.continueActivation(operation)
	}
	if activationErr != nil && !errors.Is(activationErr, sql.ErrNoRows) {
		return activationErr
	}
	step, err := loadStep(service.ctx, service.store.db, operation)
	if err != nil {
		return err
	}
	if err = service.validateStep(step); err != nil {
		return err
	}
	lock := service.vmLock(step.Candidate)
	lock.Lock()
	defer lock.Unlock()
	step, err = loadStep(service.ctx, service.store.db, operation)
	if err != nil {
		return err
	}
	if terminal(step.Status) {
		if step.Status == "succeeded" {
			return service.continueActivation(operation)
		}
		return nil
	}
	if err = service.validateStep(step); err != nil {
		return err
	}
	vm, instance, owner, err := service.loadVM(service.ctx, service.store.db, step)
	if err != nil {
		return err
	}
	if owner != step.OperationID || vm.Deleting != (step.Action == "release") || instance != "" && !expectedCurrentInstance(step, instance) {
		return fmt.Errorf("resource step lost current VM ownership")
	}
	switch step.Action {
	case "clone", "restore":
		err = service.runClone(step)
	case "cold_start":
		err = service.runCold(step)
	case "release":
		return service.runRelease(step)
	default:
		return fmt.Errorf("unsupported resource action")
	}
	if err != nil {
		return err
	}
	return service.continueActivation(operation)
}

func (service *Service) markPhase(step *resourceStep, phase string) error {
	next := step.State
	next.Phase = phase
	if step.Action == "cold_start" && phase == "prepared" {
		identity, err := loom.InspectWritableDisk(step.Input.Resource)
		if err != nil {
			return err
		}
		next.DiskIdentity = &identity
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if step.Action == "cold_start" || step.Action == "clone" || step.Action == "restore" {
		if _, _, _, err = service.checkProvisionIntent(tx, *step); err != nil {
			return err
		}
	} else if _, err = service.releaseProof(tx, *step); err != nil {
		return err
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running') AND body=?", encode(next), step.OperationID, step.Key, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if (step.Action == "cold_start" || step.Action == "clone" || step.Action == "restore") && (phase == "prepared" || phase == "launch_intent") {
		instance := ""
		if phase == "launch_intent" {
			instance = step.Input.Resource.Instance
		}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE vms SET state='created',current_instance=? WHERE id=? AND operation_id=?", instance, step.Candidate, step.OperationID)); err != nil {
			return err
		}
	}
	if step.Action == "cold_start" && phase == "prepared" {
		previous := resourceVersion{Resource: step.Input.Resource, DiskIdentity: step.State.DiskIdentity}
		version := resourceVersion{Resource: step.Input.Resource, DiskIdentity: next.DiskIdentity}
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE resource_versions SET body=? WHERE id=? AND vm_id=? AND operation_id=? AND body=?", encode(version), step.OperationID, step.Candidate, step.OperationID, encode(previous))); err != nil {
			return err
		}
	}
	if step.Action == "release" && phase == "stopped" {
		vm, _, _, err := service.loadVM(service.ctx, tx, *step)
		if err != nil {
			return err
		}
		vm.AgentReady, vm.Verified, vm.Observation = false, true, nil
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE vms SET state='stopped',body=? WHERE id=? AND current_instance=? AND operation_id=?", encode(vm), step.Candidate, step.ExpectedInstance, step.OperationID)); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State, step.Status = next, "running"
	if err = service.store.checkpoint(); err != nil {
		return err
	}
	return service.fault("step_" + phase)
}

func (service *Service) runCold(step resourceStep) error {
	service.admission.Lock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err == nil {
		_, _, _, err = service.checkProvisionIntent(tx, step)
		err = errors.Join(err, tx.Rollback())
	}
	service.admission.Unlock()
	if err != nil {
		return err
	}
	r := step.Input.Resource
	if err := verifyImageFiles(step.Input.Image, r.ImageDir); err != nil {
		return err
	}
	if step.State.Phase == "admitted" {
		if err := service.markPhase(&step, "prepare_intent"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepare_intent" {
		before, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.opts.Backend.Prepare(service.ctx, r); err != nil {
			return service.failStep(step, err)
		}
		if err := service.fault("effect_prepared"); err != nil {
			return err
		}
		if err := service.markPhase(&step, "prepared"); err != nil {
			return err
		}
	}
	if step.State.Phase == "prepared" {
		before, err := service.opts.Backend.Inspect(service.ctx, r)
		if err != nil {
			return err
		}
		if before.Alive {
			return fmt.Errorf("live candidate lacks a durable launch intent")
		}
		if err := service.markPhase(&step, "launch_intent"); err != nil {
			return err
		}
	}
	return service.startCandidate(step, false)
}

func (service *Service) startCandidate(step resourceStep, restore bool) error {
	r := step.Input.Resource
	if step.State.Phase != "launch_intent" && step.State.Phase != "launched" && step.State.Phase != "booted" && step.State.Phase != "agent_ready" {
		return fmt.Errorf("unknown cold resource phase")
	}
	if step.State.DiskIdentity == nil {
		return fmt.Errorf("prepared cold disk proof missing")
	}
	if err := loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	observation, err := service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if observation.Network {
		if err = service.opts.Backend.Network(service.ctx, r, false); err != nil {
			return err
		}
	}
	if !observation.Alive {
		if step.State.Phase != "launch_intent" {
			return service.failStep(step, loom.Fail(loom.Unknown, "previous candidate exited; refusing another launch"))
		}
		if err = service.opts.Backend.Network(service.ctx, r, false); err != nil {
			return service.failStep(step, err)
		}
		if err = service.opts.Backend.Launch(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.fault("effect_launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launch_intent" {
		if err = service.markPhase(&step, "launched"); err != nil {
			return err
		}
	}
	if step.State.Phase == "launched" {
		if err = service.opts.Backend.Boot(service.ctx, r, restore); err != nil {
			return service.failStep(step, err)
		}
		if err = service.markPhase(&step, "booted"); err != nil {
			return err
		}
	}
	if err = service.opts.Backend.Agent(service.ctx, r, restore); err != nil {
		networkErr := service.opts.Backend.Network(service.ctx, r, false)
		stopErr := service.opts.Backend.Stop(service.ctx, r)
		return service.failStep(step, errors.Join(err, networkErr, stopErr))
	}
	if step.State.Phase != "agent_ready" {
		if err = service.markPhase(&step, "agent_ready"); err != nil {
			return err
		}
	}
	observation, err = service.opts.Backend.Inspect(service.ctx, r)
	if err != nil {
		return err
	}
	if !observation.Alive || !observation.Booted || !observation.Configured || observation.Paused || observation.Network || observation.PID <= 0 || observation.StartTime == 0 {
		return service.failStep(step, loom.Fail(loom.Unknown, "candidate is not verified System ready with network off"))
	}
	if restore {
		if step.Input.Restore == nil || !step.State.BackingReady {
			return fmt.Errorf("restore start lacks backing handoff")
		}
		if err := loom.VerifyRestoreMaterials(step.State.Materials, r, step.Input.Restore.Snapshot.Manifest.Files, false); err != nil {
			return service.failStep(step, err)
		}
	}
	if err = loom.VerifyWritableDisk(r, *step.State.DiskIdentity); err != nil {
		return err
	}
	return service.completeCold(step, observation)
}

// Runtime failures fix the original public failure but retain the candidate,
// quota, reservation and dependency. SQL/inspection failures never call this
// path: they leave the original unfinished step available for inspection/replay.
func (service *Service) failStep(step resourceStep, cause error) error {
	if service.ctx.Err() != nil {
		return service.ctx.Err()
	}
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return errors.Join(cause, err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := loadStep(service.ctx, tx, step.OperationID)
	if err != nil {
		return errors.Join(cause, err)
	}
	if terminal(current.Status) {
		return cause
	}
	if current.Hash != step.Hash || !reflect.DeepEqual(current.State, step.State) {
		return fmt.Errorf("failed step ownership changed")
	}
	next := step.State
	if step.Input.Restore != nil {
		next.RestoreBlocked = true
	}
	next.Error = "resource effect failed; responsibility retained for inspection"
	if activation, lookup := loadActivation(service.ctx, tx, step.OperationID); lookup == nil && !terminal(activation.Status) {
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND input_hash=? AND status IN ('pending','running')", encode("blocked"), step.OperationID, activation.Key, activation.Hash)); err != nil {
			return err
		}
	} else if lookup != nil && !errors.Is(lookup, sql.ErrNoRows) {
		return lookup
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='failed',body=? WHERE operation_id=? AND step_key=? AND status IN ('pending','running') AND input_hash=?", encode(next), step.OperationID, step.Key, step.Hash)); err != nil {
		return errors.Join(cause, err)
	}
	var result operationResult
	var body []byte
	var status string
	if err = tx.QueryRowContext(service.ctx, "SELECT status,body FROM operations WHERE id=?", step.OperationID).Scan(&status, &body); err != nil {
		return errors.Join(cause, err)
	}
	if err = json.Unmarshal(body, &result); err != nil {
		return errors.Join(cause, err)
	}
	if !terminal(status) {
		result.Error = next.Error
		if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operations SET status='failed',body=? WHERE id=? AND status IN ('pending','running')", encode(result), step.OperationID)); err != nil {
			return errors.Join(cause, err)
		}
	}
	if err = requireWrite(tx.ExecContext(service.ctx, "INSERT INTO cleanup_intents VALUES(?,?,?,?,?,?)", "blocked:"+step.OperationID, step.OperationID, "vm", step.Candidate, "blocked", encode(step.Input))); err != nil {
		return errors.Join(cause, err)
	}
	if err = commitPublications(tx); err != nil {
		return errors.Join(cause, err)
	}
	return errors.Join(cause, service.store.checkpoint())
}

func (service *Service) loadVM(ctx context.Context, query queryer, step resourceStep) (vmRecord, string, string, error) {
	var vm vmRecord
	var body []byte
	var instance, phase string
	var owner sql.NullString
	err := query.QueryRowContext(ctx, "SELECT current_instance,state,operation_id,body FROM vms WHERE id=?", step.Candidate).Scan(&instance, &phase, &owner, &body)
	if err != nil {
		return vm, "", "", err
	}
	if err = json.Unmarshal(body, &vm); err != nil {
		return vm, "", "", err
	}
	if !reflect.DeepEqual(vm.Resource, step.Input.Resource) {
		return vm, "", "", fmt.Errorf("VM candidate resource changed")
	}
	return vm, instance, owner.String, nil
}
