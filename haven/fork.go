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
)

func loadCheckpoint(ctx context.Context, q queryer, id, tenant string) (Checkpoint, error) {
	var checkpoint Checkpoint
	var body []byte
	if err := q.QueryRowContext(ctx, "SELECT body FROM checkpoint_bindings WHERE id=? AND tenant=?", id, tenant).Scan(&body); err != nil {
		return checkpoint, err
	}
	if err := json.Unmarshal(body, &checkpoint); err != nil {
		return checkpoint, err
	}
	if checkpoint.ID != id || checkpoint.TenantID != tenant {
		return checkpoint, fmt.Errorf("%w: checkpoint binding identity changed", ErrManualRecovery)
	}
	return checkpoint, nil
}

func (service *Service) AdmitFork(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if command.Count == 0 {
		command.Count = 1
	}
	if command.Action != "fork" || (command.CheckpointID == "" && command.SandboxID == "") || command.Count < 1 || command.Count > 32 || principal.SandboxID != "" || principal.TenantID == "" || key == "" || len(key) > 256 || service.opts.Guest == nil {
		return empty, fmt.Errorf("%w: fork requires tenant authority, one source, count 1..32 and key", ErrInvalid)
	}
	requestGuard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	requestGuard.Lock()
	defer requestGuard.Unlock()
	_, secrets, _, err := splitEnv(command.Env)
	if err != nil {
		return empty, err
	}
	mac := service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{
		MAC:     mac,
		Secrets: secrets,
	}); found || err != nil {
		if found && err == nil && !terminal(operation.Status) {
			group, lookup := loadFork(ctx, service.store.db, operation.ID)
			if lookup != nil {
				return operation, lookup
			}
			service.forkSecrets(group)
		}
		return operation, err
	}
	service.mu.Lock()
	draining := service.draining
	service.mu.Unlock()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", ErrExhausted)
	}
	input := forkInput{
		RequestTenantID: principal.TenantID,
	}
	if command.CheckpointID != "" {
		checkpoint, err := service.GetCheckpoint(ctx, principal, command.CheckpointID)
		if err != nil {
			return empty, err
		}
		source, err := service.loadRestoreSource(ctx, service.store.db, checkpoint.TenantID, checkpoint.SnapshotID)
		if err != nil {
			return empty, err
		}
		input.OwnerTenantID = checkpoint.TenantID
		input.Snapshot = &source
		input.Checkpoint = &checkpoint
		input.Template = templateRecord{
			ID:       checkpoint.TemplateID,
			TenantID: checkpoint.TenantID,
			Config:   source.Snapshot.Manifest.Config,
		}
	} else {
		sandbox, err := ownedSandbox(ctx, service.store.db, principal, command.SandboxID)
		if err != nil {
			return empty, err
		}
		if sandbox.State != "running" && sandbox.State != "paused" {
			return empty, fmt.Errorf("%w: sandbox is not forkable", ErrConflict)
		}
		settings, err := loadSettings(ctx, service.store.db, sandbox.ID)
		if err != nil {
			return empty, err
		}
		if err = service.dataFence(ctx, sandbox.ID); err != nil {
			return empty, err
		}
		input.SourceSandbox = &sandbox
		input.OwnerTenantID = sandbox.TenantID
		if command.Policy.Mode == "" {
			command.Policy = settings.Policy
		}
		if sandbox.State == "paused" {
			source, err := service.loadRestoreSource(ctx, service.store.db, sandbox.TenantID, sandbox.SnapshotID)
			if err != nil {
				return empty, err
			}
			input.Snapshot = &source
			input.Template = templateRecord{
				ID:       sandbox.Identity.TemplateID,
				TenantID: sandbox.TenantID,
				Config:   source.Snapshot.Manifest.Config,
			}
		} else {
			public, err := service.Get(ctx, principal, sandbox.ID)
			if err != nil {
				return empty, err
			}
			source, err := service.dataResource(ctx, public)
			if err != nil {
				return empty, err
			}
			input.Source = &source
			input.Template = templateRecord{
				ID:       sandbox.Identity.TemplateID,
				TenantID: sandbox.TenantID,
				Config:   source.Input.Resource.Config,
			}
		}
	}
	admission, err := service.prepareClaim(command)
	if err != nil {
		return empty, err
	}
	if input.Snapshot != nil {
		if err = verifyRestoreSourceFiles(*input.Snapshot); err != nil {
			return empty, err
		}
	}
	if err = service.opts.Backend.Check(ctx, input.Template.Config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, input.Template.Config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	parent := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
	}
	admission.Settings.Generation = parent.ID
	input.Command = admission.Command
	input.Settings = admission.Settings
	for range command.Count {
		target := sandboxRecord{
			ID:          newID("sandbox_"),
			TenantID:    input.OwnerTenantID,
			OperationID: parent.ID,
			State:       "provisioning",
			Identity:    newSandboxIdentity(input.Template, ""),
		}
		input.Targets = append(input.Targets, target)
	}
	parent.Result.SandboxIDs = forkTargetIDs(input)
	parent.Result.SandboxID = parent.Result.SandboxIDs[0]
	if input.Snapshot != nil {
		parent.Result.SnapshotID = input.Snapshot.Snapshot.ID
	}
	free, err := loom.FreeBytes(service.store.directory)
	if err != nil {
		return empty, err
	}
	link := &forkAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Free:   free,
	}
	if input.Source != nil {
		child, err := service.admitCapture(ctx, CaptureRequest{
			TenantID:         input.OwnerTenantID,
			Key:              parent.ID + ":capture",
			SandboxID:        input.SourceSandbox.ID,
			ExpectedInstance: input.Source.State.Witness,
		}, link)
		if child.ID == "" {
			return empty, err
		}
		parent.Result.SnapshotID = child.Result.SnapshotID
		if err != nil {
			return parent, err
		}
	} else {
		if err = service.admitForkSnapshot(ctx, link); err != nil {
			return empty, err
		}
	}
	service.mu.Lock()
	service.opSecrets[parent.ID] = admission.Secrets
	service.mu.Unlock()
	if err = service.fault("fork_admitted"); err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) admitForkSnapshot(ctx context.Context, parent *forkAdmission) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = service.admitForkIntent(ctx, tx, parent); err != nil {
		return err
	}
	if err = service.fault("fork_admission_before_commit"); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return service.store.checkpoint()
}

func (service *Service) admitForkIntent(ctx context.Context, tx *sql.Tx, parent *forkAdmission) error {
	input := parent.Input
	if err := service.checkAdmissionOpen(); err != nil {
		return err
	}
	if input.SourceSandbox != nil {
		current, err := loadSandbox(ctx, tx, input.SourceSandbox.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.SourceSandbox) {
			return fmt.Errorf("%w: fork source sandbox changed before admission", ErrConflict)
		}
		var fence string
		err = tx.QueryRowContext(ctx, "SELECT operation_id FROM configuration_fences WHERE sandbox_id=?", current.ID).Scan(&fence)
		if err == nil {
			return fmt.Errorf("%w: fork source is configuration fenced", ErrConflict)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if input.Checkpoint != nil {
		current, err := loadCheckpoint(ctx, tx, input.Checkpoint.ID, input.OwnerTenantID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.Checkpoint) {
			return fmt.Errorf("%w: fork source checkpoint changed before admission", ErrConflict)
		}
	}
	if input.Snapshot != nil {
		current, err := service.loadRestoreSource(ctx, tx, input.OwnerTenantID, input.Snapshot.Snapshot.ID)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, *input.Snapshot) {
			return fmt.Errorf("%w: fork source snapshot changed before admission", ErrConflict)
		}
	}
	frozen := encode(struct {
		Action  string
		Request Command
	}{"fork", input.Command})
	payload := encode(input)
	state := forkState{
		Phase: "admitted",
	}
	if input.Snapshot != nil {
		state.Phase = "captured"
		state.Snapshot = input.Snapshot
	}
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,'pending',?)", []any{
			parent.Parent.ID,
			"tenant",
			input.RequestTenantID,
			parent.Parent.Key,
			digest(frozen),
			frozen,
			encode(parent.Parent.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			parent.Parent.ID,
			parent.MAC,
		}},
		{"INSERT INTO operation_steps VALUES(?,'fork_group','fork',?,?,?,?,'pending',?)", []any{
			parent.Parent.ID,
			digest(payload),
			payload,
			parent.Parent.ID,
			"",
			encode(state),
		}},
		{"INSERT INTO resource_refs VALUES('image',?,'step',?)", []any{
			input.Template.Config.Image,
			parent.Parent.ID + ":fork_group",
		}},
	} {
		if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	if err := service.reserveForkTargets(ctx, tx, parent.Parent, input, parent.Free); err != nil {
		return err
	}
	if input.Snapshot != nil {
		return pinForkSnapshot(ctx, tx, parent.Parent.ID, input.OwnerTenantID, input.Snapshot.Snapshot.ID)
	}
	return nil
}

func pinForkSnapshot(ctx context.Context, tx *sql.Tx, group, tenant, snapshot string) error {
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO source_leases VALUES(?,?,'fork_group')", []any{
			snapshot,
			group,
		}},
		{"INSERT INTO resource_refs VALUES('snapshot',?,'step',?)", []any{
			snapshot,
			group + ":fork_group",
		}},
		{"INSERT INTO quota_refs VALUES(?,'snapshot',?,'engine_dependency',?)", []any{
			tenant,
			snapshot,
			group + ":fork_group",
		}},
	} {
		if err := requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return err
		}
	}
	return nil
}
