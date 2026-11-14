package haven

import (
	"context"
	"path/filepath"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/loom"
	"github.com/woxQAQ/haven/loom/host"
)

func (service *Service) AdmitTemplate(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Template == nil || (command.Action != "template_put" && command.Action != "template_build") {
		return empty, loom.Fail(loom.Invalid, "template requires tenant authority, specification and key")
	}
	guard := service.vmLock("request:" + string(encode([]string{
		principal.TenantID,
		key,
	})))
	guard.Lock()
	defer guard.Unlock()
	mac := service.store.commandMAC(principal, command)
	if operation, found, err := service.existingSandboxOperation(ctx, principal, key, &claimAdmission{
		MAC: mac,
	}); found || err != nil {
		return operation, err
	}
	spec := *command.Template
	if spec.ID == "" || len(spec.ID) > 128 || spec.TargetWarm < 0 || spec.TargetWarm > service.opts.MaxWarm {
		return empty, loom.Fail(loom.Invalid, "template owner, identity or warm target invalid")
	}
	if principal.Root {
		if spec.TenantID == "" {
			return empty, loom.Fail(loom.Invalid, "root template administration requires explicit owner")
		}
	} else {
		if spec.TenantID != "" && spec.TenantID != principal.TenantID {
			return empty, loom.Fail(loom.Invalid, "template owner cannot change")
		}
		spec.TenantID = principal.TenantID
	}
	if command.Action == "template_build" && (service.opts.Guest == nil || spec.SnapshotID != "") {
		return empty, loom.Fail(loom.Invalid, "template build requires guest effects and a fresh snapshot")
	}
	if command.Action == "template_put" && spec.SnapshotID == "" && spec.TargetWarm != 0 {
		return empty, loom.Fail(loom.Invalid, "cold template cannot reserve warm instances")
	}
	config := loom.Config{
		Image: spec.Image,
		Shape: spec.Shape,
	}
	if err := host.CheckConfig(&config); err != nil {
		return empty, err
	}
	if err := service.opts.Backend.Check(ctx, config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, config.Image)
	if err != nil {
		return empty, err
	}
	if err = verifyImageFiles(image, filepath.Join(service.store.directory, "images", image.Digest[7:])); err != nil {
		return empty, err
	}
	var source *restoreSource
	if spec.SnapshotID != "" {
		snapshot, err := service.loadRestoreSource(ctx, service.store.db, spec.TenantID, spec.SnapshotID)
		if err != nil {
			return empty, err
		}
		if err = verifyRestoreSourceFiles(snapshot); err != nil {
			return empty, err
		}
		if snapshot.Snapshot.Manifest.Config.Image != config.Image || !reflect.DeepEqual(snapshot.Snapshot.Manifest.Config.Shape, config.Shape) {
			return empty, loom.Fail(loom.Conflict, "template source shape/image differs")
		}
		config = snapshot.Snapshot.Manifest.Config
		source = &snapshot
	}
	previous, err := currentTemplate(ctx, service.store.db, spec.ID)
	if err != nil {
		return empty, err
	}
	if previous != nil && previous.TenantID != spec.TenantID {
		return empty, loom.Fail(loom.Conflict, "template owner cannot change")
	}
	parent := Operation{
		ID:     newID("op-"),
		Key:    key,
		Status: "pending",
		Result: operationResult{
			TemplateID: spec.ID,
		},
	}
	desired := templateRecord{
		ID:         spec.ID,
		TenantID:   spec.TenantID,
		Config:     config,
		SnapshotID: spec.SnapshotID,
		TargetWarm: spec.TargetWarm,
		Warmup:     spec.Warmup,
		State:      "ready",
		Version:    parent.ID,
	}
	frozenCommand := sanitizedCommand(command)
	frozenCommand.Template = &spec
	input := supplyInput{
		RequesterRoot:   principal.Root,
		CreatedAt:       time.Now().UTC(),
		RequestTenantID: principal.TenantID,
		Command:         frozenCommand,
		Template:        desired,
		Previous:        previous,
		Source:          source,
	}
	if command.Action == "template_put" {
		return service.admitTemplatePut(ctx, parent, input, mac)
	}
	input.Template.State = "building"
	input.SandboxID = newID("sandbox_")
	parent.Result.SandboxID = input.SandboxID
	link := &supplyAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "cold",
	}
	request := ColdRequest{
		TenantID:   spec.TenantID,
		Key:        parent.ID + ":cold",
		TemplateID: spec.ID,
	}
	frozen := encode(struct {
		Action  string
		Request ColdRequest
	}{"claim", request})
	child, err := service.admitProvision(ctx, spec.TenantID, request.Key, frozen, digest(frozen), input.Template, image, nil, nil, &provisionTarget{
		NewSandboxID: input.SandboxID,
		Budget: &slotBudget{
			Kind:  "build_slot",
			Units: 1,
		},
		Parent: link,
	})
	if child.ID == "" {
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) admitTemplatePut(ctx context.Context, parent Operation, input supplyInput, mac string) (Operation, error) {
	service.admission.Lock()
	defer service.admission.Unlock()
	if err := service.checkAdmissionOpen(); err != nil {
		return Operation{}, err
	}
	tx, err := service.store.db.BeginTx(ctx, nil)
	if err != nil {
		return Operation{}, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	frozen := encode(struct {
		Action  string
		Request Command
	}{input.Command.Action, input.Command})
	payload := encode(input)
	for _, statement := range []struct {
		q    string
		args []any
	}{
		{"INSERT INTO operations VALUES(?,?,?,?,?,?,?,?)", []any{
			parent.ID,
			"tenant",
			input.RequestTenantID,
			parent.Key,
			digest(frozen),
			frozen,
			"pending",
			encode(parent.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			parent.ID,
			mac,
		}},
		{"INSERT INTO operation_steps VALUES(?,'supply','template_put',?,?,?,?,'pending',?)", []any{
			parent.ID,
			digest(payload),
			payload,
			input.Template.ID,
			"",
			encode(supplyState{
				Phase: "admitted",
			}),
		}},
		{"INSERT INTO resource_refs VALUES('image',?,'step',?)", []any{
			input.Template.Config.Image,
			parent.ID + ":supply",
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return Operation{}, err
		}
	}
	if err = service.reserveTemplateIntent(ctx, tx, parent, input); err != nil {
		return Operation{}, err
	}
	if err = service.publishTemplate(ctx, tx, supplyStep{
		OperationID: parent.ID,
		Hash:        digest(payload),
		Status:      "pending",
		Input:       input,
		State: supplyState{
			Phase: "admitted",
		},
	}, input.Template.SnapshotID); err != nil {
		return Operation{}, err
	}
	if err = service.fault("template_publication_before_commit"); err != nil {
		return Operation{}, err
	}
	if err = commitPublications(tx); err != nil {
		return Operation{}, err
	}
	parent.Status = "succeeded"
	parent.Result.SnapshotID = input.Template.SnapshotID
	return parent, service.store.checkpoint()
}
