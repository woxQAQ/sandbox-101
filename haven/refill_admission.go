package haven

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

func refillBackoff(ctx context.Context, q rowsQueryer, template string, now time.Time) (bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT s.input,s.body,s.status FROM operation_steps s WHERE s.step_key='supply' AND s.action='refill' AND s.candidate_resource_id=? AND s.status IN ('failed','succeeded') ORDER BY s.rowid DESC LIMIT 10", template)
	if err != nil {
		return true, err
	}
	defer func() {
		_ = rows.Close()
	}()
	failures := 0
	var latest time.Time
	for rows.Next() {
		var input, body []byte
		var status string
		if err = rows.Scan(&input, &body, &status); err != nil {
			return true, err
		}
		if status == "succeeded" {
			break
		}
		var intent supplyInput
		var state supplyState
		if err = json.Unmarshal(input, &intent); err != nil {
			return true, err
		}
		if err = json.Unmarshal(body, &state); err != nil {
			return true, err
		}
		if failures == 0 {
			latest = state.FinishedAt
			if latest.IsZero() {
				latest = intent.CreatedAt
			}
		}
		failures++
	}
	if err = rows.Err(); err != nil {
		return true, err
	}
	if failures == 0 {
		return false, nil
	}
	delay := time.Second << min(failures-1, 8)
	return now.Before(latest.Add(delay)), nil
}

func (service *Service) AdmitRefill(ctx context.Context, principal Principal, key string, command Command) (Operation, error) {
	var empty Operation
	if principal.TenantID == "" || principal.SandboxID != "" || key == "" || len(key) > 256 || command.Action != "refill" || command.TemplateID == "" {
		return empty, fmt.Errorf("%w: refill requires tenant authority, template and key", ErrInvalid)
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
	service.mu.Lock()
	draining := service.draining
	service.mu.Unlock()
	if draining {
		return empty, fmt.Errorf("%w: node is draining", ErrExhausted)
	}
	public, err := service.GetTemplate(ctx, principal, command.TemplateID)
	if err != nil {
		return empty, err
	}
	template, err := loadOwnedTemplate(ctx, service.store.db, public.TenantID, public.ID)
	if err != nil {
		return empty, err
	}
	if template.TargetWarm < 1 || template.SnapshotID == "" {
		return empty, fmt.Errorf("%w: refill requires golden template with warm target", ErrInvalid)
	}
	now := time.Now().UTC()
	blocked, err := refillBackoff(ctx, service.store.db, template.ID, now)
	if err != nil {
		return empty, err
	}
	if blocked {
		return empty, fmt.Errorf("%w: template refill backoff is active", ErrExhausted)
	}
	source, err := service.loadRestoreSource(ctx, service.store.db, template.TenantID, template.SnapshotID)
	if err != nil {
		return empty, err
	}
	if err = verifyRestoreSourceFiles(source); err != nil {
		return empty, err
	}
	if err = service.opts.Backend.Check(ctx, template.Config); err != nil {
		return empty, err
	}
	image, err := loadImage(ctx, service.store.db, template.Config.Image)
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
		Result: operationResult{
			TemplateID: template.ID,
			SandboxID:  newID("sandbox_"),
			SnapshotID: template.SnapshotID,
		},
	}
	input := supplyInput{
		CreatedAt:       now,
		RequestTenantID: principal.TenantID,
		Command:         sanitizedCommand(command),
		Template:        template,
		Source:          &source,
		SandboxID:       parent.Result.SandboxID,
	}
	admission := &supplyAdmission{
		Parent: parent,
		Input:  input,
		MAC:    mac,
		Role:   "clone",
	}
	request := CloneRequest{
		TenantID:   template.TenantID,
		Key:        parent.ID + ":clone",
		SnapshotID: template.SnapshotID,
	}
	frozen := encode(struct {
		Action  string
		Request CloneRequest
	}{"clone", request})
	child, err := service.admitProvision(ctx, template.TenantID, request.Key, frozen, digest(frozen), template, image, &source, nil, &provisionTarget{
		NewSandboxID: input.SandboxID,
		Budget: &slotBudget{
			Kind:  "sandbox_slot",
			Units: 0,
		},
		Parent: admission,
	})
	if child.ID == "" {
		if errors.Is(err, ErrExhausted) {
			return service.recordRefillFailure(ctx, parent, input, mac)
		}
		return empty, err
	}
	parent.Result.VMID = child.Result.VMID
	if err != nil {
		return parent, err
	}
	service.dispatch(parent.ID)
	return parent, nil
}

func (service *Service) recordRefillFailure(ctx context.Context, parent Operation, input supplyInput, mac string) (Operation, error) {
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
	}{"refill", input.Command})
	payload := encode(input)
	parent.Status = "failed"
	parent.Result.Error = "refill admission exhausted"
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
			"failed",
			encode(parent.Result),
		}},
		{"INSERT INTO operation_credentials VALUES(?,?)", []any{
			parent.ID,
			mac,
		}},
		{"INSERT INTO operation_steps VALUES(?,'supply','refill',?,?,?,?,'failed',?)", []any{
			parent.ID,
			digest(payload),
			payload,
			input.Template.ID,
			"",
			encode(supplyState{
				Phase:      "failed",
				FinishedAt: time.Now().UTC(),
				Error:      parent.Result.Error,
			}),
		}},
	} {
		if err = requireWrite(tx.ExecContext(ctx, statement.q, statement.args...)); err != nil {
			return Operation{}, err
		}
	}
	if err = commitPublications(tx); err != nil {
		return Operation{}, err
	}
	return parent, service.store.checkpoint()
}
