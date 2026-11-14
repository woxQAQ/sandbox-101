package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
)

// A fork group owns the whole requested batch before any capture or allocation.
// Source contains either a completed snapshot or the exact generation to capture.
type forkInput struct {
	RequestTenantID string          `json:"request_tenant_id"`
	OwnerTenantID   string          `json:"owner_tenant_id"`
	Command         Command         `json:"command"`
	Template        templateRecord  `json:"template"`
	Settings        sandboxSettings `json:"settings"`
	Targets         []sandboxRecord `json:"targets"`
	SourceSandbox   *sandboxRecord  `json:"source_sandbox,omitempty"`
	Source          *resourceStep   `json:"source,omitempty"`
	Snapshot        *restoreSource  `json:"snapshot,omitempty"`
	Checkpoint      *Checkpoint     `json:"checkpoint,omitempty"`
}
type forkState struct {
	Phase    string         `json:"phase"`
	Snapshot *restoreSource `json:"snapshot,omitempty"`
	Children []string       `json:"children,omitempty"`
	Error    string         `json:"error,omitempty"`
}
type forkStep struct {
	OperationID, Hash, Status string
	Input                     forkInput
	State                     forkState
}
type forkAdmission struct {
	Parent Operation
	Input  forkInput
	MAC    string
	Free   int64
}
type forkBranchAdmission struct {
	Group  forkStep
	Target sandboxRecord
	Index  int
}

func loadFork(ctx context.Context, q queryer, id string) (forkStep, error) {
	step := forkStep{
		OperationID: id,
	}
	var payload, body []byte
	var action, candidate, expected string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,action,candidate_resource_id,expected_instance FROM operation_steps WHERE operation_id=? AND step_key='fork_group'", id).Scan(&step.Hash, &payload, &step.Status, &body, &action, &candidate, &expected)
	if err != nil {
		return step, err
	}
	if digest(payload) != step.Hash {
		return step, fmt.Errorf("fork group input changed")
	}
	if err = json.Unmarshal(payload, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != "fork" || candidate != id || expected != "" || step.Input.Command.Action != "fork" || len(step.Input.Targets) < 1 || len(step.Input.Targets) > 32 || len(step.Input.Targets) != step.Input.Command.Count || (step.Input.Source == nil) == (step.Input.Snapshot == nil) {
		return step, fmt.Errorf("fork group source or batch identity changed")
	}
	ids := map[string]bool{}
	for _, target := range step.Input.Targets {
		if target.TenantID != step.Input.OwnerTenantID || target.OperationID != id || target.State != "provisioning" || target.VMID != "" || target.SourceOperation != "" || target.Identity.TemplateID != step.Input.Template.ID || target.Identity.Shape != step.Input.Template.Config.Shape || target.Identity.Epoch == "" || ids[target.ID] {
			return step, fmt.Errorf("fork placement identity changed")
		}
		ids[target.ID] = true
	}
	if len(step.State.Children) != 0 && len(step.State.Children) != len(step.Input.Targets) {
		return step, fmt.Errorf("fork child batch is incomplete")
	}
	switch step.State.Phase {
	case "admitted", "captured", "allocated", "compensating", "complete", "failed":
	default:
		return step, fmt.Errorf("fork group phase is unknown")
	}
	return step, nil
}

func (parent *forkAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	if parent.Input.Source == nil || kind != "capture" {
		return fmt.Errorf("%w: fork capture dependency changed", ErrConflict)
	}
	source, err := loadStep(ctx, tx, parent.Input.Source.OperationID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(source, *parent.Input.Source) || child.Result.VMID != source.Candidate || child.Result.SandboxID != source.Input.SandboxID || candidate != child.Result.SnapshotID {
		return fmt.Errorf("%w: fork capture original source changed", ErrConflict)
	}
	parent.Parent.Result.SnapshotID = child.Result.SnapshotID
	if err = service.admitForkIntent(ctx, tx, parent); err != nil {
		return err
	}
	return insertResourceLink(ctx, tx, parent.Parent.ID, "capture", parent.Input.Source.State.Witness, child, kind, candidate, hash)
}

func (parent *forkBranchAdmission) validateProvisionTarget(ctx context.Context, service *Service, tx *sql.Tx, sandbox sandboxRecord, source restoreSource, holder string) (*quotaDestination, error) {
	group, err := loadFork(ctx, tx, parent.Group.OperationID)
	if err != nil {
		return nil, err
	}
	if group.Hash != parent.Group.Hash || terminal(group.Status) || group.State.Phase != "captured" || !reflect.DeepEqual(group.State.Snapshot, parent.Group.State.Snapshot) || group.State.Snapshot == nil || !reflect.DeepEqual(*group.State.Snapshot, source) || parent.Index < 0 || parent.Index >= len(group.Input.Targets) || !reflect.DeepEqual(group.Input.Targets[parent.Index], sandbox) || holder != forkSlotHolder(group.OperationID, sandbox.ID) {
		return nil, fmt.Errorf("%w: fork placement no longer owns its prefunded slot", ErrConflict)
	}
	settings, err := loadSettings(ctx, tx, sandbox.ID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(settings, group.Input.Settings) {
		return nil, fmt.Errorf("%w: fork placement settings changed", ErrConflict)
	}
	return nil, nil
}

func (parent *forkBranchAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	if kind != "clone" || child.Result.SandboxID != parent.Target.ID {
		return fmt.Errorf("%w: fork branch resource identity changed", ErrConflict)
	}
	if err := requireWrite(tx.ExecContext(ctx, "DELETE FROM sandbox_settings WHERE sandbox_id=? AND body=?", parent.Target.ID, encode(parent.Group.Input.Settings))); err != nil {
		return err
	}
	return insertResourceLink(ctx, tx, parent.Group.OperationID, fmt.Sprintf("branch:%d", parent.Index), "", child, kind, candidate, hash)
}

func forkSlotHolder(group, sandbox string) string {
	return "fork:" + group + ":" + sandbox
}

func forkTargetIDs(input forkInput) []string {
	ids := make([]string, len(input.Targets))
	for index, target := range input.Targets {
		ids[index] = target.ID
	}
	return ids
}

func (service *Service) saveFork(step *forkStep, state forkState) error {
	service.admission.Lock()
	defer service.admission.Unlock()
	tx, err := service.store.db.BeginTx(service.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err = requireWrite(tx.ExecContext(service.ctx, "UPDATE operation_steps SET status='running',body=? WHERE operation_id=? AND step_key='fork_group' AND input_hash=? AND status IN ('pending','running') AND body=?", encode(state), step.OperationID, step.Hash, encode(step.State))); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	step.State = state
	return service.store.checkpoint()
}

func (service *Service) forkSecrets(group forkStep) {
	service.mu.Lock()
	defer service.mu.Unlock()
	for _, child := range group.State.Children {
		service.opSecrets[child] = service.opSecrets[group.OperationID]
	}
}

func sameForkTargets(first, second []string) bool {
	return slices.Equal(first, second)
}
