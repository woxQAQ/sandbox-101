package haven

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/woxQAQ/haven/loom"
)

type Template struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	Image      string     `json:"image"`
	Shape      loom.Shape `json:"shape"`
	SnapshotID string     `json:"snapshot_id,omitempty"`
	State      string     `json:"state"`
	TargetWarm int        `json:"target_warm"`
	Warmup     string     `json:"warmup,omitempty"`
}

func publicTemplate(record templateRecord) Template {
	return Template{ID: record.ID, TenantID: record.TenantID, Image: record.Config.Image, Shape: record.Config.Shape, SnapshotID: record.SnapshotID, State: record.State, TargetWarm: record.TargetWarm, Warmup: record.Warmup}
}

func (service *Service) GetTemplate(ctx context.Context, principal Principal, id string) (Template, error) {
	if principal.SandboxID != "" {
		return Template{}, loom.Fail(loom.NotFound, "not found")
	}
	var body []byte
	err := service.store.db.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).Scan(&body)
	if err != nil {
		return Template{}, err
	}
	var record templateRecord
	if err = json.Unmarshal(body, &record); err != nil {
		return Template{}, err
	}
	if !principal.Root && principal.TenantID != record.TenantID {
		return Template{}, loom.Fail(loom.NotFound, "not found")
	}
	verified, err := loadOwnedTemplate(ctx, service.store.db, record.TenantID, id)
	if err != nil {
		return Template{}, err
	}
	return publicTemplate(verified), nil
}

type supplyInput struct {
	RequesterRoot   bool            `json:"requester_root,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	RequestTenantID string          `json:"request_tenant_id"`
	Command         Command         `json:"command"`
	Template        templateRecord  `json:"template"`
	Previous        *templateRecord `json:"previous,omitempty"`
	Source          *restoreSource  `json:"source,omitempty"`
	SandboxID       string          `json:"sandbox_id"`
}
type (
	supplyState struct {
		FinishedAt time.Time `json:"finished_at"`
		Phase      string    `json:"phase"`
		SnapshotID string    `json:"snapshot_id,omitempty"`
		Error      string    `json:"error,omitempty"`
	}
	supplyStep struct {
		OperationID, Hash, Status string
		Input                     supplyInput
		State                     supplyState
	}
	supplyAdmission struct {
		Parent    Operation
		Input     supplyInput
		MAC, Role string
	}
)

func loadSupply(ctx context.Context, q queryer, id string) (supplyStep, error) {
	step := supplyStep{OperationID: id}
	var input, body []byte
	var candidate, expected, action string
	err := q.QueryRowContext(ctx, "SELECT input_hash,input,status,body,candidate_resource_id,expected_instance,action FROM operation_steps WHERE operation_id=? AND step_key='supply'", id).Scan(&step.Hash, &input, &step.Status, &body, &candidate, &expected, &action)
	if err != nil {
		return step, err
	}
	if digest(input) != step.Hash {
		return step, fmt.Errorf("supply input checksum changed")
	}
	if err = json.Unmarshal(input, &step.Input); err != nil {
		return step, err
	}
	if err = json.Unmarshal(body, &step.State); err != nil {
		return step, err
	}
	if action != step.Input.Command.Action || candidate != step.Input.Template.ID || expected != "" || step.Input.Template.TenantID == "" {
		return step, fmt.Errorf("supply frozen identity changed")
	}
	return step, nil
}

func (parent *supplyAdmission) admitResource(ctx context.Context, service *Service, tx *sql.Tx, child Operation, kind, candidate, hash string) error {
	return service.admitSupplyResource(ctx, tx, parent, child, kind, candidate, hash)
}

func (parent *supplyAdmission) provisioningTemplate() (templateRecord, bool) {
	if parent == nil || parent.Input.Command.Action != "template_build" {
		return templateRecord{}, false
	}
	return parent.Input.Template, true
}

type provisionalTemplateParent interface{ provisioningTemplate() (templateRecord, bool) }

func provisionalTemplate(parent resourceParent) (templateRecord, bool) {
	if provider, ok := parent.(provisionalTemplateParent); ok {
		return provider.provisioningTemplate()
	}
	return templateRecord{}, false
}

func currentTemplate(ctx context.Context, q queryer, id string) (*templateRecord, error) {
	var body []byte
	err := q.QueryRowContext(ctx, "SELECT body FROM templates WHERE id=?", id).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record templateRecord
	if err = json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	if record.ID != id {
		return nil, loom.Fail(loom.ManualRecovery, "template catalog identity changed")
	}
	return &record, nil
}
