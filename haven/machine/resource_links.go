package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/execution"
	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine/internal/dbgen"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/store"
)

type ResourceLink struct {
	ChildID     string `json:"child_id"`
	Kind        string `json:"kind"`
	RequestHash string `json:"request_hash"`
	Candidate   string `json:"candidate"`
}

func LoadResourceLink(ctx context.Context, database store.DBTX, parent, role string) (ResourceLink, string, error) {
	var link ResourceLink
	operations := operation.New(database)
	record, err := operations.ResourceLink(ctx, parent, "link:"+role)
	if err != nil {
		return link, record.Status, err
	}
	if operation.Digest(record.Input) != record.Hash {
		return link, record.Status, fmt.Errorf("resource link checksum changed")
	}
	if err = json.Unmarshal(record.Input, &link); err != nil {
		return link, record.Status, err
	}
	childHash, err := operations.RequestHash(ctx, link.ChildID)
	if err != nil {
		return link, record.Status, err
	}
	if link.RequestHash != childHash || link.Candidate != record.Candidate {
		return link, record.Status, fmt.Errorf("resource link immutable child changed")
	}
	return link, record.Status, nil
}

// ResourceLinkTarget names the parent responsibility receiving a child link.
type ResourceLinkTarget struct {
	OperationID      string
	Role             string
	ExpectedInstance string
}

func InsertResourceLink(ctx context.Context, tx *sql.Tx, target ResourceLinkTarget, request ResourceAdmission) error {
	link := ResourceLink{
		ChildID:     request.Child.ID,
		Kind:        request.Kind,
		RequestHash: request.RequestHash,
		Candidate:   request.Candidate,
	}
	payload := operation.Encode(link)
	return store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         target.OperationID,
		StepKey:             "link:" + target.Role,
		Action:              "resource_link",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: request.Candidate,
		ExpectedInstance:    target.ExpectedInstance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	}))
}

// Complete verifies the original child dependency in the caller's
// transaction. The returned flag reports a write requiring commit/checkpoint.
// It neither runs the child nor commits or checkpoints the store.
func (link ResourceLink) Complete(ctx context.Context, tx *sql.Tx, parent, role string) (bool, error) {
	current, status, err := LoadResourceLink(ctx, tx, parent, role)
	if err != nil {
		return false, err
	}
	if !reflect.DeepEqual(current, link) {
		return false, fmt.Errorf("resource link completion changed")
	}
	if operation.Terminal(status) {
		return false, nil
	}
	key := map[string]string{
		"cold_start":      "resource",
		"release":         "resource",
		"clone":           "resource",
		"restore":         "resource",
		"lifecycle":       "lifecycle",
		"capture_discard": "capture_discard",
		"capture_cleanup": "capture_cleanup",
		"capture":         "capture",
		"snapshot_delete": "snapshot_delete",
		"discard":         "discard",
		"stop":            "stop",
	}[link.Kind]
	if key == "" {
		return false, fmt.Errorf("unsupported resource link kind %q", link.Kind)
	}
	operations := operation.New(tx)
	childStatus, err := operations.StepStatus(ctx, link.ChildID, key)
	if err != nil {
		return false, err
	}
	if !operation.Terminal(childStatus) {
		return false, errs.ErrInterrupted
	}
	if err = operations.CompletePendingLink(ctx, operation.LinkCompletion{
		OperationID: parent,
		StepKey:     "link:" + role,
		Status:      childStatus,
		Body:        operation.Encode("complete"),
	}); err != nil {
		return false, err
	}
	return true, nil
}

// ResourceLinks coordinates child receipts in the parent's workflow. The parent
// supplies its domain child execution; locks, transactions and HEAD are shared.
type ResourceLinks struct {
	store        store.Workflow
	ctx          context.Context
	storeWriteMu execution.Guard
}

func NewResourceLinks(state *store.State, storeWriteMu execution.Guard, ctx context.Context) *ResourceLinks {
	return &ResourceLinks{
		store: store.Workflow{
			Database:   state.Database(),
			Checkpoint: state.Checkpoint,
		},
		ctx:          ctx,
		storeWriteMu: storeWriteMu,
	}
}

func (rl *ResourceLinks) Complete(parent, role string, link ResourceLink) error {
	rl.storeWriteMu.Lock()
	defer rl.storeWriteMu.Unlock()
	tx, err := rl.store.Database.BeginTx(rl.ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	changed, err := link.Complete(rl.ctx, tx, parent, role)
	if err != nil || !changed {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return rl.store.Checkpoint()
}

func (rl *ResourceLinks) Run(parent, role string, run func(ResourceLink) error) (ResourceLink, error) {
	link, status, err := LoadResourceLink(rl.ctx, rl.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status == "failed" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	if status == "succeeded" {
		return link, nil
	}
	runErr := run(link)
	if errors.Is(runErr, errs.ErrInterrupted) {
		return link, runErr
	}
	if err = rl.Complete(parent, role, link); err != nil {
		return link, errors.Join(err, runErr)
	}
	_, status, err = LoadResourceLink(rl.ctx, rl.store.Database, parent, role)
	if err != nil {
		return link, err
	}
	if status != "succeeded" {
		return link, fmt.Errorf("%w: retained lifecycle resource failed", errs.ErrConflict)
	}
	return link, nil
}
