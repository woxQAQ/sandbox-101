package machine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"

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

func InsertResourceLink(ctx context.Context, tx *sql.Tx, parent, role, instance string, child operation.Record, kind, candidate, hash string) error {
	link := ResourceLink{
		ChildID:     child.ID,
		Kind:        kind,
		RequestHash: hash,
		Candidate:   candidate,
	}
	payload := operation.Encode(link)
	return store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
		OperationID:         parent,
		StepKey:             "link:" + role,
		Action:              "resource_link",
		InputHash:           operation.Digest(payload),
		Input:               payload,
		CandidateResourceID: candidate,
		ExpectedInstance:    instance,
		Status:              "pending",
		Body:                operation.Encode("admitted"),
	}))
}

// CompleteResourceLink verifies the original child dependency in the caller's
// transaction. The returned flag reports a write requiring commit/checkpoint.
// It neither runs the child nor commits or checkpoints the store.
func CompleteResourceLink(ctx context.Context, tx *sql.Tx, parent, role string, link ResourceLink) (bool, error) {
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
	if err = operations.CompletePendingLink(ctx, parent, "link:"+role, childStatus, operation.Encode("complete")); err != nil {
		return false, err
	}
	return true, nil
}
