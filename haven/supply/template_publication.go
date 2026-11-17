package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

func (service *Supply) publishTemplate(ctx context.Context, tx *sql.Tx, step SupplyStep, snapshot string) error {
	current, err := LoadSupply(ctx, tx, step.OperationID)
	if err != nil {
		return err
	}
	if current.Hash != step.Hash || string(operation.Encode(current.Input)) != string(operation.Encode(step.Input)) || !reflect.DeepEqual(current.State, step.State) || operation.Terminal(current.Status) {
		return fmt.Errorf("template publication intent changed")
	}
	input := step.Input
	queries := dbgen.New(tx)
	references := quota.New(tx)
	operations := operation.New(tx)
	intent, err := queries.GetIntent(ctx, input.Template.ID)
	if err != nil {
		return err
	}
	if intent.TenantID != input.Template.TenantID || intent.OperationID != step.OperationID {
		return fmt.Errorf("template publication was superseded")
	}
	previous, err := CurrentTemplate(ctx, tx, input.Template.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(previous, input.Previous) {
		return fmt.Errorf("template prior publication changed")
	}
	desired := input.Template
	desired.State = "ready"
	desired.SnapshotID = snapshot
	if snapshot != "" {
		source, err := service.proofs.Snapshots().SnapshotPublication(ctx, tx, desired.TenantID, snapshot)
		if err != nil {
			return err
		}
		if string(operation.Encode(source.Snapshot.Manifest.Config)) != string(operation.Encode(desired.Config)) {
			return fmt.Errorf("template snapshot configuration changed")
		}
		if input.Command.Action == "template_put" {
			if input.Source == nil || !reflect.DeepEqual(*input.Source, source) {
				return fmt.Errorf("template import original publication changed")
			}
		} else {
			captured, status, err := machine.LoadResourceLink(ctx, tx, step.OperationID, "capture")
			if err != nil {
				return err
			}
			if status != "succeeded" || captured.ChildID != source.CaptureOperation {
				return fmt.Errorf("build snapshot lacks original capture completion")
			}
			released, status, err := machine.LoadResourceLink(ctx, tx, step.OperationID, "release")
			if err != nil {
				return err
			}
			if status != "succeeded" {
				return fmt.Errorf("build scratch retirement incomplete")
			}
			resource, err := machine.LoadResourceStep(ctx, tx, released.ChildID)
			if err != nil {
				return err
			}
			if resource.Status != "succeeded" || resource.State.Phase != "complete" || resource.Input.SandboxID != input.SandboxID {
				return fmt.Errorf("build scratch deletion proof changed")
			}
		}
	}
	if previous == nil || previous.SnapshotID != snapshot {
		if snapshot != "" {
			if err = references.AddResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         snapshot,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return err
			}
			if err = references.AddCostReference(ctx, quota.CostReference{
				TenantID:   desired.TenantID,
				Kind:       "snapshot",
				Key:        snapshot,
				HolderKind: "active_use",
				HolderID:   "template:" + desired.ID,
			}); err != nil {
				return err
			}
		}
		if previous != nil && previous.SnapshotID != "" {
			if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "snapshot",
				ID:         previous.SnapshotID,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return err
			}
			if err = references.RemoveCostReference(ctx, quota.CostReference{
				TenantID:   desired.TenantID,
				Kind:       "snapshot",
				Key:        previous.SnapshotID,
				HolderKind: "active_use",
				HolderID:   "template:" + desired.ID,
			}); err != nil {
				return err
			}
		}
	}
	if previous == nil || previous.Config.Image != desired.Config.Image {
		if err = references.AddResourceReference(ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         desired.Config.Image,
			HolderKind: "template",
			HolderID:   desired.ID,
		}); err != nil {
			return err
		}
		if previous != nil {
			if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
				Kind:       "image",
				ID:         previous.Config.Image,
				HolderKind: "template",
				HolderID:   desired.ID,
			}); err != nil {
				return err
			}
		}
	} else {
		count, lookup := references.CountResourceReference(ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         desired.Config.Image,
			HolderKind: "template",
			HolderID:   desired.ID,
		})
		if lookup != nil || count != 1 {
			return fmt.Errorf("template prior image responsibility missing: %v", lookup)
		}
	}
	if err = publishTemplate(ctx, tx, desired, previous); err != nil {
		return err
	}
	parent, err := operations.Projection(ctx, step.OperationID)
	if err != nil {
		return err
	}
	if operation.Terminal(parent.Status) || parent.ScopeKind != "tenant" || parent.ScopeID != input.RequestTenantID {
		return fmt.Errorf("template result authority changed")
	}
	var result operation.Result
	if err = json.Unmarshal(parent.Body, &result); err != nil {
		return err
	}
	result.TemplateID = desired.ID
	result.SnapshotID = snapshot
	completed := operation.Encode(SupplyState{
		FinishedAt: time.Now().UTC(),
		Phase:      "complete",
		SnapshotID: snapshot,
	})
	previousState, resultBody := operation.Encode(step.State), operation.Encode(result)
	if err = references.RemoveResourceReference(ctx, quota.ResourceReference{
		Kind:       "image",
		ID:         desired.Config.Image,
		HolderKind: "step",
		HolderID:   step.OperationID + ":supply",
	}); err != nil {
		return err
	}
	if err = store.RequireOne(dbgen.New(tx).CompleteUnchangedStep(ctx, dbgen.CompleteUnchangedStepParams{
		OperationID:  step.OperationID,
		StepKey:      "supply",
		InputHash:    step.Hash,
		PreviousBody: previousState,
		NewBody:      completed,
	})); err != nil {
		return err
	}
	return operations.CompleteResult(ctx, step.OperationID, resultBody)
}
