package supply

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/internal/errs"
	"github.com/woxQAQ/haven/machine"
	"github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/ownership"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/store"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

type SupplyAdmission struct {
	Parent operation.Record
	Input  SupplyInput
	MAC    string
	Role   string
}

func (parent *SupplyAdmission) ProvisioningTemplate() (TemplateRecord, bool) {
	if parent == nil || parent.Input.Command.Action != "template_build" {
		return TemplateRecord{}, false
	}
	return parent.Input.Template, true
}

func (admission *SupplyAdmission) AdmitResource(ctx context.Context, tx *sql.Tx, request machine.ResourceAdmission) error {
	if admission == nil {
		return nil
	}
	child := request.Child
	input, parent := admission.Input, admission.Parent
	existing, err := LoadSupply(ctx, tx, parent.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if input.Command.Action != "template_build" && input.Command.Action != "refill" {
			return fmt.Errorf("%w: new supply resource requires build or refill intent", errs.ErrInvalid)
		}
		if input.CreatedAt.IsZero() {
			return fmt.Errorf("%w: supply admission timestamp missing", errs.ErrInvalid)
		}
		if input.Command.Action == "refill" {
			blocked, err := RefillBackoff(ctx, tx, input.Template.ID, input.CreatedAt)
			if err != nil {
				return err
			}
			if blocked {
				return fmt.Errorf("%w: template refill backoff is active", errs.ErrExhausted)
			}
			current, err := NewTemplateCatalog(tx).Owned(ctx, input.Template.TenantID, input.Template.ID)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(current, input.Template) || current.TargetWarm < 1 || current.SnapshotID == "" {
				return fmt.Errorf("%w: refill template version changed", errs.ErrConflict)
			}
			count, lookup := dbgen.New(tx).CountTemplateReservations(ctx, current.ID)
			if lookup != nil {
				return lookup
			}
			if count >= int64(current.TargetWarm) {
				return fmt.Errorf("%w: template pool occupation reached target", errs.ErrExhausted)
			}
		}
		frozen := operation.Encode(struct {
			Action  string
			Request operation.Command
		}{input.Command.Action, input.Command})
		payload := operation.Encode(input)
		parent.Result.TemplateID = input.Template.ID
		parent.Result.SandboxID = input.SandboxID
		parent.Result.VMID = child.Result.VMID
		operations := operation.New(tx)
		if err = operations.Insert(ctx, operation.Admission{
			ID:        parent.ID,
			ScopeKind: "tenant",
			ScopeID:   input.RequestTenantID,
			Key:       parent.Key,
			Hash:      operation.Digest(frozen),
			Request:   frozen,
			Status:    "pending",
			Body:      operation.Encode(parent.Result),
		}); err != nil {
			return err
		}
		if err = operations.InsertCredential(ctx, parent.ID, admission.MAC); err != nil {
			return err
		}
		if err = store.RequireOne(dbgen.New(tx).InsertStep(ctx, dbgen.InsertStepParams{
			OperationID:         parent.ID,
			StepKey:             "supply",
			Action:              input.Command.Action,
			InputHash:           operation.Digest(payload),
			Input:               payload,
			CandidateResourceID: input.Template.ID,
			ExpectedInstance:    "",
			Status:              "pending",
			Body: operation.Encode(SupplyState{
				Phase: "admitted",
			}),
		})); err != nil {
			return err
		}
		if err = quota.New(tx).AddResourceReference(ctx, quota.ResourceReference{
			Kind:       "image",
			ID:         input.Template.Config.Image,
			HolderKind: "step",
			HolderID:   parent.ID + ":supply",
		}); err != nil {
			return err
		}
		if input.Command.Action == "template_build" {
			if err = input.Template.reservePublication(ctx, tx, input.Previous, parent.ID); err != nil {
				return err
			}
		} else {
			resource, err := machine.LoadResourceStep(ctx, tx, child.ID)
			if err != nil {
				return err
			}
			reservation := PoolReservation{
				Template:  input.Template,
				SandboxID: input.SandboxID,
				VMID:      child.Result.VMID,
				Source: ownership.GenerationRef{
					OperationID: child.ID,
					InputHash:   resource.Hash,
				},
				CreatedAt: input.CreatedAt,
			}
			reservationBody, err := json.Marshal(reservation)
			if err != nil {
				return err
			}
			if err = store.RequireOne(dbgen.New(tx).InsertReservation(ctx, dbgen.InsertReservationParams{
				OperationID:     parent.ID,
				TemplateID:      reservation.Template.ID,
				TemplateVersion: reservation.Template.Version,
				VmID:            reservation.VMID,
				Body:            reservationBody,
			})); err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	} else {
		if operation.Terminal(existing.Status) || !reflect.DeepEqual(existing.Input, input) {
			return fmt.Errorf("%w: supply no longer owns resource admission", errs.ErrConflict)
		}
		if input.Command.Action == "template_build" && admission.Role != "release" && admission.Role != "discard" {
			intent, lookup := dbgen.New(tx).GetIntent(ctx, input.Template.ID)
			if lookup != nil {
				return lookup
			}
			if intent.TenantID != input.Template.TenantID || intent.OperationID != parent.ID {
				return fmt.Errorf("%w: template intent was superseded", errs.ErrConflict)
			}
		}
	}
	return machine.InsertResourceLink(ctx, tx, machine.ResourceLinkTarget{
		OperationID: parent.ID,
		Role:        admission.Role,
	}, request)
}
