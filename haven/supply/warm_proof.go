package supply

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/woxQAQ/haven/machine"
	operation "github.com/woxQAQ/haven/operation"
	"github.com/woxQAQ/haven/quota"
	"github.com/woxQAQ/haven/supply/internal/dbgen"
)

// VerifyReadyWarm verifies the original refill publication, generation and zero-slot
// funding before a selection or collector can transfer the pool responsibility.
func VerifyReadyWarm(proofs machine.ResourceProofs, ctx context.Context, tx *sql.Tx, id string) (PoolReservation, machine.ResourceStep, error) {
	queries := dbgen.New(tx)
	reservation, status, err := LoadPoolReservation(ctx, tx, id)
	if err != nil {
		return reservation, machine.ResourceStep{}, err
	}
	if status != "ready" || reservation.Instance == "" {
		return reservation, machine.ResourceStep{}, fmt.Errorf("pool entry is not ready")
	}
	parent, err := LoadSupply(ctx, tx, id)
	if err != nil {
		return reservation, machine.ResourceStep{}, err
	}
	if parent.Status != "succeeded" || parent.State.Phase != "complete" || parent.Input.Command.Action != "refill" || !reflect.DeepEqual(parent.Input.Template, reservation.Template) || parent.Input.SandboxID != reservation.SandboxID {
		return reservation, machine.ResourceStep{}, fmt.Errorf("warm refill publication changed")
	}
	source, err := machine.LoadResourceStep(ctx, tx, reservation.Source.OperationID)
	if err != nil {
		return reservation, source, err
	}
	if source.Hash != reservation.Source.InputHash || source.Status != "succeeded" || source.State.Witness != reservation.Instance || source.Candidate != reservation.VMID || source.Input.SandboxID != reservation.SandboxID || source.Input.TenantID != reservation.Template.TenantID || source.Input.SlotBudget != (quota.SlotBudget{
		Kind:  "sandbox_slot",
		Units: 0,
	}) || source.Input.Restore == nil || source.Input.Restore.Snapshot.ID != reservation.Template.SnapshotID || string(operation.Encode(source.Input.Resource.Config)) != string(operation.Encode(reservation.Template.Config)) {
		return reservation, source, fmt.Errorf("warm resource birth proof differs from refill")
	}
	link, linked, err := machine.LoadResourceLink(ctx, tx, id, "clone")
	if err != nil {
		return reservation, source, err
	}
	if linked != "succeeded" || link.ChildID != source.OperationID {
		return reservation, source, fmt.Errorf("warm clone link changed")
	}
	if err = proofs.VerifyGeneration(ctx, tx, source); err != nil {
		return reservation, source, err
	}
	currentMachine, instance, owner, err := machine.LoadVM(ctx, tx, source)
	if err != nil {
		return reservation, source, err
	}
	if owner != "" || instance != reservation.Instance || !currentMachine.Verified || !currentMachine.AgentReady || currentMachine.Deleting {
		return reservation, source, fmt.Errorf("warm current resource ownership changed")
	}
	warm, err := queries.CountWarmIdentity(ctx, dbgen.CountWarmIdentityParams{
		VmID:       reservation.VMID,
		TemplateID: reservation.Template.ID,
		SnapshotID: reservation.Template.SnapshotID,
	})
	if err != nil || warm != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	responsibilities := quota.New(tx)
	checks := []struct {
		read func() (int64, error)
		want int64
	}{
		{func() (int64, error) {
			return responsibilities.CountResourceHolders(ctx, "vm", reservation.VMID)
		}, 2},
		{func() (int64, error) {
			return responsibilities.CountResourceReference(ctx, quota.ResourceReference{
				Kind:       "vm",
				ID:         reservation.VMID,
				HolderKind: "warm",
				HolderID:   id,
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountMatchingCost(ctx, quota.Cost{
				CostIdentity: quota.CostIdentity{
					TenantID:        reservation.Template.TenantID,
					Kind:            "sandbox_slot",
					Key:             reservation.SandboxID,
					SourceOperation: source.Input.SlotOperation,
				},
			})
		}, 1},
		{func() (int64, error) {
			return responsibilities.CountCostReference(ctx, quota.CostReference{
				TenantID:   reservation.Template.TenantID,
				Kind:       "sandbox_slot",
				Key:        reservation.SandboxID,
				HolderKind: "active_use",
				HolderID:   "sandbox:" + reservation.SandboxID,
			})
		}, 1},
	}
	for _, check := range checks {
		if count, err := check.read(); err != nil || count != check.want {
			return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
		}
	}

	if count, err := machine.RetainedInstanceCount(ctx, tx, reservation.VMID); err != nil || count != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	if count, err := machine.RetainedVersionCount(ctx, tx, reservation.VMID); err != nil || count != 1 {
		return reservation, source, fmt.Errorf("warm responsibility incomplete: %v", err)
	}
	return reservation, source, nil
}
